"""Abstract base class for terminal backends."""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from terminals.config import settings
from terminals.utils.policy_lifecycle import mark_reset_applied, reset_due_for

log = logging.getLogger(__name__)


@dataclass
class RefreshResult:
    matched: int = 0
    refreshed: int = 0
    reset: int = 0
    skipped_active: int = 0


class Backend(ABC):
    """Lifecycle interface for provisioning and managing terminal instances.

    Includes an in-memory activity tracker and idle reaper that automatically
    tears down terminals that haven't been accessed within the configured
    timeout (``settings.idle_timeout_minutes`` or per-policy
    ``idle_timeout_minutes``).
    """

    # Seconds between persisted-activity writes per key (debounce, keeps DB
    # traffic at ≤1 write/min per active terminal).
    _ACTIVITY_PERSIST_INTERVAL = 60

    def __init__(self) -> None:
        # key = "{user_id}:{policy_id}"
        self._activity: dict[str, float] = {}      # → last-active unix timestamp
        self._activity_wall: dict[str, float] = {} # → last-active wall-clock timestamp
        self._activity_persisted: dict[str, float] = {}  # → last DB write (wall clock)
        self._instances: dict[str, dict] = {}       # → provision result dict
        self._specs: dict[str, dict] = {}           # → resolved policy spec
        self._locks: dict[str, asyncio.Lock] = {}   # → per-key provisioning lock
        self._status_ok_at: dict[str, float] = {}   # → last confirmed-running check
        self._bg_tasks: set[asyncio.Task] = set()   # strong refs to persist tasks
        self._reaper_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def provision(
        self,
        user_id: str,
        policy_id: str = "default",
        spec: Optional[dict] = None,
    ) -> dict:
        """Create a new terminal instance for *user_id*.

        *policy_id* scopes the container (one per user+policy pair).
        *spec* is the resolved policy spec dict; if ``None``, the backend
        uses ``settings.*`` defaults.

        Returns a dict with at least:
        ``instance_id``, ``instance_name``, ``api_key``, ``host``, ``port``.
        """

    @abstractmethod
    async def start(self, instance_id: str) -> bool:
        """Idempotent start — no-op if already running."""

    @abstractmethod
    async def teardown(self, instance_id: str) -> None:
        """Stop and remove the instance."""

    @abstractmethod
    async def status(self, instance_id: str) -> str:
        """Return ``'running'``, ``'stopped'``, or ``'missing'``."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources on shutdown."""

    async def reset(
        self, user_id: str, policy_id: str, spec: Optional[dict] = None
    ) -> None:
        """Delete persisted files for a user terminal."""
        raise NotImplementedError("Reset is not supported by this backend")

    async def lookup(
        self, user_id: str, policy_id: str = "default"
    ) -> Optional[dict]:
        """Discover an already-running instance this process isn't tracking.

        Backends with deterministic instance names override this so that
        multiple worker processes adopt each other's containers instead of
        replacing them (which would kill live sessions). Returns the
        instance info dict, or ``None`` if nothing suitable exists.
        """
        return None

    # ------------------------------------------------------------------
    # Instance tracking
    # ------------------------------------------------------------------

    @staticmethod
    def _key(user_id: str, policy_id: str = "default") -> str:
        return f"{user_id}:{policy_id}"

    def _record_activity(self, key: str) -> None:
        self._activity[key] = time.monotonic()
        now_wall = time.time()
        self._activity_wall[key] = now_wall

        # Persist activity (debounced) so the idle reaper in *other* worker
        # processes doesn't tear down a terminal that is active here.
        last_persist = self._activity_persisted.get(key, 0.0)
        if now_wall - last_persist < self._ACTIVITY_PERSIST_INTERVAL:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._activity_persisted[key] = now_wall
        # Hold a strong reference — the loop only keeps weak refs, and a
        # GC'd task would silently drop the write.
        task = loop.create_task(self._persist_activity(key, now_wall))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _persist_activity(self, key: str, ts: float) -> None:
        """Best-effort write of last-active time to the shared database."""
        from terminals.db.session import async_session

        if async_session is None:
            return
        try:
            from terminals.models.activity import TerminalActivity

            user_id, _, policy_id = key.partition(":")
            async with async_session() as session:
                row = await session.get(TerminalActivity, key)
                if row is None:
                    session.add(
                        TerminalActivity(
                            id=key,
                            user_id=user_id,
                            policy_id=policy_id or "default",
                            last_active_at=ts,
                        )
                    )
                elif ts > (row.last_active_at or 0.0):
                    row.last_active_at = ts
                await session.commit()
        except Exception:
            # Roll the debounce marker back so the next request retries the
            # write instead of leaving the shared record stale for 60s.
            if self._activity_persisted.get(key) == ts:
                self._activity_persisted.pop(key, None)
            log.debug("Failed to persist activity for %s", key, exc_info=True)

    async def _load_persisted_activity(self, key: str) -> Optional[float]:
        """Read the cross-worker last-active timestamp (wall clock) for *key*.

        Returns ``None`` when no record exists. Database errors propagate —
        callers about to destroy an instance must fail closed, not open.
        """
        from terminals.db.session import async_session

        if async_session is None:
            return None
        from terminals.models.activity import TerminalActivity

        async with async_session() as session:
            row = await session.get(TerminalActivity, key)
            return row.last_active_at if row else None

    async def _idle_across_workers(self, key: str, spec: Optional[dict], now: float) -> bool:
        """True when *key* is idle locally AND per the shared activity record.

        The persisted timestamp lags real activity by up to
        ``_ACTIVITY_PERSIST_INTERVAL`` (debounce), so that interval is added
        as slack before declaring the terminal globally idle. Database
        errors fail closed (not idle) — tearing down an active terminal is
        worse than keeping an idle one an extra cycle.
        """
        if not self._is_idle_by_activity(key, spec, now):
            return False
        timeout_min = (spec or {}).get(
            "idle_timeout_minutes", settings.idle_timeout_minutes
        )
        try:
            persisted = await self._load_persisted_activity(key)
        except Exception:
            log.warning(
                "Could not read shared activity for %s; skipping idle teardown", key
            )
            return False
        if persisted is None:
            return True
        wall_idle = time.time() - persisted
        return wall_idle >= timeout_min * 60 + self._ACTIVITY_PERSIST_INTERVAL

    # ------------------------------------------------------------------
    # Status cache — avoid re-inspecting the container on every request
    # ------------------------------------------------------------------

    def _status_fresh(self, key: str) -> bool:
        ttl = settings.status_cache_ttl
        if ttl <= 0:
            return False
        checked = self._status_ok_at.get(key)
        return checked is not None and (time.monotonic() - checked) < ttl

    def _mark_status_ok(self, key: str) -> None:
        self._status_ok_at[key] = time.monotonic()

    def invalidate_status(self, user_id: str, policy_id: str = "default") -> None:
        """Force the next request for this key to re-verify instance status.

        Called by the proxy when it fails to connect to an instance that
        was assumed running.
        """
        self._status_ok_at.pop(self._key(user_id, policy_id), None)

    def drop_instance(self, user_id: str, policy_id: str = "default") -> None:
        """Stop tracking an unreachable instance without tearing it down.

        Only backends with discovery (an overridden :meth:`lookup`) actually
        forget the entry — their next request re-adopts the instance with
        freshly extracted host/port, or provisions anew if it is gone. For
        backends without discovery this only invalidates the status cache:
        forgetting would make the next request provision a replacement over
        a possibly-live instance.
        """
        if type(self).lookup is Backend.lookup:
            self.invalidate_status(user_id, policy_id)
            return
        self._forget(self._key(user_id, policy_id))

    def _forget(self, key: str) -> None:
        """Drop all tracking state for *key* (not the provisioning lock)."""
        self._instances.pop(key, None)
        self._specs.pop(key, None)
        self._activity.pop(key, None)
        self._activity_wall.pop(key, None)
        self._activity_persisted.pop(key, None)
        self._status_ok_at.pop(key, None)

    async def ensure_terminal(
        self,
        user_id: str,
        policy_id: str = "default",
        spec: Optional[dict] = None,
    ) -> Optional[dict]:
        """Get-or-create a terminal for *user_id*.

        Returns a dict with ``api_key``, ``host``, ``port``, or ``None``.
        Tracks the instance for idle reaping.

        Uses a per-key lock so concurrent requests for the same user+policy
        don't race to provision the same container.
        """
        key = self._key(user_id, policy_id)

        # Fast path — already tracked and running.
        info = self._instances.get(key)
        if info is not None:
            # Skip the backend status inspection while the last confirmed
            # check is fresh — at hundreds of users this is the difference
            # between pure dict lookups and 2 Docker API calls per request.
            if self._status_fresh(key):
                self._record_activity(key)
                return info
            st = await self.status(info["instance_id"])
            if st == "running":
                self._mark_status_ok(key)
                self._record_activity(key)
                return info

        # Serialise provisioning per key.
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        async with self._locks[key]:
            # Re-check after acquiring lock — another request may have
            # already provisioned while we were waiting.
            info = self._instances.get(key)
            if info is not None:
                st = await self.status(info["instance_id"])
                if st == "running":
                    self._mark_status_ok(key)
                    self._record_activity(key)
                    return info
                self._forget(key)

            # A due scheduled reset must not adopt the pre-reset container —
            # it tears down any survivor and provisions fresh. Otherwise,
            # another worker process may have provisioned this terminal
            # already; adopt it rather than replacing it.
            if not await self._apply_due_reset(
                user_id, policy_id, spec, teardown_existing=True
            ):
                adopted = await self.lookup(user_id, policy_id)
                if adopted:
                    self._instances[key] = adopted
                    self._specs[key] = spec or {}
                    self._mark_status_ok(key)
                    self._record_activity(key)
                    return adopted

            result = await self.provision(user_id, policy_id=policy_id, spec=spec)
            if result:
                self._instances[key] = result
                self._specs[key] = spec or {}
                self._mark_status_ok(key)
                self._record_activity(key)
            return result

    async def get_terminal_info(self, user_id: str) -> Optional[dict]:
        """Look up an existing terminal without creating one."""
        return None

    async def touch_activity(
        self, user_id: str, policy_id: str = "default"
    ) -> None:
        """Record that *user_id*'s terminal is actively being used."""
        key = self._key(user_id, policy_id)
        self._record_activity(key)

    async def _apply_due_reset(
        self,
        user_id: str,
        policy_id: str,
        spec: Optional[dict],
        *,
        teardown_existing: bool = False,
    ) -> bool:
        if not await reset_due_for(user_id, policy_id, spec):
            return False
        if teardown_existing:
            # Tear down any still-running container before wiping its files.
            stale = await self.lookup(user_id, policy_id)
            if stale:
                await self.teardown(stale["instance_id"])
        await self.reset(user_id, policy_id, spec)
        await mark_reset_applied(user_id, policy_id, spec)
        log.info("Reset files for user=%s policy=%s", user_id, policy_id)
        return True

    def _tracked_items(
        self,
        *,
        user_id: str | None = None,
        policy_id: str | None = None,
    ) -> list[tuple[str, str, str, dict, dict]]:
        matches = []
        for key, info in list(self._instances.items()):
            item_user, item_policy = key.split(":", 1)
            if user_id and item_user != user_id:
                continue
            if policy_id and item_policy != policy_id:
                continue
            matches.append((key, item_user, item_policy, info, self._specs.get(key, {})))
        return matches

    def _is_idle_by_activity(self, key: str, spec: Optional[dict], now: float) -> bool:
        timeout_min = (spec or {}).get(
            "idle_timeout_minutes", settings.idle_timeout_minutes
        )
        if not timeout_min or timeout_min <= 0:
            return False
        last_active = self._activity.get(key, now)
        return now - last_active >= timeout_min * 60

    async def refresh(
        self,
        *,
        user_id: str | None = None,
        policy_id: str | None = None,
        only_idle: bool = True,
        reset: bool = False,
    ) -> RefreshResult:
        """Tear down matching terminals so the next access provisions fresh."""
        result = RefreshResult()
        now = time.monotonic()

        for key, item_user, item_policy, info, spec in self._tracked_items(
            user_id=user_id, policy_id=policy_id
        ):
            result.matched += 1
            st = await self.status(info["instance_id"])
            idle = st != "running" or await self._idle_across_workers(key, spec, now)
            if only_idle and not idle:
                result.skipped_active += 1
                continue

            await self.teardown(info["instance_id"])
            self._forget(key)
            self._locks.pop(key, None)
            result.refreshed += 1

            if reset:
                await self.reset(item_user, item_policy, spec)
                result.reset += 1

        return result

    async def list_terminals(self) -> list[dict]:
        """Return sanitized tracked terminal instances for the admin UI."""
        rows = []
        now = time.monotonic()
        for key, user_id, policy_id, info, spec in self._tracked_items():
            status = await self.status(info["instance_id"])
            last_active = self._activity.get(key)
            last_active_wall = self._activity_wall.get(key)
            timeout_min = (spec or {}).get(
                "idle_timeout_minutes", settings.idle_timeout_minutes
            )
            rows.append(
                {
                    "user_id": user_id,
                    "policy_id": policy_id,
                    "status": status,
                    "instance_id": info.get("instance_id", ""),
                    "instance_name": info.get("instance_name", info.get("instance_id", "")),
                    "host": info.get("host", ""),
                    "port": info.get("port"),
                    "last_active_at": (
                        datetime.fromtimestamp(last_active_wall, timezone.utc).isoformat()
                        if last_active_wall
                        else None
                    ),
                    "idle_seconds": int(now - last_active) if last_active else None,
                    "idle_timeout_minutes": timeout_min or 0,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Idle reaper
    # ------------------------------------------------------------------

    def start_reaper(self) -> None:
        """Start the background idle-reaper task."""
        if self._reaper_task is not None:
            return
        self._reaper_task = asyncio.create_task(self._reaper_loop())
        log.info("Idle reaper started")

    async def stop_reaper(self) -> None:
        """Cancel the reaper and wait for it to finish."""
        if self._reaper_task is None:
            return
        self._reaper_task.cancel()
        try:
            await self._reaper_task
        except asyncio.CancelledError:
            pass
        self._reaper_task = None
        log.info("Idle reaper stopped")

    async def _reaper_loop(self) -> None:
        """Periodically check for idle terminals and tear them down."""
        while True:
            try:
                await asyncio.sleep(60)
                await self._reap_idle()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Idle reaper error")

    async def _reap_idle(self) -> None:
        """Scan tracked instances and tear down any that exceeded their timeout."""
        now = time.monotonic()

        for key in list(self._instances):
            info = self._instances.get(key)
            if info is None:
                continue

            spec = self._specs.get(key, {})
            timeout_min = spec.get(
                "idle_timeout_minutes", settings.idle_timeout_minutes
            )
            if not timeout_min or timeout_min <= 0:
                continue

            last_active = self._activity.get(key, now)
            idle_seconds = now - last_active

            # The cross-worker check consults the shared activity record so
            # traffic handled by another worker process blocks the teardown.
            if idle_seconds >= timeout_min * 60 and await self._idle_across_workers(
                key, spec, now
            ):
                parts = key.split(":", 1)
                user_id = parts[0]
                policy_id = parts[1] if len(parts) > 1 else "default"
                log.info(
                    "Reaping idle terminal %s (user=%s, policy=%s, idle=%.0fs, timeout=%dm)",
                    info.get("instance_name", info.get("instance_id")),
                    user_id,
                    policy_id,
                    idle_seconds,
                    timeout_min,
                )
                try:
                    await self.teardown(info["instance_id"])
                except Exception:
                    log.exception("Failed to tear down %s", key)
                try:
                    await self._apply_due_reset(user_id, policy_id, spec)
                except NotImplementedError:
                    log.warning("Reset due for %s but backend does not support it", key)
                except Exception:
                    log.exception("Failed to reset files for %s", key)
                self._forget(key)
                self._locks.pop(key, None)
