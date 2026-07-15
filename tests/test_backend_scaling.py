import asyncio
import time
import unittest
from typing import Optional

from terminals.backends.base import Backend
from terminals.config import settings


class FakeBackend(Backend):
    """In-memory backend for exercising base-class scaling behaviour."""

    def __init__(self) -> None:
        super().__init__()
        self.status_calls = 0
        self.provision_calls = 0
        self.teardowns: list[str] = []
        self.status_result = "running"
        self.lookup_result: Optional[dict] = None
        self.reset_due = False

    async def provision(self, user_id, policy_id="default", spec=None):
        self.provision_calls += 1
        return {
            "instance_id": f"i-{user_id}-{policy_id}",
            "instance_name": f"n-{user_id}-{policy_id}",
            "api_key": "key",
            "host": "127.0.0.1",
            "port": 9999,
        }

    async def lookup(self, user_id, policy_id="default"):
        return self.lookup_result

    async def start(self, instance_id):
        return True

    async def teardown(self, instance_id):
        self.teardowns.append(instance_id)

    async def status(self, instance_id):
        self.status_calls += 1
        return self.status_result

    async def close(self):
        pass

    async def _persist_activity(self, key, ts):
        pass

    async def _load_persisted_activity(self, key):
        return None

    async def _apply_due_reset(self, user_id, policy_id, spec, **kwargs):
        if self.reset_due and kwargs.get("teardown_existing"):
            stale = await self.lookup(user_id, policy_id)
            if stale:
                await self.teardown(stale["instance_id"])
        return self.reset_due


class StatusCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._orig_ttl = settings.status_cache_ttl
        self.backend = FakeBackend()

    def tearDown(self) -> None:
        settings.status_cache_ttl = self._orig_ttl

    async def test_fresh_status_skips_backend_inspection(self) -> None:
        settings.status_cache_ttl = 30
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.provision_calls, 1)

        for _ in range(5):
            info = await self.backend.ensure_terminal("u1")
        self.assertIsNotNone(info)
        self.assertEqual(self.backend.status_calls, 0)
        self.assertEqual(self.backend.provision_calls, 1)

    async def test_ttl_zero_checks_every_request(self) -> None:
        settings.status_cache_ttl = 0
        await self.backend.ensure_terminal("u1")
        await self.backend.ensure_terminal("u1")
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.status_calls, 2)

    async def test_expired_status_rechecks(self) -> None:
        settings.status_cache_ttl = 30
        await self.backend.ensure_terminal("u1")
        # Age the cached check past the TTL.
        key = self.backend._key("u1", "default")
        self.backend._status_ok_at[key] = time.monotonic() - 31
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.status_calls, 1)

    async def test_invalidate_status_forces_recheck(self) -> None:
        settings.status_cache_ttl = 30
        await self.backend.ensure_terminal("u1")
        self.backend.invalidate_status("u1")
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.status_calls, 1)

    async def test_dead_instance_reprovisions_after_invalidation(self) -> None:
        settings.status_cache_ttl = 30
        await self.backend.ensure_terminal("u1")
        self.backend.status_result = "missing"
        self.backend.invalidate_status("u1")
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.provision_calls, 2)


class AdoptionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._orig_ttl = settings.status_cache_ttl
        settings.status_cache_ttl = 30
        self.backend = FakeBackend()

    def tearDown(self) -> None:
        settings.status_cache_ttl = self._orig_ttl

    async def test_existing_container_is_adopted_not_replaced(self) -> None:
        adopted = {
            "instance_id": "other-worker-instance",
            "instance_name": "terminals-abc",
            "api_key": "existing-key",
            "host": "127.0.0.1",
            "port": 4321,
        }
        self.backend.lookup_result = adopted

        info = await self.backend.ensure_terminal("u1")
        self.assertEqual(info, adopted)
        self.assertEqual(self.backend.provision_calls, 0)

    async def test_provisions_when_nothing_to_adopt(self) -> None:
        self.backend.lookup_result = None
        info = await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.provision_calls, 1)
        self.assertEqual(info["instance_id"], "i-u1-default")

    async def test_due_reset_skips_adoption_and_provisions_fresh(self) -> None:
        self.backend.reset_due = True
        self.backend.lookup_result = {
            "instance_id": "pre-reset-instance",
            "instance_name": "terminals-abc",
            "api_key": "old-key",
            "host": "127.0.0.1",
            "port": 4321,
        }
        info = await self.backend.ensure_terminal("u1")
        # The pre-reset container must not be adopted.
        self.assertEqual(info["instance_id"], "i-u1-default")
        self.assertEqual(self.backend.provision_calls, 1)
        self.assertEqual(self.backend.teardowns, ["pre-reset-instance"])

    async def test_drop_instance_forces_rediscovery(self) -> None:
        await self.backend.ensure_terminal("u1")
        self.assertEqual(self.backend.provision_calls, 1)

        self.backend.drop_instance("u1")
        adopted = {
            "instance_id": "rediscovered",
            "instance_name": "terminals-abc",
            "api_key": "k2",
            "host": "127.0.0.1",
            "port": 5000,
        }
        self.backend.lookup_result = adopted
        info = await self.backend.ensure_terminal("u1")
        self.assertEqual(info, adopted)
        self.assertEqual(self.backend.provision_calls, 1)


class ReaperCrossWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._orig_ttl = settings.status_cache_ttl
        self._orig_idle = settings.idle_timeout_minutes
        settings.status_cache_ttl = 30
        settings.idle_timeout_minutes = 30
        self.backend = FakeBackend()

    def tearDown(self) -> None:
        settings.status_cache_ttl = self._orig_ttl
        settings.idle_timeout_minutes = self._orig_idle

    async def _track_idle_terminal(self) -> str:
        await self.backend.ensure_terminal("u1")
        key = self.backend._key("u1", "default")
        # Locally the terminal looks long idle.
        self.backend._activity[key] = time.monotonic() - 3600
        return key

    async def test_reaper_tears_down_idle_terminal(self) -> None:
        await self._track_idle_terminal()
        await self.backend._reap_idle()
        self.assertEqual(self.backend.teardowns, ["i-u1-default"])

    async def test_reaper_trusts_activity_from_other_worker(self) -> None:
        await self._track_idle_terminal()

        async def recent_activity(_key):
            return time.time() - 60  # active a minute ago on another worker

        self.backend._load_persisted_activity = recent_activity
        await self.backend._reap_idle()
        self.assertEqual(self.backend.teardowns, [])

    async def test_reaper_allows_debounce_slack(self) -> None:
        # Persisted activity lags real activity by up to the 60s debounce —
        # a timestamp just past the timeout must NOT trigger a reap.
        await self._track_idle_terminal()
        timeout_seconds = 30 * 60

        async def just_past_timeout(_key):
            return time.time() - (timeout_seconds + 30)

        self.backend._load_persisted_activity = just_past_timeout
        await self.backend._reap_idle()
        self.assertEqual(self.backend.teardowns, [])

        async def well_past_timeout(_key):
            return time.time() - (timeout_seconds + 61)

        self.backend._load_persisted_activity = well_past_timeout
        await self.backend._reap_idle()
        self.assertEqual(self.backend.teardowns, ["i-u1-default"])

    async def test_reaper_fails_closed_on_db_error(self) -> None:
        await self._track_idle_terminal()

        async def db_down(_key):
            raise RuntimeError("database is locked")

        self.backend._load_persisted_activity = db_down
        await self.backend._reap_idle()
        self.assertEqual(self.backend.teardowns, [])


class ActivityDebounceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.backend = FakeBackend()
        self.persisted: list[tuple[str, float]] = []

        async def capture(key, ts):
            self.persisted.append((key, ts))

        self.backend._persist_activity = capture

    async def test_activity_writes_are_debounced(self) -> None:
        for _ in range(10):
            self.backend._record_activity("u1:default")
        await asyncio.sleep(0)
        self.assertEqual(len(self.persisted), 1)

    async def test_activity_writes_after_interval(self) -> None:
        self.backend._record_activity("u1:default")
        self.backend._activity_persisted["u1:default"] -= (
            Backend._ACTIVITY_PERSIST_INTERVAL + 1
        )
        self.backend._record_activity("u1:default")
        await asyncio.sleep(0)
        self.assertEqual(len(self.persisted), 2)

    async def test_failed_persist_rolls_back_debounce_marker(self) -> None:
        import terminals.db.session as db_session

        backend = FakeBackend()
        # Restore the real base-class persist implementation.
        backend._persist_activity = Backend._persist_activity.__get__(backend)

        class BoomSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                raise RuntimeError("db down")

            async def __aexit__(self, *args):
                return False

        original = db_session.async_session
        db_session.async_session = BoomSessionFactory()
        try:
            backend._activity_persisted["u1:default"] = 123.0
            await backend._persist_activity("u1:default", 123.0)
            # Marker rolled back so the next request retries the write.
            self.assertNotIn("u1:default", backend._activity_persisted)
        finally:
            db_session.async_session = original


if __name__ == "__main__":
    unittest.main()
