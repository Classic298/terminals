# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added multi-worker support (`TERMINALS_WORKERS` / `terminals serve --workers N`, Docker backend only) to scale the orchestrator beyond a single CPU core. Workers adopt each other's containers via deterministic names instead of replacing them, and share last-active timestamps through the database so the idle reaper never tears down a terminal that is active on another worker.
- Added `TERMINALS_STATUS_CACHE_TTL` (default `30`s) so the proxy hot path no longer inspects the container on every request. On connection failure the proxy invalidates the cache and re-resolves the instance mid-request, so a died or replaced container heals transparently.

### Changed
- Disabled WebSocket permessage-deflate compression by default on both proxy legs; compressing every terminal frame in pure Python dominated orchestrator CPU at high session counts. Re-enable with `TERMINALS_WS_COMPRESSION=true`.
- Docker provisioning no longer replaces an existing same-name container (which killed live sessions when worker processes raced); it adopts the running container instead.
- SQLite databases are now opened in WAL mode (`synchronous=NORMAL`, 30s busy timeout), and startup migrations are serialized across worker processes with a file lock.

### Fixed
- Fixed the active WebSocket connection counter leaking on failed connection attempts.

## [0.0.5] - 2026-07-09

### Added
- Added Kubernetes node selector and toleration overrides for terminal and reset pods.

## [0.0.4] - 2026-06-29

### Added
- Added a minimal admin UI for viewing terminal status, active sessions, and policies.
- Added policy lifecycle support, including scheduled resets and lifecycle state tracking.
- Added OpenShift-focused security context controls and deployment documentation.
- Added frontend build packaging to the server Docker image.
- Added terminal environment propagation for system prompts and resource metadata.
- Added configurable server and operator log levels.

### Fixed
- Fixed Docker backend storage limit handling with a best-effort fallback when the host driver cannot enforce quotas.
- Fixed stale proxy connection handling by retrying once after keep-alive failures.
- Fixed Kubernetes and operator provisioning paths to pass effective policy environment values consistently.

## [0.0.1] - 2026-04-02

### Added
- Multi-tenant terminal orchestrator with Docker and Kubernetes backends.
- Kubernetes operator for terminal custom resource management.
- CLI interface for managing terminals.
- Docker build workflows for orchestrator and operator images (multi-arch: amd64/arm64).
