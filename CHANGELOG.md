# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.1] — 2026-05-31

### Fixed
- Added `py.typed` marker so mypy can resolve types from installed package.
- Fixed `__init__.py` lazy imports causing `"object" not callable` in type checkers.
- Fixed bare `dict` annotation in `examples/async_server.py`.

## [0.1.0] — Unreleased

### Added
- Initial PyPI release of `rise-l-net`.
- Synchronous device client (`RISELDevice`) and server (`RISELServer`).
- Asynchronous device client (`AsyncRISELDevice`) and server (`AsyncRISELServer`) backed by aiohttp.
- Middleware framework on both client and server.
- Plugin framework on the server (Webhook, Alert).
- Storage abstraction with in-memory and SQLite backends, sync and async.
- Constant-time API-key authentication; rate limiting; payload validation.
- MicroPython-compatible synchronous client.
- pytest test suite covering storage, middleware, sync/async server, and end-to-end client→server flows.
