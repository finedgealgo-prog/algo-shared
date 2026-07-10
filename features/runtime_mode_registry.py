"""
runtime_mode_registry.py
────────────────────────
Shared in-memory registry for active live/fast-forward strategy snapshots.

Why this exists
───────────────
- The monitor/supervisor refreshes active strategies from DB.
- The broker tick dispatcher can cheaply decide which mode queues need work.
- Live stays highest priority because we avoid dispatching unnecessary work.
"""

from __future__ import annotations

from threading import Lock
from typing import Any


class _RuntimeModeRegistry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._enabled = False
        self._last_refresh_at = ''
        self._records_by_mode: dict[str, list[dict[str, Any]]] = {
            'live': [],
            'fast-forward': [],
            'forward-test': [],
        }

    def enable(self) -> None:
        with self._lock:
            self._enabled = True

    def disable(self) -> None:
        with self._lock:
            self._enabled = False
            self._last_refresh_at = ''
            self._records_by_mode = {
                'live': [],
                'fast-forward': [],
                'forward-test': [],
            }

    @property
    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def update(self, *, records_by_mode: dict[str, list[dict[str, Any]]], refreshed_at: str) -> None:
        normalized = {
            'live': list(records_by_mode.get('live') or []),
            'fast-forward': list(records_by_mode.get('fast-forward') or []),
            'forward-test': list(records_by_mode.get('forward-test') or []),
        }
        with self._lock:
            self._records_by_mode = normalized
            self._last_refresh_at = str(refreshed_at or '').strip()
            self._enabled = True

    def has_active_mode(self, activation_mode: str) -> bool:
        normalized_mode = str(activation_mode or '').strip()
        with self._lock:
            if not self._enabled:
                return False
            return bool(self._records_by_mode.get(normalized_mode) or [])

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            records_by_mode = {
                'live': [dict(item) for item in (self._records_by_mode.get('live') or [])],
                'fast-forward': [dict(item) for item in (self._records_by_mode.get('fast-forward') or [])],
                'forward-test': [dict(item) for item in (self._records_by_mode.get('forward-test') or [])],
            }
            return {
                'enabled': self._enabled,
                'last_refresh_at': self._last_refresh_at,
                'records_by_mode': records_by_mode,
                'counts': {
                    mode: len(records)
                    for mode, records in records_by_mode.items()
                },
            }


runtime_mode_registry = _RuntimeModeRegistry()

