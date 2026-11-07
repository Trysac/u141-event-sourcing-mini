"""Minimal event sourcing: append-only streams with optimistic concurrency,
global positions, projections with checkpoints and snapshots, in memory or SQLite.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, Protocol, TypeVar

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "0.1.0"

__all__ = [
    "EventStore",
    "EventStoreError",
    "EventStoreErrorCode",
    "InMemoryEventStore",
    "NewEvent",
    "Projector",
    "SqliteEventStore",
    "StoredEvent",
    "__version__",
    "load_aggregate",
]

EventStoreErrorCode = Literal[
    "concurrency_conflict",
    "invalid_stream_id",
    "invalid_event",
    "event_too_large",
    "store_full",
    "snapshot_mismatch",
    "in_transaction",
]

STREAM_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
MAX_EVENT_JSON_BYTES = 1_048_576
MAX_TYPE_LENGTH = 100


class EventStoreError(Exception):
    """Base exception raised by event store operations."""

    def __init__(
        self,
        code: EventStoreErrorCode,
        message: str,
        *,
        expected: int | None = None,
        actual: int | None = None,
    ) -> None:
        truncated_msg = message[:200]
        super().__init__(truncated_msg)
        self.code: EventStoreErrorCode = code
        self.expected: int | None = expected
        self.actual: int | None = actual


@dataclass(frozen=True)
class NewEvent:
    """An event to be appended to a stream."""

    type: str
    data: dict[str, Any]


@dataclass(frozen=True)
class StoredEvent:
    """An immutable event stored in the event store."""

    stream_id: str
    version: int
    position: int
    type: str
    data: dict[str, Any]


def _validate_stream_id(stream_id: str) -> None:
    if not isinstance(stream_id, str) or not STREAM_ID_PATTERN.match(stream_id):
        raise EventStoreError(
            "invalid_stream_id",
            f"Stream id must match [A-Za-z0-9_.:-]{{1,200}}, got: {stream_id!r}",
        )


def _encode_event_data(type_: str, data: dict[str, Any]) -> str:
    if not isinstance(type_, str) or len(type_) < 1 or len(type_) > MAX_TYPE_LENGTH:
        raise EventStoreError(
            "invalid_event",
            f"Event type must be 1-{MAX_TYPE_LENGTH} characters, got: {type_!r}",
        )
    if not isinstance(data, dict):
        raise EventStoreError("invalid_event", "Event data must be a dictionary")
    try:
        encoded = json.dumps(
            data,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (ValueError, TypeError) as exc:
        raise EventStoreError("invalid_event", f"Invalid event data JSON: {exc}") from exc

    encoded_bytes = encoded.encode("utf-8")
    if len(encoded_bytes) > MAX_EVENT_JSON_BYTES:
        raise EventStoreError(
            "event_too_large",
            f"Event JSON size {len(encoded_bytes)} exceeds limit {MAX_EVENT_JSON_BYTES}",
        )
    return encoded


def _validate_events(events: Sequence[NewEvent]) -> None:
    if not events:
        raise EventStoreError("invalid_event", "Events sequence cannot be empty")
    for ev in events:
        if not isinstance(ev, NewEvent):
            raise EventStoreError("invalid_event", "Event must be an instance of NewEvent")
        _encode_event_data(ev.type, ev.data)


class EventStore(Protocol):
    """Protocol defining the capabilities of an event store."""

    def append(
        self,
        stream_id: str,
        events: Sequence[NewEvent],
        *,
        expected_version: int | None = None,
    ) -> int:
        """Append events to a stream and return the new stream version."""
        ...

    def read(self, stream_id: str, *, from_version: int = 1) -> list[StoredEvent]:
        """Read events from a stream starting from from_version (1-based)."""
        ...

    def read_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        """Read events globally after a given global position in position order."""
        ...

    def save_snapshot(self, stream_id: str, version: int, state: dict[str, Any]) -> None:
        """Save a snapshot for a stream at a specific version."""
        ...

    def load_snapshot(self, stream_id: str) -> tuple[int, dict[str, Any]] | None:
        """Load the latest snapshot for a stream, returning (version, state) or None."""
        ...


class InMemoryEventStore:
    """Thread-safe in-memory event store implementation."""

    def __init__(self, *, max_events: int = 1_000_000) -> None:
        if max_events < 1:
            raise ValueError("max_events must be positive")
        self._max_events = max_events
        self._lock = threading.Lock()
        self._global_events: list[StoredEvent] = []
        self._stream_events: dict[str, list[StoredEvent]] = {}
        self._snapshots: dict[str, tuple[int, dict[str, Any]]] = {}

    def append(
        self,
        stream_id: str,
        events: Sequence[NewEvent],
        *,
        expected_version: int | None = None,
    ) -> int:
        _validate_stream_id(stream_id)
        _validate_events(events)

        with self._lock:
            if len(self._global_events) + len(events) > self._max_events:
                raise EventStoreError(
                    "store_full",
                    f"Store capacity {self._max_events} exceeded",
                )

            stream = self._stream_events.get(stream_id, [])
            current_version = len(stream)

            if expected_version is not None and current_version != expected_version:
                raise EventStoreError(
                    "concurrency_conflict",
                    f"Concurrency conflict on stream {stream_id}: "
                    f"expected {expected_version}, got {current_version}",
                    expected=expected_version,
                    actual=current_version,
                )

            new_stored: list[StoredEvent] = []
            for i, ev in enumerate(events):
                encoded_json = _encode_event_data(ev.type, ev.data)
                data_copy = json.loads(encoded_json)
                stored = StoredEvent(
                    stream_id=stream_id,
                    version=current_version + i + 1,
                    position=len(self._global_events) + 1,
                    type=ev.type,
                    data=data_copy,
                )
                self._global_events.append(stored)
                new_stored.append(stored)

            if stream_id not in self._stream_events:
                self._stream_events[stream_id] = []
            self._stream_events[stream_id].extend(new_stored)
            logger.debug("Appended %d events to stream %s", len(new_stored), stream_id)
            return current_version + len(events)

    def read(self, stream_id: str, *, from_version: int = 1) -> list[StoredEvent]:
        _validate_stream_id(stream_id)
        with self._lock:
            stream = self._stream_events.get(stream_id, [])
            if from_version <= 1:
                return list(stream)
            return [e for e in stream if e.version >= from_version]

    def read_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        if limit <= 0:
            return []
        with self._lock:
            res: list[StoredEvent] = []
            for e in self._global_events:
                if e.position > after_position:
                    res.append(e)
                    if len(res) == limit:
                        break
            return res

    def save_snapshot(self, stream_id: str, version: int, state: dict[str, Any]) -> None:
        _validate_stream_id(stream_id)
        encoded = _encode_event_data("snapshot", state)
        state_copy = json.loads(encoded)
        with self._lock:
            self._snapshots[stream_id] = (version, state_copy)

    def load_snapshot(self, stream_id: str) -> tuple[int, dict[str, Any]] | None:
        _validate_stream_id(stream_id)
        with self._lock:
            snap = self._snapshots.get(stream_id)
            if snap is None:
                return None
            version, state = snap
            return (version, json.loads(json.dumps(state)))


class SqliteEventStore:
    """SQLite-backed event store implementation."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._init_tables()

    def _init_tables(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    stream_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    data TEXT NOT NULL,
                    UNIQUE(stream_id, version)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    stream_id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    state TEXT NOT NULL
                )
                """
            )

    def append(
        self,
        stream_id: str,
        events: Sequence[NewEvent],
        *,
        expected_version: int | None = None,
    ) -> int:
        _validate_stream_id(stream_id)
        _validate_events(events)

        if self._conn.in_transaction:
            raise EventStoreError("in_transaction", "Connection already in a transaction")

        cursor = self._conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                (stream_id,),
            )
            row = cursor.fetchone()
            current_version: int = row[0] if row else 0

            if expected_version is not None and current_version != expected_version:
                cursor.execute("ROLLBACK")
                raise EventStoreError(
                    "concurrency_conflict",
                    f"Concurrency conflict on stream {stream_id}: "
                    f"expected {expected_version}, got {current_version}",
                    expected=expected_version,
                    actual=current_version,
                )

            for i, ev in enumerate(events):
                encoded = _encode_event_data(ev.type, ev.data)
                version = current_version + i + 1
                cursor.execute(
                    "INSERT INTO events(stream_id, version, type, data) VALUES (?, ?, ?, ?)",
                    (stream_id, version, ev.type, encoded),
                )

            cursor.execute("COMMIT")
            new_version = current_version + len(events)
            logger.debug("Appended %d events to stream %s in SQLite", len(events), stream_id)
            return new_version
        except Exception:
            try:
                if self._conn.in_transaction:
                    cursor.execute("ROLLBACK")
            except Exception as rollback_err:  # pragma: no cover
                logger.debug("Rollback failed: %s", rollback_err)
            raise

    def read(self, stream_id: str, *, from_version: int = 1) -> list[StoredEvent]:
        _validate_stream_id(stream_id)
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT stream_id, version, position, type, data
            FROM events
            WHERE stream_id = ? AND version >= ?
            ORDER BY version ASC
            """,
            (stream_id, from_version),
        )
        rows = cursor.fetchall()
        return [
            StoredEvent(
                stream_id=r[0],
                version=r[1],
                position=r[2],
                type=r[3],
                data=json.loads(r[4]),
            )
            for r in rows
        ]

    def read_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        if limit <= 0:
            return []
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT stream_id, version, position, type, data
            FROM events
            WHERE position > ?
            ORDER BY position ASC
            LIMIT ?
            """,
            (after_position, limit),
        )
        rows = cursor.fetchall()
        return [
            StoredEvent(
                stream_id=r[0],
                version=r[1],
                position=r[2],
                type=r[3],
                data=json.loads(r[4]),
            )
            for r in rows
        ]

    def save_snapshot(self, stream_id: str, version: int, state: dict[str, Any]) -> None:
        _validate_stream_id(stream_id)
        encoded = _encode_event_data("snapshot", state)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO snapshots(stream_id, version, state)
                VALUES (?, ?, ?)
                ON CONFLICT(stream_id) DO UPDATE SET
                    version = excluded.version,
                    state = excluded.state
                """,
                (stream_id, version, encoded),
            )

    def load_snapshot(self, stream_id: str) -> tuple[int, dict[str, Any]] | None:
        _validate_stream_id(stream_id)
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT version, state FROM snapshots WHERE stream_id = ?",
            (stream_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return (row[0], json.loads(row[1]))


S = TypeVar("S")


class Projector(Generic[S]):
    """Projects streams into a read model by reading events across all streams."""

    def __init__(
        self,
        handlers: Mapping[str, Callable[[S, StoredEvent], S]],
        initial: S,
    ) -> None:
        self._handlers = handlers
        self._initial = initial

    def run(
        self,
        store: EventStore,
        *,
        after_position: int = 0,
        batch: int = 1000,
    ) -> tuple[S, int]:
        """Process events starting after `after_position` in batches."""
        state = self._initial
        checkpoint = after_position
        while True:
            events = store.read_all(after_position=checkpoint, limit=batch)
            if not events:
                break
            for ev in events:
                handler = self._handlers.get(ev.type)
                if handler is not None:
                    state = handler(state, ev)
                checkpoint = ev.position
        return state, checkpoint


def load_aggregate(
    store: EventStore,
    stream_id: str,
    initial: S,
    apply: Callable[[S, StoredEvent], S],
    *,
    snapshot_every: int = 0,
    to_dict: Callable[[S], dict[str, Any]] | None = None,
    from_dict: Callable[[dict[str, Any]], S] | None = None,
) -> tuple[S, int]:
    """Reconstruct an aggregate from its stream, optionally leveraging snapshots."""
    _validate_stream_id(stream_id)
    state = initial
    from_version = 1
    snapshot_version = 0

    snap = store.load_snapshot(stream_id)
    if snap is not None:
        snap_ver, snap_dict = snap
        # Validate snapshot does not exceed stream version before attempting deserialization
        # A snapshot whose version is above the stream version raises 'snapshot_mismatch'
        all_events = store.read(stream_id, from_version=1)
        total_stream_ver = len(all_events)
        if snap_ver > total_stream_ver:
            msg = f"Snapshot version {snap_ver} exceeds stream version {total_stream_ver}"
            raise EventStoreError("snapshot_mismatch", msg)
        state = from_dict(snap_dict) if from_dict is not None else snap_dict  # type: ignore[assignment]
        snapshot_version = snap_ver
        from_version = snap_ver + 1

    events = store.read(stream_id, from_version=from_version)

    current_version = snapshot_version
    if events:
        current_version = events[-1].version

    for ev in events:
        state = apply(state, ev)

    if snapshot_every > 0 and (current_version - snapshot_version) >= snapshot_every:
        if to_dict is not None:
            saved_dict = to_dict(state)
        else:
            if not isinstance(state, dict):
                raise EventStoreError(
                    "invalid_event",
                    "Aggregate state must be a dict when to_dict is None",
                )
            saved_dict = state
        store.save_snapshot(stream_id, current_version, saved_dict)

    return state, current_version
