# event-sourcing-mini

Teams trying event sourcing either adopt a heavy framework or write an events table without optimistic concurrency, so two writers silently interleave and aggregates rebuild from the first event on every request. The core rules are small but must be exact. This library provides append-only streams with expected-version checks, a global ordering for projections and snapshots, backed by memory or the standard library's sqlite3.

## Installation

```bash
pip install event-sourcing-mini
```

## Usage

```python
from event_sourcing_mini import InMemoryEventStore, NewEvent

store = InMemoryEventStore()
# Appending creates the first version (1)
new_version = store.append("order-1", [NewEvent("Created", {"total": 50})], expected_version=0)
events = store.read("order-1")
assert new_version == 1 and events[0].data["total"] == 50
```

## API Summary

- `NewEvent(type: str, data: dict)`: Payload to append.
- `StoredEvent(stream_id, version, position, type, data)`: Immutable record of an event.
- `EventStore`: Protocol defining `append`, `read`, `read_all`, `save_snapshot`, `load_snapshot`.
- `InMemoryEventStore(max_events=1_000_000)`: Thread-safe in-memory backend.
- `SqliteEventStore(conn)`: SQLite backend utilizing `BEGIN IMMEDIATE` transactions.
- `Projector`: Reconstructs state from global event streams.
- `load_aggregate`: Reconstructs aggregates with snapshot support.

Errors (class `EventStoreError`) use codes: `concurrency_conflict`, `invalid_stream_id`, `invalid_event`, `event_too_large`, `store_full`, `snapshot_mismatch`, `in_transaction`.

## Why not <best-known alternative>?

A few hundred lines with exact concurrency rules and a stdlib SQLite backend for small services and teaching, rather than the heavyweight architecture of `eventsourcing` or `EventStoreDB`.

See [SECURITY.md](docs/SECURITY.md) for vulnerability details.