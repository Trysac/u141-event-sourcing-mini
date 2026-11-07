# Architecture

## Core Domain Concepts

- `NewEvent`: An immutable event payload containing a `type` string and a JSON-serializable `data` dictionary.
- `StoredEvent`: An immutable event with assigned `stream_id`, 1-based stream `version`, and global 1-based `position`.
- `EventStore`: The storage interface defining append, stream read, global read, and snapshot operations.

## Storage Backends

1. `InMemoryEventStore`: Uses internal Python lists and dictionaries guarded by a `threading.Lock` to guarantee atomic append and read consistency across multiple threads.
2. `SqliteEventStore`: Uses SQLite with strict atomic transactions (`BEGIN IMMEDIATE ... COMMIT/ROLLBACK`) ensuring no concurrent writers interleave or create conflicting versions.

## Concurrency and Snapshots

- Stream versions start at 1.
- When appending, `expected_version` represents the expected stream version prior to append (`0` for a new stream). If the actual version differs, an `EventStoreError(code='concurrency_conflict')` is raised.
- Projections read across streams ordered strictly by `position`.
- `load_aggregate` applies snapshots if present and updates snapshots when `version - snapshot_version >= snapshot_every`.
