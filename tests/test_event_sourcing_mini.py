"""Comprehensive tests for trysac-event-sourcing-mini exercising all contract rules."""

from __future__ import annotations

import sqlite3
import threading
from typing import Any

import pytest

from event_sourcing_mini import (
    EventStoreError,
    InMemoryEventStore,
    NewEvent,
    Projector,
    SqliteEventStore,
    StoredEvent,
    load_aggregate,
)


def test_acceptance_1_and_concurrency_conflict() -> None:
    store = InMemoryEventStore()
    v1 = store.append("order-1", [NewEvent("Created", {"id": 1})], expected_version=0)
    assert v1 == 1

    with pytest.raises(EventStoreError) as exc_info:
        store.append("order-1", [NewEvent("Created", {"id": 1})], expected_version=0)
    err = exc_info.value
    assert err.code == "concurrency_conflict"
    assert err.expected == 0
    assert err.actual == 1

    stream_events = store.read("order-1")
    assert len(stream_events) == 1
    assert stream_events[0].type == "Created"
    assert stream_events[0].data == {"id": 1}


def test_acceptance_2_expected_version_none_and_read_from_version() -> None:
    store = InMemoryEventStore()
    store.append("order-1", [NewEvent("Created", {"id": 1})], expected_version=0)
    store.append(
        "order-1",
        [NewEvent("Updated", {"id": 1, "step": 1}), NewEvent("Closed", {"id": 1})],
        expected_version=None,
    )

    events = store.read("order-1", from_version=2)
    assert len(events) == 2
    ev2, ev3 = events[0], events[1]
    assert ev2.stream_id == "order-1"
    assert ev2.version == 2
    assert ev2.position == 2
    assert ev2.type == "Updated"
    assert ev2.data == {"id": 1, "step": 1}

    assert ev3.stream_id == "order-1"
    assert ev3.version == 3
    assert ev3.position == 3
    assert ev3.type == "Closed"
    assert ev3.data == {"id": 1}


def test_acceptance_3_positions_global_and_read_all() -> None:
    store = InMemoryEventStore()
    store.append("stream-A", [NewEvent("A1", {}), NewEvent("A2", {})])
    store.append("stream-B", [NewEvent("B1", {}), NewEvent("B2", {})])

    all_events = store.read_all(after_position=2, limit=10)
    assert len(all_events) == 2
    assert all_events[0].position == 3
    assert all_events[0].stream_id == "stream-B"
    assert all_events[0].type == "B1"
    assert all_events[1].position == 4
    assert all_events[1].stream_id == "stream-B"
    assert all_events[1].type == "B2"


def test_acceptance_4_projector_checkpoint() -> None:
    store = InMemoryEventStore()
    store.append("order-1", [NewEvent("Created", {}), NewEvent("Paid", {})])
    store.append(
        "order-2",
        [NewEvent("Created", {}), NewEvent("Paid", {}), NewEvent("Paid", {})],
    )

    def inc_created(state: dict[str, int], ev: StoredEvent) -> dict[str, int]:
        state = dict(state)
        state["Created"] = state.get("Created", 0) + 1
        return state

    def inc_paid(state: dict[str, int], ev: StoredEvent) -> dict[str, int]:
        state = dict(state)
        state["Paid"] = state.get("Paid", 0) + 1
        return state

    projector: Projector[dict[str, int]] = Projector(
        {"Created": inc_created, "Paid": inc_paid},
        initial={"Created": 0, "Paid": 0},
    )

    state, cp = projector.run(store, after_position=0, batch=2)
    assert state == {"Created": 2, "Paid": 3}
    assert cp == 5

    call_count = 0

    def tracking_created(s: dict[str, int], ev: StoredEvent) -> dict[str, int]:
        nonlocal call_count
        call_count += 1
        return inc_created(s, ev)

    projector2: Projector[dict[str, int]] = Projector(
        {"Created": tracking_created, "Paid": inc_paid},
        initial=state,
    )
    state2, cp2 = projector2.run(store, after_position=5)
    assert state2 == {"Created": 2, "Paid": 3}
    assert cp2 == 5
    assert call_count == 0

    # Projector with events of other types skipped but advancing checkpoint
    store.append("order-3", [NewEvent("OtherType", {})])
    state_other, cp_other = projector.run(store, after_position=5)
    assert state_other == {"Created": 0, "Paid": 0}
    assert cp_other == 6


def test_acceptance_5_load_aggregate_snapshot_every() -> None:
    store = InMemoryEventStore()
    for i in range(1, 6):
        store.append("order-1", [NewEvent("Updated", {"v": i})])

    calls = 0

    def apply(state: dict[str, Any], ev: StoredEvent) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        res = dict(state)
        res["count"] = res.get("count", 0) + 1
        return res

    s1, v1 = load_aggregate(store, "order-1", {"count": 0}, apply, snapshot_every=2)
    assert calls == 5
    assert v1 == 5
    assert s1 == {"count": 5}
    snap = store.load_snapshot("order-1")
    assert snap is not None
    assert snap[0] == 5
    assert snap[1] == {"count": 5}

    # Second load: snapshot at 5 matches stream version 5 -> apply called 0 times
    calls = 0
    s2, v2 = load_aggregate(store, "order-1", {"count": 0}, apply, snapshot_every=2)
    assert calls == 0
    assert v2 == 5
    assert s2 == {"count": 5}

    # Append one more event (version 6)
    store.append("order-1", [NewEvent("Updated", {"v": 6})])

    # Third load: calls apply once
    calls = 0
    s3, v3 = load_aggregate(store, "order-1", {"count": 0}, apply, snapshot_every=2)
    assert calls == 1
    assert v3 == 6
    assert s3 == {"count": 6}


def test_acceptance_6_sqlite_contract_and_atomic_rollback() -> None:
    conn = sqlite3.connect(":memory:")
    store = SqliteEventStore(conn)

    # Verify SQLite behaves identically on append, concurrency, and read
    v1 = store.append("order-1", [NewEvent("Created", {"id": 1})], expected_version=0)
    assert v1 == 1

    with pytest.raises(EventStoreError) as exc_info:
        store.append("order-1", [NewEvent("Created", {"id": 1})], expected_version=0)
    assert exc_info.value.code == "concurrency_conflict"
    assert exc_info.value.expected == 0
    assert exc_info.value.actual == 1

    # Conflicting append of 3 events leaves 0 of them stored
    with pytest.raises(EventStoreError):
        store.append(
            "order-1",
            [
                NewEvent("E1", {"k": 1}),
                NewEvent("E2", {"k": 2}),
                NewEvent("E3", {"k": 3}),
            ],
            expected_version=99,
        )

    events = store.read("order-1")
    assert len(events) == 1

    # Snapshots in SQLite
    store.save_snapshot("order-1", 1, {"status": "open"})
    snap = store.load_snapshot("order-1")
    assert snap is not None
    assert snap[0] == 1
    assert snap[1] == {"status": "open"}


def test_acceptance_7_validation_and_errors() -> None:
    # Event JSON > 1_048_576 bytes
    large_store = InMemoryEventStore()
    big_payload = {"large": "x" * (1_048_576 + 10)}
    with pytest.raises(EventStoreError) as exc1:
        large_store.append("stream-1", [NewEvent("Big", big_payload)])
    assert exc1.value.code == "event_too_large"

    # Empty stream id
    with pytest.raises(EventStoreError) as exc2:
        large_store.append("", [NewEvent("Valid", {})])
    assert exc2.value.code == "invalid_stream_id"

    # Invalid stream id characters or too long
    with pytest.raises(EventStoreError) as exc2b:
        large_store.append("stream!invalid", [NewEvent("Valid", {})])
    assert exc2b.value.code == "invalid_stream_id"

    with pytest.raises(EventStoreError) as exc2c:
        large_store.append("a" * 201, [NewEvent("Valid", {})])
    assert exc2c.value.code == "invalid_stream_id"

    # NaN in event data
    with pytest.raises(EventStoreError) as exc3:
        large_store.append("stream-1", [NewEvent("NaNEvent", {"x": float("nan")})])
    assert exc3.value.code == "invalid_event"

    # Store full
    cap_store = InMemoryEventStore(max_events=3)
    cap_store.append("stream-1", [NewEvent("E1", {}), NewEvent("E2", {}), NewEvent("E3", {})])
    with pytest.raises(EventStoreError) as exc4:
        cap_store.append("stream-1", [NewEvent("E4", {})])
    assert exc4.value.code == "store_full"


def test_edge_cases_and_adversarial_inputs() -> None:
    store = InMemoryEventStore()

    # Empty events list raises invalid_event
    with pytest.raises(EventStoreError) as exc_empty:
        store.append("stream-1", [])
    assert exc_empty.value.code == "invalid_event"

    # Unknown stream returns empty list
    assert store.read("unknown-stream") == []
    assert store.load_snapshot("unknown-stream") is None

    # read_all with limit 0
    assert store.read_all(limit=0) == []

    # Unicode data
    unicode_data = {"greeting": "こんにちは世界", "emoji": "🚀✨", "special": "café résumé"}
    store.append("unicode-stream", [NewEvent("Greet", unicode_data)])
    read_back = store.read("unicode-stream")
    assert len(read_back) == 1
    assert read_back[0].data == unicode_data

    # Snapshot mismatch detection
    store.save_snapshot("mismatch-stream", 10, {"state": 1})
    with pytest.raises(EventStoreError) as exc_mismatch:
        load_aggregate(store, "mismatch-stream", {}, lambda s, e: s)
    assert exc_mismatch.value.code == "snapshot_mismatch"

    # Custom to_dict / from_dict in load_aggregate
    class Counter:
        def __init__(self, val: int) -> None:
            self.val = val

    c, v = load_aggregate(
        store,
        "counter-stream",
        Counter(0),
        lambda c, ev: Counter(c.val + 1),
        to_dict=lambda c: {"val": c.val},
        from_dict=lambda d: Counter(d["val"]),
        snapshot_every=1,
    )
    assert v == 0
    assert c.val == 0

    store.append("counter-stream", [NewEvent("Inc", {})])
    c, v = load_aggregate(
        store,
        "counter-stream",
        Counter(0),
        lambda c, ev: Counter(c.val + 1),
        to_dict=lambda c: {"val": c.val},
        from_dict=lambda d: Counter(d["val"]),
        snapshot_every=1,
    )
    assert v == 1
    assert c.val == 1

    # Re-loading with custom serializers uses snapshot
    c2, v2 = load_aggregate(
        store,
        "counter-stream",
        Counter(0),
        lambda c, ev: Counter(c.val + 1),
        to_dict=lambda c: {"val": c.val},
        from_dict=lambda d: Counter(d["val"]),
    )
    assert v2 == 1
    assert c2.val == 1

    # Non-dict state when to_dict is None raises invalid_event
    store.append("non-dict-stream", [NewEvent("Inc", {})])
    with pytest.raises(EventStoreError) as exc_non_dict:
        load_aggregate(
            store,
            "non-dict-stream",
            Counter(0),
            lambda c, ev: Counter(c.val + 1),
            snapshot_every=1,
        )
    assert exc_non_dict.value.code == "invalid_event"


def test_sqlite_in_transaction_check() -> None:
    conn = sqlite3.connect(":memory:")
    store = SqliteEventStore(conn)
    # Put connection into an active transaction outside
    conn.isolation_level = None  # autocommit off when issuing BEGIN manually
    conn.execute("BEGIN TRANSACTION")
    with pytest.raises(EventStoreError) as exc:
        store.append("stream-1", [NewEvent("Ev", {})])
    assert exc.value.code == "in_transaction"
    conn.execute("ROLLBACK")


def test_sqlite_read_all_and_limits() -> None:
    conn = sqlite3.connect(":memory:")
    store = SqliteEventStore(conn)
    store.append("stream-1", [NewEvent("E1", {}), NewEvent("E2", {})])
    store.append("stream-2", [NewEvent("E3", {})])

    assert store.read_all(limit=0) == []
    res = store.read_all(after_position=1, limit=2)
    assert len(res) == 2
    assert res[0].position == 2
    assert res[1].position == 3

    # Snapshot update
    assert store.load_snapshot("unknown-stream") is None
    store.save_snapshot("stream-1", 1, {"count": 1})
    store.save_snapshot("stream-1", 2, {"count": 2})
    snap = store.load_snapshot("stream-1")
    assert snap == (2, {"count": 2})


def test_in_memory_multithreaded_appends() -> None:
    store = InMemoryEventStore(max_events=2000)
    num_threads = 2
    appends_per_thread = 50
    errors: list[Exception] = []

    def worker(worker_id: int) -> None:
        try:
            for i in range(appends_per_thread):
                store.append(
                    f"worker-{worker_id}",
                    [NewEvent("WorkDone", {"step": i})],
                )
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    all_events = store.read_all(limit=200)
    assert len(all_events) == num_threads * appends_per_thread
    positions = [e.position for e in all_events]
    assert positions == list(range(1, 101))


def test_invalid_event_types_and_payloads() -> None:
    store = InMemoryEventStore()
    # Non-string or too long event type
    with pytest.raises(EventStoreError) as exc_type1:
        store.append("stream-1", [NewEvent("", {})])  # type: ignore[arg-type]
    assert exc_type1.value.code == "invalid_event"

    with pytest.raises(EventStoreError) as exc_type2:
        store.append("stream-1", [NewEvent("A" * 101, {})])
    assert exc_type2.value.code == "invalid_event"

    # Non-dict data
    with pytest.raises(EventStoreError) as exc_data:
        store.append("stream-1", [NewEvent("Valid", "not-a-dict")])  # type: ignore[arg-type]
    assert exc_data.value.code == "invalid_event"

    # Invalid item in events list
    with pytest.raises(EventStoreError) as exc_item:
        store.append("stream-1", ["not-new-event"])  # type: ignore[list-item]
    assert exc_item.value.code == "invalid_event"

    # Max events invalid init
    with pytest.raises(ValueError):
        InMemoryEventStore(max_events=0)
