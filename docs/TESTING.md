# Testing

Tests are located in `tests/` and cover all contract requirements for both `InMemoryEventStore` and `SqliteEventStore`.

- `test_acceptance_1_and_concurrency_conflict`: Validates version checking and concurrency exceptions.
- `test_acceptance_2_expected_version_none_and_read_from_version`: Confirms basic append and version-based reading.
- `test_acceptance_3_positions_global_and_read_all`: Verifies global order and position-based scanning.
- `test_acceptance_4_projector_checkpoint`: Tests state aggregation and checkpointing.
- `test_acceptance_all`: Integration tests for snapshots and error condition handling (e.g., event size, max capacity, invalid data).