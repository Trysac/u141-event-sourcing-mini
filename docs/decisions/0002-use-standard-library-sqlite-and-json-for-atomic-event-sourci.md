# Use standard library SQLite and JSON for atomic event sourcing

Status: accepted (generated, pending human review)

## Context

Event sourcing requires atomic optimistic concurrency checks and global sequential ordering. Adding heavy third-party frameworks creates dependency baggage, while homegrown SQL without strict locks risks silent version interleaving.

## Decision

Implement in-memory and SQLite-backed event stores with standard library primitives (sqlite3, json, threading). SQLite operations use immediate transactions (BEGIN IMMEDIATE ... COMMIT/ROLLBACK) to serialize writes safely.

## Consequences

Zero external dependencies, robust multi-threading safety, and verifiable contract compliance across in-memory and SQLite storage implementations.
