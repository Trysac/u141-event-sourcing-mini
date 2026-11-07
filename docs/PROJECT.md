# Project record

## Objective

Minimal event sourcing: append-only streams with optimistic concurrency, global positions, projections with checkpoints and snapshots, in memory or SQLite.

## Scope

- Support append-only streams with strict optimistic concurrency (atomic expected-version checks).
- Support global positioning for projections.
- Implement both In-Memory and SQLite backends using Python standard library only.
- Support snapshotting for efficient aggregate reconstruction.

## Out of Scope

- Subscriptions and real-time push.
- Upcasting of event versions.
- Support for backends other than In-Memory and SQLite (e.g., PostgreSQL).
- Deletion of streams.

## Factory run u141 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4259 | 10599 | 0 | 0 | 0.0215 |
| fix-1 | gemini-3.8-flash | no | 15927 | 773 | 0 | 18176 | 0.0162 |
| fix-2 | gemini-3.8-flash | no | 69319 | 697 | 0 | 448487 | 0.0882 |
| fix-3 | gemini-3.8-flash | no | 74023 | 2920 | 0 | 360781 | 0.0935 |
| fix-4 | gemini-3.8-flash | no | 60899 | 918 | 0 | 333568 | 0.0741 |
| audit | gemini-3.1-flash-lite | no | 64622 | 736 | 0 | 148412 | 0.0210 |
| docs | gemini-3.1-flash-lite | no | 168852 | 1204 | 0 | 0 | 0.0440 |

Total: $0.3586. Prices: Anthropic list prices.
