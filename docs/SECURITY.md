# Security

## Threat Model

### Defended

- **Injection**: Stream IDs are validated via regex `^[A-Za-z0-9_.:-]{1,200}$`.
- **Resource Exhaustion**: `InMemoryEventStore` bounds storage to a configurable `max_events` limit; events are limited to a JSON-serialized size of 1,048,576 bytes.
- **Data Corruption**: SQLite backends utilize `BEGIN IMMEDIATE` transactions to prevent write interleaving; JSON serialization forbids `NaN` and `Infinity` (via `allow_nan=False`).
- **Information Disclosure**: Error messages truncate inputs to 200 characters and never expose database connection details.

### Not Defended

- **Persistence Integrity**: If the external SQLite file is modified by an unauthorized process outside of this library, the event stream integrity cannot be guaranteed.
- **Memory/Disk Availability**: The library assumes the host system has sufficient disk/memory to handle the database file or in-memory store; it does not implement OS-level rate limiting or quota enforcement.