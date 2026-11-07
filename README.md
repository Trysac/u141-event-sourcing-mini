# event-sourcing-mini

[![PyPI version](https://img.shields.io/pypi/v/trysac-event-sourcing-mini)](https://pypi.org/project/trysac-event-sourcing-mini/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-event-sourcing-mini)](LICENSE)
[![CI](https://github.com/Trysac/u141-event-sourcing-mini/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u141-event-sourcing-mini/actions/workflows/ci.yml)

Minimal event sourcing: append-only streams with optimistic concurrency, global positions, projections with checkpoints and snapshots, in memory or SQLite

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-event-sourcing-mini
# or
uv add trysac-event-sourcing-mini
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import event_sourcing_mini

# normalize() trims surrounding whitespace and lower-cases the text.
print(event_sourcing_mini.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    event_sourcing_mini.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
