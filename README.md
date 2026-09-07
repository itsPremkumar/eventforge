# EventForge — Event Sourcing Framework

> Production-grade event sourcing for Python applications.

## Features

- **Event** — Immutable domain events with metadata
- **AggregateRoot** — Event-sourced aggregate base class
- **InMemoryEventStore** — Fast in-memory event storage with indexing
- **EventBus** — In-memory pub/sub event bus
- **SnapshotManager** — Automatic snapshot management
- **Projection** — Read-model projections
- **EventForge** — Main facade for event sourcing operations

## Quick Start

```python
from eventforge import EventForge, AggregateRoot

# Create the event forge
forge = EventForge()

# Define an aggregate
class BankAccount(AggregateRoot):
    def __init__(self, id: str) -> None:
        super().__init__(id)
        self.balance = 0

    def on_deposit(self, data: dict) -> None:
        self.balance += data["amount"]

    def on_withdraw(self, data: dict) -> None:
        self.balance -= data["amount"]

# Use the aggregate
account = BankAccount("acc-1")
account.raise_event("deposit", {"amount": 100})
account.raise_event("deposit", {"amount": 50})
account.raise_event("withdraw", {"amount": 30})

# Commit events
forge.commit(account)

# Rebuild from events
rebuilt = forge.rebuild("acc-1", BankAccount)
print(rebuilt.balance)  # 120
```

## License

MIT
