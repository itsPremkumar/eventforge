"""EventForge — Event Sourcing Framework."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Protocol
from collections import defaultdict


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    """A domain event."""
    event_id: str
    aggregate_id: str
    event_type: str
    data: dict[str, Any]
    timestamp: str
    version: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        aggregate_id: str,
        event_type: str,
        data: dict[str, Any],
        version: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> Event:
        return cls(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            event_type=event_type,
            data=data,
            timestamp=_now(),
            version=version,
            metadata=metadata or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Event:
        return cls(**d)


class EventStore(Protocol):
    """Protocol for event stores."""

    def append(self, event: Event) -> None: ...
    def get_events(self, aggregate_id: str) -> list[Event]: ...
    def get_all(self) -> list[Event]: ...


class InMemoryEventStore:
    """In-memory event store."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._index: dict[str, list[Event]] = defaultdict(list)

    def append(self, event: Event) -> None:
        self._events.append(event)
        self._index[event.aggregate_id].append(event)

    def get_events(self, aggregate_id: str) -> list[Event]:
        return list(self._index.get(aggregate_id, []))

    def get_all(self) -> list[Event]:
        return list(self._events)

    def count(self) -> int:
        return len(self._events)


class AggregateRoot:
    """Base class for event-sourced aggregates."""

    def __init__(self, aggregate_id: str) -> None:
        self._id = aggregate_id
        self._version = 0
        self._changes: list[Event] = []

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> int:
        return self._version

    def apply_event(self, event: Event) -> None:
        """Apply an event to mutate state."""
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is not None:
            handler(event.data)
        self._version = event.version

    def raise_event(self, event_type: str, data: dict[str, Any], metadata: dict[str, Any] | None = None) -> Event:
        self._version += 1
        event = Event.create(
            aggregate_id=self._id,
            event_type=event_type,
            data=data,
            version=self._version,
            metadata=metadata,
        )
        self.apply_event(event)
        self._changes.append(event)
        return event

    def collect_changes(self) -> list[Event]:
        changes = list(self._changes)
        self._changes.clear()
        return changes


class EventBus:
    """Simple in-memory event bus."""

    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        for handler in self._handlers.get(event.event_type, []):
            handler(event)

    def publish_all(self, events: list[Event]) -> None:
        for event in events:
            self.publish(event)


class Snapshot:
    """A point-in-time snapshot of aggregate state."""

    def __init__(self, aggregate_id: str, version: int, state: dict[str, Any], timestamp: str | None = None) -> None:
        self.aggregate_id = aggregate_id
        self.version = version
        self.state = state
        self.timestamp = timestamp or _now()


class SnapshotManager:
    """Manages aggregate snapshots."""

    def __init__(self, every: int = 10) -> None:
        self._snapshots: dict[str, Snapshot] = {}
        self._every = every

    def save(self, aggregate: AggregateRoot, state: dict[str, Any]) -> None:
        if aggregate.version % self._every == 0:
            self._snapshots[aggregate.id] = Snapshot(
                aggregate_id=aggregate.id,
                version=aggregate.version,
                state=state,
            )

    def load(self, aggregate_id: str) -> Snapshot | None:
        return self._snapshots.get(aggregate_id)


class Projection:
    """A read-model projection."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._handlers: dict[str, Callable[[Event], None]] = {}
        self._state: dict[str, Any] = {}

    def when(self, event_type: str, handler: Callable[[Event], None]) -> Projection:
        self._handlers[event_type] = handler
        return self

    def handle(self, event: Event) -> None:
        handler = self._handlers.get(event.event_type)
        if handler is not None:
            handler(event)

    @property
    def state(self) -> dict[str, Any]:
        return dict(self._state)


class EventForge:
    """Main event sourcing facade."""

    def __init__(self, store: EventStore | None = None, snapshot_every: int = 10) -> None:
        self.store = store or InMemoryEventStore()
        self.bus = EventBus()
        self.snapshots = SnapshotManager(every=snapshot_every)
        self._projections: dict[str, Projection] = {}

    def register_projection(self, projection: Projection) -> None:
        self._projections[projection.name] = projection

    def commit(self, aggregate: AggregateRoot) -> list[Event]:
        changes = aggregate.collect_changes()
        for event in changes:
            self.store.append(event)
            self.bus.publish(event)
            for projection in self._projections.values():
                projection.handle(event)
        return changes

    def rebuild(self, aggregate_id: str, factory: Callable[[str], AggregateRoot]) -> AggregateRoot:
        aggregate = factory(aggregate_id)
        for event in self.store.get_events(aggregate_id):
            aggregate.apply_event(event)
        return aggregate
