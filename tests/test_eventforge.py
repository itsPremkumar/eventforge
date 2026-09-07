"""Tests for EventForge."""
from __future__ import annotations

import pytest

from eventforge import (
    Event,
    EventForge,
    AggregateRoot,
    InMemoryEventStore,
    EventBus,
    Projection,
    Snapshot,
    SnapshotManager,
)


class TestEvent:
    def test_create(self):
        event = Event.create("agg-1", "created", {"name": "test"})
        assert event.aggregate_id == "agg-1"
        assert event.event_type == "created"
        assert event.data == {"name": "test"}
        assert event.version == 1
        assert event.event_id is not None

    def test_to_dict(self):
        event = Event.create("agg-1", "test", {"a": 1})
        d = event.to_dict()
        assert d["aggregate_id"] == "agg-1"
        assert d["event_type"] == "test"

    def test_from_dict(self):
        event = Event.create("agg-1", "test", {"a": 1})
        d = event.to_dict()
        restored = Event.from_dict(d)
        assert restored.event_id == event.event_id
        assert restored.data == event.data


class TestInMemoryEventStore:
    def test_append_and_get(self):
        store = InMemoryEventStore()
        event = Event.create("agg-1", "test", {})
        store.append(event)
        events = store.get_events("agg-1")
        assert len(events) == 1
        assert events[0].event_id == event.event_id

    def test_get_all(self):
        store = InMemoryEventStore()
        store.append(Event.create("agg-1", "test", {}))
        store.append(Event.create("agg-2", "test", {}))
        assert len(store.get_all()) == 2

    def test_count(self):
        store = InMemoryEventStore()
        store.append(Event.create("agg-1", "test", {}))
        assert store.count() == 1


class TestAggregateRoot:
    def test_raise_event(self):
        agg = AggregateRoot("agg-1")
        event = agg.raise_event("created", {"name": "test"})
        assert event.version == 1
        assert event.event_type == "created"
        assert agg.version == 1

    def test_collect_changes(self):
        agg = AggregateRoot("agg-1")
        agg.raise_event("created", {"name": "test"})
        agg.raise_event("updated", {"name": "updated"})
        changes = agg.collect_changes()
        assert len(changes) == 2
        assert agg.version == 2

    def test_apply_event(self):
        class Counter(AggregateRoot):
            def __init__(self, id: str) -> None:
                super().__init__(id)
                self.count = 0

            def on_increment(self, data: dict) -> None:
                self.count += data.get("amount", 1)

        counter = Counter("c-1")
        event = Event.create("c-1", "increment", {"amount": 5}, version=1)
        counter.apply_event(event)
        assert counter.count == 5


class TestEventBus:
    def test_subscribe_and_publish(self):
        bus = EventBus()
        received = []
        bus.subscribe("test", lambda e: received.append(e))
        event = Event.create("agg-1", "test", {"a": 1})
        bus.publish(event)
        assert len(received) == 1
        assert received[0].event_id == event.event_id

    def test_publish_all(self):
        bus = EventBus()
        received = []
        bus.subscribe("test", lambda e: received.append(e))
        events = [Event.create("agg-1", "test", {}), Event.create("agg-2", "test", {})]
        bus.publish_all(events)
        assert len(received) == 2


class TestSnapshot:
    def test_create(self):
        snap = Snapshot("agg-1", 5, {"count": 10})
        assert snap.aggregate_id == "agg-1"
        assert snap.version == 5
        assert snap.state == {"count": 10}


class TestSnapshotManager:
    def test_save_and_load(self):
        mgr = SnapshotManager(every=5)
        agg = AggregateRoot("agg-1")
        for _ in range(5):
            agg.raise_event("test", {})
        mgr.save(agg, {"version": 5})
        snap = mgr.load("agg-1")
        assert snap is not None
        assert snap.version == 5

    def test_no_save_before_threshold(self):
        mgr = SnapshotManager(every=10)
        agg = AggregateRoot("agg-1")
        agg.raise_event("test", {})
        mgr.save(agg, {"version": 1})
        assert mgr.load("agg-1") is None


class TestProjection:
    def test_when_and_handle(self):
        proj = Projection("counter")
        counts = []
        proj.when("increment", lambda e: counts.append(e.data.get("amount", 1)))
        event = Event.create("agg-1", "increment", {"amount": 3})
        proj.handle(event)
        assert counts == [3]


class TestEventForge:
    def test_commit(self):
        forge = EventForge()
        agg = AggregateRoot("agg-1")
        agg.raise_event("created", {"name": "test"})
        changes = forge.commit(agg)
        assert len(changes) == 1
        assert forge.store.count() == 1

    def test_rebuild(self):
        forge = EventForge()
        agg = AggregateRoot("agg-1")
        agg.raise_event("created", {"name": "test"})
        agg.raise_event("updated", {"name": "updated"})
        forge.commit(agg)

        def factory(id: str) -> AggregateRoot:
            return AggregateRoot(id)

        rebuilt = forge.rebuild("agg-1", factory)
        assert rebuilt.version == 2

    def test_register_projection(self):
        forge = EventForge()
        proj = Projection("test-proj")
        received = []
        proj.when("created", lambda e: received.append(e))
        forge.register_projection(proj)

        agg = AggregateRoot("agg-1")
        agg.raise_event("created", {"name": "test"})
        forge.commit(agg)
        assert len(received) == 1
