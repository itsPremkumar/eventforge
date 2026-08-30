"""Tests for EventForge event sourcing framework."""

import os
import tempfile

import pytest

from eventforge import EventStore, Event, replay


class TestEventStore:
    """Test event store operations."""

    def test_append_event(self):
        store = EventStore()
        event = store.append("stream_1", "user.created", {"name": "Alice"})
        assert event.stream_id == "stream_1"
        assert event.event_type == "user.created"
        assert event.position == 0

    def test_append_multiple_events(self):
        store = EventStore()
        e1 = store.append("s1", "a", {})
        e2 = store.append("s1", "b", {})
        e3 = store.append("s2", "c", {})
        assert e1.position == 0
        assert e2.position == 1
        assert e3.position == 0

    def test_read_stream(self):
        store = EventStore()
        store.append("s1", "a", {"x": 1})
        store.append("s1", "b", {"x": 2})
        events = store.read_stream("s1")
        assert len(events) == 2

    def test_read_stream_from_position(self):
        store = EventStore()
        for i in range(5):
            store.append("s1", "event", {"i": i})
        events = store.read_stream("s1", from_position=3)
        assert len(events) == 2

    def test_get_stream_not_found(self):
        store = EventStore()
        assert store.get_stream("nonexistent") is None

    def test_subscribe(self):
        store = EventStore()
        received = []
        unsub = store.subscribe(lambda e: received.append(e))
        store.append("s1", "test", {})
        assert len(received) == 1
        unsub()
        store.append("s1", "test2", {})
        assert len(received) == 1

    def test_all_streams(self):
        store = EventStore()
        store.append("s1", "a", {})
        store.append("s2", "b", {})
        streams = store.all_streams()
        assert len(streams) == 2


class TestSQLitePersistence:
    """Test SQLite-backed event store."""

    def test_sqlite_store_and_read(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            with EventStore(db_path=db_path) as store:
                store.append("s1", "created", {"x": 1})
                store.append("s1", "updated", {"x": 2})
                events = store.read_stream("s1")
                assert len(events) == 2
                assert events[0].event_type == "created"
        finally:
            os.unlink(db_path)

    def test_sqlite_persists_across_instances(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            with EventStore(db_path=db_path) as store1:
                store1.append("s1", "event1", {"a": 1})
            with EventStore(db_path=db_path) as store2:
                events = store2.read_stream("s1")
                assert len(events) == 1
        finally:
            os.unlink(db_path)


class TestReplay:
    """Test event replay."""

    def test_replay_simple(self):
        events = [
            Event(stream_id="s1", event_type="add", data={"value": 10}, position=0),
            Event(stream_id="s1", event_type="add", data={"value": 5}, position=1),
        ]
        handlers = {"add": lambda state, data: {"total": state.get("total", 0) + data["value"]}}
        state = replay(events, handlers)
        assert state == {"total": 15}

    def test_replay_unknown_event_type(self):
        events = [Event(stream_id="s1", event_type="unknown", data={}, position=0)]
        state = replay(events, {})
        assert state == {}
