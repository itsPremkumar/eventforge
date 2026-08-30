"""EventForge — event sourcing framework for agentic AI systems."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field


class Event(BaseModel):
    """A single event in the event store."""

    stream_id: str
    event_type: str
    data: dict[str, Any] = Field(default_factory=dict)
    position: int = 0
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_row(cls, row: tuple) -> "Event":
        """Create Event from SQLite row."""
        return cls(
            event_id=row[0],
            stream_id=row[1],
            position=row[2],
            event_type=row[3],
            data=json.loads(row[4]),
            timestamp=row[5],
        )


class EventStream(BaseModel):
    """A stream of events."""

    stream_id: str
    events: list[Event] = Field(default_factory=list)
    position: int = 0

    def append(self, event_type: str, data: dict[str, Any]) -> Event:
        event = Event(
            stream_id=self.stream_id,
            event_type=event_type,
            data=data,
            position=self.position,
        )
        self.events.append(event)
        self.position += 1
        return event


class EventStore:
    """Event store with optional SQLite persistence."""

    def __init__(self, db_path: str = ":memory:"):
        self._streams: dict[str, EventStream] = {}
        self._subscribers: list[Callable[[Event], None]] = []
        self._lock = threading.Lock()
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

        if db_path != ":memory:":
            self._init_sqlite(db_path)

    def _init_sqlite(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                data TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                UNIQUE (stream_id, position)
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_stream ON events (stream_id, position)")
        self._conn.commit()
        # Load existing streams from SQLite
        self._load_from_sqlite()

    def _load_from_sqlite(self) -> None:
        """Load existing events from SQLite into memory."""
        cursor = self._conn.execute(
            "SELECT event_id, stream_id, position, event_type, data, timestamp FROM events ORDER BY stream_id, position"
        )
        for row in cursor.fetchall():
            event = Event.from_row(row)
            if event.stream_id not in self._streams:
                self._streams[event.stream_id] = EventStream(stream_id=event.stream_id)
            self._streams[event.stream_id].events.append(event)
            self._streams[event.stream_id].position = max(
                self._streams[event.stream_id].position, event.position + 1
            )

    def append(self, stream_id: str, event_type: str, data: dict[str, Any]) -> Event:
        with self._lock:
            if stream_id not in self._streams:
                self._streams[stream_id] = EventStream(stream_id=stream_id)

            event = self._streams[stream_id].append(event_type, data)

            if self._conn:
                self._conn.execute(
                    "INSERT INTO events (event_id, stream_id, position, event_type, data, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    (event.event_id, stream_id, event.position, event.event_type, json.dumps(event.data), event.timestamp),
                )
                self._conn.commit()

            for subscriber in self._subscribers:
                subscriber(event)

            return event

    def read_stream(self, stream_id: str, from_position: int = 0) -> list[Event]:
        if stream_id not in self._streams:
            return []
        return [e for e in self._streams[stream_id].events if e.position >= from_position]

    def get_stream(self, stream_id: str) -> Optional[EventStream]:
        return self._streams.get(stream_id)

    def subscribe(self, callback: Callable[[Event], None]) -> Callable[[], None]:
        self._subscribers.append(callback)

        def unsubscribe():
            self._subscribers.remove(callback)
        return unsubscribe

    def all_streams(self) -> list[EventStream]:
        return list(self._streams.values())

    def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def replay(events: list[Event], handlers: dict[str, Callable[[dict[str, Any]], Any]]) -> Any:
    """Rebuild state from events using registered handlers."""
    state = {}
    for event in events:
        handler = handlers.get(event.event_type)
        if handler:
            state = handler(state, event.data) or state
    return state
