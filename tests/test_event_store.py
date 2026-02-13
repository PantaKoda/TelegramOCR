import os
import uuid
import unittest
from dataclasses import asdict
from datetime import date, datetime, timezone

import psycopg
from psycopg.rows import dict_row

from domain.schedule_diff import ShiftAdded, ShiftTimeChanged
from infra.event_store import (
    EVENT_TYPE_SHIFT_ADDED,
    EVENT_TYPE_SHIFT_TIME_CHANGED,
    load_day_snapshot,
    persist_events_and_snapshot,
    process_observation,
)
from parser.entity_identity import customer_fingerprint, location_fingerprint
from parser.semantic_normalizer import CanonicalShift

DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


def _shift(
    *,
    start: str = "10:00",
    end: str = "14:00",
    customer_name: str = "Marie Sjoberg",
    street: str = "Valebergsvagen",
    street_number: str = "316",
    city: str = "Billdal",
    shift_type: str = "HOME_VISIT",
) -> CanonicalShift:
    return CanonicalShift(
        start=start,
        end=end,
        customer_name=customer_name,
        customer_fingerprint=customer_fingerprint(customer_name),
        street=street,
        street_number=street_number,
        postal_code="",
        postal_area="",
        city=city,
        location_fingerprint=location_fingerprint(
            street=street,
            street_number=street_number,
            postal_area="",
            city=city,
        ),
        shift_type=shift_type,
    )


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class EventStoreIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_event_store_{uuid.uuid4().hex[:12]}"
        self.user_id = 8225717176
        self.schedule_date = date(2026, 8, 22)
        self._create_schema()

    def tearDown(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")

    def _create_schema(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {self.schema}")
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.day_snapshot (
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        snapshot_payload JSONB NOT NULL CHECK (jsonb_typeof(snapshot_payload) = 'array'),
                        source_session_id UUID NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (user_id, schedule_date)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.schedule_event (
                        event_id UUID PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        event_type TEXT NOT NULL,
                        location_fingerprint TEXT NOT NULL,
                        customer_fingerprint TEXT NOT NULL,
                        old_value_hash TEXT NOT NULL,
                        new_value_hash TEXT NOT NULL,
                        old_value JSONB NULL,
                        new_value JSONB NULL,
                        detected_at TIMESTAMPTZ NOT NULL,
                        source_session_id UUID NOT NULL
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE UNIQUE INDEX {self.schema}_schedule_event_dedupe
                    ON {self.schema}.schedule_event (
                        user_id,
                        schedule_date,
                        location_fingerprint,
                        event_type,
                        old_value_hash,
                        new_value_hash
                    )
                    """
                )

    def _events(self) -> list[dict]:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT event_type, old_value, new_value, source_session_id::text AS source_session_id, detected_at
                    FROM {self.schema}.schedule_event
                    WHERE user_id = %s
                      AND schedule_date = %s
                    ORDER BY detected_at ASC, event_id ASC
                    """,
                    (self.user_id, self.schedule_date),
                )
                return list(cur.fetchall())

    def _snapshot_row(self) -> dict | None:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT snapshot_payload, source_session_id::text AS source_session_id
                    FROM {self.schema}.day_snapshot
                    WHERE user_id = %s
                      AND schedule_date = %s
                    """,
                    (self.user_id, self.schedule_date),
                )
                return cur.fetchone()

    def test_first_observation_creates_added_event_and_snapshot(self) -> None:
        current = [_shift()]
        session_id = str(uuid.uuid4())

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                events = process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=session_id,
                    current_snapshot=current,
                    detected_at=datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftAdded)

        rows = self._events()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], EVENT_TYPE_SHIFT_ADDED)
        self.assertIsNone(rows[0]["old_value"])
        self.assertEqual(rows[0]["new_value"]["start"], "10:00")

        snapshot = self._snapshot_row()
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["source_session_id"], session_id)
        self.assertEqual(len(snapshot["snapshot_payload"]), 1)

    def test_repeated_same_observation_adds_no_new_event(self) -> None:
        current = [_shift()]
        first_session = str(uuid.uuid4())
        second_session = str(uuid.uuid4())

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=first_session,
                    current_snapshot=current,
                )
            with conn.transaction():
                events_second = process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=second_session,
                    current_snapshot=current,
                )

        self.assertEqual(events_second, [])

        rows = self._events()
        self.assertEqual(len(rows), 1)
        snapshot = self._snapshot_row()
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["source_session_id"], second_session)

    def test_time_change_persists_shift_time_changed_event(self) -> None:
        first = [_shift(start="10:00", end="14:00")]
        second = [_shift(start="11:00", end="15:00")]

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=str(uuid.uuid4()),
                    current_snapshot=first,
                    detected_at=datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc),
                )
            with conn.transaction():
                events = process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=str(uuid.uuid4()),
                    current_snapshot=second,
                    detected_at=datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftTimeChanged)

        rows = self._events()
        self.assertEqual([row["event_type"] for row in rows], [EVENT_TYPE_SHIFT_ADDED, EVENT_TYPE_SHIFT_TIME_CHANGED])
        self.assertEqual(rows[1]["old_value"]["start"], "10:00")
        self.assertEqual(rows[1]["new_value"]["start"], "11:00")

        with psycopg.connect(DB_URL) as conn:
            loaded = load_day_snapshot(conn, self.schema, user_id=self.user_id, schedule_date=self.schedule_date)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].start, "11:00")
        self.assertEqual(loaded[0].end, "15:00")

    def test_persist_events_is_idempotent_for_same_semantic_event(self) -> None:
        session_id = str(uuid.uuid4())
        detected_at = datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc)
        current = _shift()
        events = [ShiftAdded(schedule_date=self.schedule_date.isoformat(), shift=current)]

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                inserted_first = persist_events_and_snapshot(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=session_id,
                    events=events,
                    snapshot=[current],
                    detected_at=detected_at,
                )
            with conn.transaction():
                inserted_second = persist_events_and_snapshot(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=session_id,
                    events=events,
                    snapshot=[current],
                    detected_at=detected_at,
                )

        self.assertEqual(inserted_first, 1)
        self.assertEqual(inserted_second, 0)
        rows = self._events()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], EVENT_TYPE_SHIFT_ADDED)

    def test_replay_events_reconstructs_snapshot(self) -> None:
        start_state = [_shift(start="10:00", end="14:00", customer_name="Marie Sjoberg")]
        end_state = [_shift(start="11:00", end="15:00", customer_name="Marie Sjoberg")]
        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=str(uuid.uuid4()),
                    current_snapshot=start_state,
                    detected_at=datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc),
                )
            with conn.transaction():
                process_observation(
                    conn,
                    self.schema,
                    user_id=self.user_id,
                    schedule_date=self.schedule_date,
                    source_session_id=str(uuid.uuid4()),
                    current_snapshot=end_state,
                    detected_at=datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc),
                )

            stored_snapshot = load_day_snapshot(conn, self.schema, user_id=self.user_id, schedule_date=self.schedule_date)

        replayed = _replay_events(self._events())
        self.assertEqual(_shift_list_key(replayed), _shift_list_key(stored_snapshot))


def _replay_events(rows: list[dict]) -> list[CanonicalShift]:
    state: list[CanonicalShift] = []
    for row in rows:
        event_type = row["event_type"]
        old_value = row["old_value"]
        new_value = row["new_value"]

        if event_type == EVENT_TYPE_SHIFT_ADDED:
            if new_value is not None:
                state.append(_shift_from_dict(new_value))
            continue

        if event_type == "shift_removed":
            if old_value is not None:
                state = [shift for shift in state if _event_shift_key(shift) != _event_shift_key(_shift_from_dict(old_value))]
            continue

        if event_type in {"shift_time_changed", "shift_relocated", "shift_retitled", "shift_reclassified"}:
            if old_value is not None:
                old_key = _event_shift_key(_shift_from_dict(old_value))
                state = [shift for shift in state if _event_shift_key(shift) != old_key]
            if new_value is not None:
                state.append(_shift_from_dict(new_value))
            continue

        raise RuntimeError(f"Unexpected event_type in replay: {event_type}")

    return sorted(state, key=_event_shift_key)


def _shift_from_dict(value: dict) -> CanonicalShift:
    return CanonicalShift(
        start=value["start"],
        end=value["end"],
        customer_name=value["customer_name"],
        customer_fingerprint=value["customer_fingerprint"],
        street=value["street"],
        street_number=value["street_number"],
        postal_code=value["postal_code"],
        postal_area=value["postal_area"],
        city=value["city"],
        location_fingerprint=value["location_fingerprint"],
        shift_type=value["shift_type"],
    )


def _event_shift_key(shift: CanonicalShift) -> tuple:
    return (
        shift.location_fingerprint,
        shift.customer_fingerprint,
        shift.start,
        shift.end,
        shift.shift_type,
        shift.street,
        shift.street_number,
        shift.city,
        shift.customer_name,
    )


def _shift_list_key(shifts: list[CanonicalShift]) -> list[dict]:
    rows = [asdict(shift) for shift in shifts]
    rows.sort(
        key=lambda value: (
            value["location_fingerprint"],
            value["customer_fingerprint"],
            value["start"],
            value["end"],
            value["customer_name"],
        )
    )
    return rows


if __name__ == "__main__":
    unittest.main()
