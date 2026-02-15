"""Run OCR backfill from local images using DB session metadata."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from domain.notification_rules import build_notifications
from domain.schedule_diff import diff_schedules
from domain.session_aggregate import aggregate_session_shifts
from infra.event_store import load_day_snapshot, process_observation
from infra.notification_store import persist_notifications
from ocr.paddle_adapter import create_paddle_ocr, run_paddle_on_image
from parser.layout_parser import parse_layout
from parser.semantic_normalizer import CanonicalShift, normalize_entries
from worker.run_forever import _extract_schedule_date_from_boxes, _resolve_session_schedule_dates

LOG = logging.getLogger("local-backfill-runner")


@dataclass(frozen=True)
class SessionRow:
    session_id: str
    user_id: int
    state: str
    created_at: datetime
    closed_at: datetime | None


@dataclass(frozen=True)
class ImageRow:
    sequence: int
    r2_key: str
    created_at: datetime


@dataclass
class ImageIndex:
    root: Path
    by_relative: dict[str, Path]
    by_basename: dict[str, list[Path]]


class BackfillStageError(RuntimeError):
    def __init__(self, stage: str, message: str) -> None:
        super().__init__(message)
        self.stage = stage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OCR backfill from local images using DB session metadata.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"), help="PostgreSQL URL. Defaults to DATABASE_URL env var.")
    parser.add_argument("--db-schema", default=os.getenv("DB_SCHEMA", "schedule_ingest"))
    parser.add_argument("--local-root", required=True, help="Local root directory that contains downloaded screenshots.")
    parser.add_argument("--output-file", default="backfill_report.json", help="Path to summary JSON report.")
    parser.add_argument("--output-dir", default="backfill_sessions", help="Directory for per-session JSON output.")
    parser.add_argument("--user-id", type=int, help="Filter sessions for one user.")
    parser.add_argument("--session-id", action="append", dest="session_ids", help="Specific session id (repeatable).")
    parser.add_argument("--states", default="done,failed", help="Comma-separated states to select from capture_session.")
    parser.add_argument("--from-date", help="Filter sessions created_at >= YYYY-MM-DD.")
    parser.add_argument("--to-date", help="Filter sessions created_at < YYYY-MM-DD + 1 day.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of sessions (0 = no limit).")
    parser.add_argument("--ocr-lang", default=os.getenv("OCR_LANG", "sv"), help="Paddle OCR language (default: sv).")
    parser.add_argument(
        "--ocr-default-year",
        type=int,
        default=datetime.now(UTC).year,
        help="Year fallback when OCR date header has no year (default: current UTC year).",
    )
    parser.add_argument("--summary-threshold", type=int, default=3)
    parser.add_argument("--apply", action="store_true", help="Persist events/snapshots to DB.")
    parser.add_argument("--store-notifications", action="store_true", help="Store generated notifications (requires --apply).")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise RuntimeError("Missing database URL. Pass --database-url or set DATABASE_URL.")
    if args.store_notifications and not args.apply:
        raise RuntimeError("--store-notifications requires --apply.")
    if args.limit < 0:
        raise RuntimeError("--limit must be >= 0.")
    if not str(args.ocr_lang).strip():
        raise RuntimeError("--ocr-lang must be non-empty.")
    return args


def parse_states_csv(value: str) -> tuple[str, ...]:
    tokens = [item.strip() for item in value.split(",")]
    states = tuple(item for item in tokens if item)
    if not states:
        raise RuntimeError("states must include at least one value.")
    return states


def parse_optional_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def build_image_index(root: Path) -> ImageIndex:
    if not root.exists() or not root.is_dir():
        raise RuntimeError(f"Local root does not exist or is not a directory: {root}")
    by_relative: dict[str, Path] = {}
    by_basename: dict[str, list[Path]] = defaultdict(list)
    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue
        relative = file_path.relative_to(root).as_posix()
        by_relative[relative] = file_path
        by_basename[file_path.name].append(file_path)
    return ImageIndex(root=root, by_relative=by_relative, by_basename=dict(by_basename))


def resolve_local_image_path(r2_key: str, index: ImageIndex) -> Path:
    normalized_key = r2_key.lstrip("/")
    direct = index.by_relative.get(normalized_key)
    if direct is not None:
        return direct

    basename = Path(normalized_key).name
    candidates = index.by_basename.get(basename, [])
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        rendered = ", ".join(str(path) for path in candidates)
        raise BackfillStageError("input", f"Ambiguous local files for {basename}: {rendered}")
    raise BackfillStageError("input", f"Missing local image for key: {r2_key}")


def fetch_sessions(
    conn: psycopg.Connection,
    *,
    schema: str,
    user_id: int | None,
    states: tuple[str, ...],
    session_ids: tuple[str, ...],
    from_date: date | None,
    to_date: date | None,
    limit: int,
) -> list[SessionRow]:
    conditions = [sql.SQL("cs.state::text = ANY(%s)")]
    params: list[Any] = [list(states)]

    if user_id is not None:
        conditions.append(sql.SQL("cs.user_id = %s"))
        params.append(user_id)
    if session_ids:
        conditions.append(sql.SQL("cs.id::text = ANY(%s)"))
        params.append(list(session_ids))
    if from_date is not None:
        conditions.append(sql.SQL("cs.created_at >= %s"))
        params.append(datetime.combine(from_date, datetime.min.time(), tzinfo=UTC))
    if to_date is not None:
        to_exclusive = datetime.combine(to_date, datetime.min.time(), tzinfo=UTC).replace(tzinfo=UTC)
        to_exclusive = to_exclusive + timedelta(days=1)
        conditions.append(sql.SQL("cs.created_at < %s"))
        params.append(to_exclusive)

    where_sql = sql.SQL(" AND ").join(conditions)
    limit_sql = sql.SQL(" LIMIT %s") if limit > 0 else sql.SQL("")
    if limit > 0:
        params.append(limit)

    query = sql.SQL(
        """
        SELECT
            cs.id::text AS session_id,
            cs.user_id AS user_id,
            cs.state::text AS state,
            cs.created_at AS created_at,
            cs.closed_at AS closed_at
        FROM {}.capture_session cs
        WHERE {}
        ORDER BY cs.created_at ASC, cs.id ASC
        {}
        """
    ).format(sql.Identifier(schema), where_sql, limit_sql)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        rows = list(cur.fetchall())
    return [
        SessionRow(
            session_id=str(row["session_id"]),
            user_id=int(row["user_id"]),
            state=str(row["state"]),
            created_at=row["created_at"],
            closed_at=row["closed_at"],
        )
        for row in rows
    ]


def fetch_session_images(conn: psycopg.Connection, *, schema: str, session_id: str) -> list[ImageRow]:
    query = sql.SQL(
        """
        SELECT
            sequence,
            r2_key,
            created_at
        FROM {}.capture_image
        WHERE session_id = %s
        ORDER BY sequence ASC
        """
    ).format(sql.Identifier(schema))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (session_id,))
        rows = list(cur.fetchall())
    return [
        ImageRow(
            sequence=int(row["sequence"]),
            r2_key=str(row["r2_key"]),
            created_at=row["created_at"],
        )
        for row in rows
    ]


def load_session_events_for_stamp(
    conn: psycopg.Connection,
    *,
    schema: str,
    source_session_id: str,
    detected_at: datetime,
) -> list[dict[str, Any]]:
    query = sql.SQL(
        """
        SELECT
            event_id::text AS event_id,
            user_id,
            schedule_date,
            event_type,
            location_fingerprint,
            customer_fingerprint,
            old_value,
            new_value,
            source_session_id::text AS source_session_id,
            detected_at
        FROM {}.schedule_event
        WHERE source_session_id = %s
          AND detected_at = %s
        ORDER BY detected_at ASC, event_id ASC
        """
    ).format(sql.Identifier(schema))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (source_session_id, detected_at))
        return list(cur.fetchall())


def canonical_shift_sort_key(shift: CanonicalShift) -> tuple[str, str, str, str, str, str]:
    return (
        shift.location_fingerprint,
        shift.customer_fingerprint,
        shift.start,
        shift.end,
        shift.city,
        shift.customer_name,
    )


def domain_event_type_name(value: Any) -> str:
    return type(value).__name__


def process_one_session(
    conn: psycopg.Connection,
    *,
    schema: str,
    session: SessionRow,
    images: list[ImageRow],
    index: ImageIndex,
    ocr_client: Any,
    ocr_default_year: int | None,
    apply: bool,
    store_notifications: bool,
    summary_threshold: int,
) -> dict[str, Any]:
    if not images:
        raise BackfillStageError("input", f"Session {session.session_id} has no images.")

    image_dates: list[date | None] = []
    image_shifts: list[list[CanonicalShift]] = []
    image_details: list[dict[str, Any]] = []
    date_extraction_errors: list[tuple[str, Exception]] = []
    total_boxes = 0

    for image in images:
        local_path = resolve_local_image_path(image.r2_key, index)
        boxes = run_paddle_on_image(local_path, ocr=ocr_client)
        total_boxes += len(boxes)
        extracted_date: date | None = None
        try:
            extracted_date = _extract_schedule_date_from_boxes(boxes, default_year=ocr_default_year)
        except Exception as error:  # noqa: BLE001
            date_extraction_errors.append((image.r2_key, error))

        try:
            entries = parse_layout(boxes)
            canonical = normalize_entries(entries)
            canonical.sort(key=canonical_shift_sort_key)
        except Exception as error:  # noqa: BLE001
            raise BackfillStageError("layout", f"Failed parsing {image.r2_key}: {error}") from error

        image_dates.append(extracted_date)
        image_shifts.append(canonical)
        image_details.append(
            {
                "sequence": image.sequence,
                "r2_key": image.r2_key,
                "local_path": str(local_path),
                "detected_date": extracted_date.isoformat() if extracted_date is not None else None,
                "box_count": len(boxes),
                "entry_count": len(canonical),
            }
        )

    try:
        schedule_date, resolved_dates, inherited_image_count = _resolve_session_schedule_dates(image_dates)
    except Exception as error:  # noqa: BLE001
        if image_dates and all(item is None for item in image_dates) and date_extraction_errors:
            key, first = date_extraction_errors[0]
            raise BackfillStageError("date", f"No session date from OCR; first failure on {key}: {first}") from first
        raise BackfillStageError("date", f"Failed resolving session date: {error}") from error

    aggregated = aggregate_session_shifts(image_shifts, schedule_date=schedule_date.isoformat())
    canonical_shifts = [item.shift for item in aggregated.shifts]
    canonical_shifts.sort(key=canonical_shift_sort_key)

    old_snapshot = load_day_snapshot(conn, schema, user_id=session.user_id, schedule_date=schedule_date)
    preview_events = diff_schedules(old_snapshot, canonical_shifts, schedule_date=schedule_date.isoformat())

    persisted_events = preview_events
    stored_notifications = 0
    notification_preview: list[dict[str, Any]] = []
    detected_at: datetime | None = None

    if apply:
        detected_at = datetime.now(UTC)
        persisted_events = process_observation(
            conn,
            schema,
            user_id=session.user_id,
            schedule_date=schedule_date,
            source_session_id=session.session_id,
            current_snapshot=canonical_shifts,
            detected_at=detected_at,
        )

        if store_notifications:
            event_rows = load_session_events_for_stamp(
                conn,
                schema=schema,
                source_session_id=session.session_id,
                detected_at=detected_at,
            )
            notifications = build_notifications(event_rows, summary_threshold=summary_threshold)
            stored_notifications = persist_notifications(conn, schema, notifications=notifications)
            notification_preview = [
                {
                    "notification_id": item.notification_id,
                    "notification_type": item.notification_type,
                    "message": item.message,
                    "event_ids": list(item.event_ids),
                }
                for item in notifications
            ]

    return {
        "session_id": session.session_id,
        "user_id": session.user_id,
        "source_state": session.state,
        "schedule_date": schedule_date.isoformat(),
        "resolved_image_dates": [value.isoformat() for value in resolved_dates],
        "inherited_image_count": inherited_image_count,
        "image_count": len(images),
        "image_names": [Path(item.r2_key).name for item in images],
        "image_details": image_details,
        "total_boxes": total_boxes,
        "shift_count": len(canonical_shifts),
        "shifts": [asdict(item) for item in canonical_shifts],
        "preview_event_count": len(preview_events),
        "preview_event_types": sorted({domain_event_type_name(item) for item in preview_events}),
        "applied": apply,
        "persisted_event_count": len(persisted_events),
        "persisted_event_types": sorted({domain_event_type_name(item) for item in persisted_events}),
        "stored_notifications": stored_notifications,
        "notifications": notification_preview,
        "detected_at": detected_at.isoformat() if detected_at is not None else None,
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    states = parse_states_csv(args.states)
    from_date = parse_optional_date(args.from_date)
    to_date = parse_optional_date(args.to_date)
    session_ids = tuple(args.session_ids or ())

    local_root = Path(args.local_root).expanduser().resolve()
    output_file = Path(args.output_file).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    image_index = build_image_index(local_root)

    LOG.info("Indexed %s local files under %s", len(image_index.by_relative), image_index.root)

    ocr_client = create_paddle_ocr(lang=args.ocr_lang)
    started_at = datetime.now(UTC)
    summary: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "database_schema": args.db_schema,
        "local_root": str(local_root),
        "output_dir": str(output_dir),
        "apply": args.apply,
        "store_notifications": args.store_notifications,
        "ocr_lang": args.ocr_lang,
        "ocr_default_year": args.ocr_default_year,
        "selection": {
            "user_id": args.user_id,
            "states": list(states),
            "session_ids": list(session_ids),
            "from_date": args.from_date,
            "to_date": args.to_date,
            "limit": args.limit,
        },
        "sessions_total": 0,
        "sessions_ok": 0,
        "sessions_failed": 0,
        "results": [],
    }

    with psycopg.connect(args.database_url) as conn:
        sessions = fetch_sessions(
            conn,
            schema=args.db_schema,
            user_id=args.user_id,
            states=states,
            session_ids=session_ids,
            from_date=from_date,
            to_date=to_date,
            limit=args.limit,
        )
        summary["sessions_total"] = len(sessions)
        LOG.info("Selected %s sessions", len(sessions))

        for index, session in enumerate(sessions, start=1):
            LOG.info("Processing session %s/%s: %s", index, len(sessions), session.session_id)
            images = fetch_session_images(conn, schema=args.db_schema, session_id=session.session_id)
            try:
                with conn.transaction():
                    result = process_one_session(
                        conn,
                        schema=args.db_schema,
                        session=session,
                        images=images,
                        index=image_index,
                        ocr_client=ocr_client,
                        ocr_default_year=args.ocr_default_year,
                        apply=args.apply,
                        store_notifications=args.store_notifications,
                        summary_threshold=args.summary_threshold,
                    )
                result["status"] = "ok"
                summary["sessions_ok"] += 1
            except Exception as error:  # noqa: BLE001
                stage = getattr(error, "stage", "runtime")
                result = {
                    "session_id": session.session_id,
                    "user_id": session.user_id,
                    "source_state": session.state,
                    "image_count": len(images),
                    "image_names": [Path(item.r2_key).name for item in images],
                    "status": "failed",
                    "error_stage": stage,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
                summary["sessions_failed"] += 1

            summary["results"].append(result)
            write_json(output_dir / f"{session.session_id}.json", result)

    summary["finished_at"] = datetime.now(UTC).isoformat()
    write_json(output_file, summary)
    LOG.info("Done. ok=%s failed=%s summary=%s", summary["sessions_ok"], summary["sessions_failed"], output_file)


if __name__ == "__main__":
    main()
