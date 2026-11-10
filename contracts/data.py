"""Executable recommendation-v1 contracts; no application or cloud adapters.

This reference checker freezes label/availability semantics for later Spark and
online implementations. It deliberately does not construct model features.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = Path(__file__).parent / "schemas" / "data.schema.json"
MAX_DATASET_BYTES = 50 * 1024 * 1024
MAX_METADATA_BYTES = 256 * 1024
MAX_ROWS = 100_000
DEFAULT_POLICY = {"version": "click-v1", "attribution_seconds": 1800, "lateness_seconds": 86400}
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")
INTEGER_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)$")


class ContractError(ValueError):
    """A safe machine-readable contract violation, without source row contents."""

    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def as_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


def timestamp(value):
    """Strict offset-aware RFC3339 subset, normalized to UTC (microseconds max)."""
    if not isinstance(value, str) or not DATE_PATTERN.fullmatch(value):
        raise ContractError("invalid_timestamp", "Expected an offset-aware RFC3339 timestamp.")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise ContractError("invalid_timestamp", "Timestamp cannot represent a real UTC calendar instant.") from exc


def _add_seconds(value, seconds):
    try:
        return value + timedelta(seconds=seconds)
    except OverflowError as exc:
        raise ContractError("invalid_timestamp", "Timestamp cannot represent the required policy window.") from exc


def isoformat(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@lru_cache(maxsize=None)
def validator(definition):
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator({**schema, "$ref": f"#/$defs/{definition}"}, format_checker=FormatChecker())


def validate_object(value, definition):
    errors = sorted(validator(definition).iter_errors(value), key=lambda error: str(list(error.path)))
    if errors:
        error = errors[0]
        raise ContractError("invalid_schema", "Value does not satisfy the declared schema.",
                            schema=definition, field=".".join(map(str, error.path)), rule=error.validator)


def is_late(event, policy=None):
    policy = policy or DEFAULT_POLICY
    return ("received_at" in event and timestamp(event["received_at"]) -
            timestamp(event["occurred_at"]) > timedelta(seconds=policy["lateness_seconds"]))


def history_at(events, request_time, snapshot_watermark, availability_mode="recorded_arrival", policy=None):
    """Prior events actually available at a pinned immutable snapshot cutoff.

    A click is its own event: an earlier impression never exposes its future
    outcome. Equal event timestamps are ordered by stable event_id, but events
    at the prediction instant are excluded regardless of their event_id.
    """
    request = timestamp(request_time)
    cutoff = timestamp(snapshot_watermark)
    if cutoff > request:
        raise ContractError("future_snapshot", "Snapshot cutoff must not follow request time.")
    if availability_mode not in {"recorded_arrival", "event_time_only"}:
        raise ContractError("invalid_availability", "Unknown availability mode.")
    selected = []
    for event in events:
        occurred = timestamp(event["occurred_at"])
        if occurred >= request or is_late(event, policy):
            continue
        if availability_mode == "recorded_arrival":
            if "received_at" not in event:
                raise ContractError("missing_receipt", "Arrival replay requires received_at on every event.")
            if timestamp(event["received_at"]) > cutoff:
                continue
        elif occurred > cutoff:
            continue
        selected.append(event)
    return sorted(selected, key=lambda event: (timestamp(event["occurred_at"]), event["event_id"]))


def label_impressions(events, policy, split_cutoffs, extraction_watermark, availability_mode="recorded_arrival"):
    """Return only mature, on-time, non-crossing observed impressions.

    Attribution is (impression, impression + window], receipt lateness is <=
    the allowance, maturity is <= the extraction watermark. The next split
    owns its boundary, so an attribution window touching it is purged.
    """
    validate_object(policy, "LabelPolicy")
    validate_object(split_cutoffs, "SplitCutoffs")
    if availability_mode not in {"recorded_arrival", "event_time_only"}:
        raise ContractError("invalid_availability", "Unknown availability mode.")
    watermark = timestamp(extraction_watermark)
    starts = [timestamp(split_cutoffs[name]) for name in ("train_start", "validation_start", "test_start", "test_end")]
    if starts != sorted(set(starts)):
        raise ContractError("invalid_splits", "Split cutoffs must be strictly increasing.")
    clicks = {}
    impressions = []
    for event in events:
        if is_late(event, policy):
            continue
        if availability_mode == "recorded_arrival":
            if "received_at" not in event:
                raise ContractError("missing_receipt", "Arrival replay requires received_at on every event.")
            if timestamp(event["received_at"]) > watermark:
                continue
        if event["event_type"] == "impression":
            impressions.append(event)
        elif event["event_type"] == "click":
            clicks.setdefault(event["impression_id"], []).append(event)
    rows = []
    for impression in impressions:
        at = timestamp(impression["occurred_at"])
        end = _add_seconds(at, policy["attribution_seconds"])
        if _add_seconds(end, policy["lateness_seconds"]) > watermark:
            continue
        split = next((name for index, name in enumerate(("train", "validation", "test"))
                      if starts[index] <= at < starts[index + 1] and end < starts[index + 1]), None)
        if split is None:
            continue
        label = any(at < timestamp(click["occurred_at"]) <= end
                    for click in clicks.get(impression["impression_id"], [])
                    if click["user_id"] == impression["user_id"] and click["item_id"] == impression["item_id"])
        rows.append({"impression_id": impression["impression_id"], "user_id": impression["user_id"],
                     "item_id": impression["item_id"], "occurred_at": impression["occurred_at"],
                     "split": split, "label": int(label)})
    return sorted(rows, key=lambda row: (timestamp(row["occurred_at"]), row["impression_id"]))


def _load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise ContractError("invalid_json", "Expected a valid UTF-8 JSON object.", file=path.name) from exc


def _read_csv(path, definition):
    props = validator(definition).schema["$defs"][definition]["properties"]
    required = set(validator(definition).schema["$defs"][definition]["required"])
    rows = []
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            headers = reader.fieldnames
            if (not headers or len(headers) != len(set(headers)) or not required.issubset(headers)
                    or set(headers) - set(props)):
                raise ContractError("invalid_headers", "CSV headers are missing, repeated, or unsupported.", file=path.name)
            for number, raw in enumerate(reader, 2):
                if number > MAX_ROWS + 1:
                    raise ContractError("too_many_rows", "File exceeds the row limit.", file=path.name)
                if None in raw or any(value is None for value in raw.values()):
                    raise ContractError("invalid_row", "CSV row width does not match its headers.", file=path.name, row=number)
                row = {key: value for key, value in raw.items() if value != "" or key in required}
                for name in ("duration_ms", "watch_ms"):
                    if name in row:
                        if len(row[name]) > 8 or not INTEGER_PATTERN.fullmatch(row[name]):
                            raise ContractError("invalid_integer", "Expected a nonnegative decimal integer.", file=path.name, row=number, field=name)
                        row[name] = int(row[name])
                if "active" in row:
                    if row["active"] not in {"true", "false"}:
                        raise ContractError("invalid_boolean", "CSV booleans must be true or false.", file=path.name, row=number)
                    row["active"] = row["active"] == "true"
                try:
                    validate_object(row, definition)
                    for name in ("occurred_at", "received_at", "published_at", "created_at"):
                        if name in row:
                            timestamp(row[name])
                except ContractError as exc:
                    raise ContractError(exc.code, exc.message, **{**exc.details, "file": path.name, "row": number}) from exc
                rows.append(row)
    except (UnicodeError, csv.Error) as exc:
        raise ContractError("invalid_csv", "Expected well-formed UTF-8 CSV.", file=path.name) from exc
    return rows


def _deduplicate(rows, field, file):
    result = {}
    duplicates = 0
    for row in rows:
        key = row[field]
        if key in result:
            if result[key] != row:
                raise ContractError("conflicting_duplicate", "An ID is reused with a different payload.", file=file, field=field)
            duplicates += 1
        result[key] = row
    return result, duplicates


def validate_media(catalog, *, check_assets=False):
    validate_object(catalog, "MediaCatalog")
    items = catalog["items"]
    if len({item["item_id"] for item in items}) != len(items):
        raise ContractError("duplicate_media", "Media item IDs must be unique.")
    if check_assets:
        for item in items:
            for path_field, digest_field in (("video_path", "video_sha256"), ("poster_path", "poster_sha256")):
                asset = ROOT / item[path_field]
                if asset.is_symlink() or not asset.is_file() or hashlib.sha256(asset.read_bytes()).hexdigest() != item[digest_field]:
                    raise ContractError("invalid_media", "Bundled media is absent or does not match its digest.", field=path_field)
    return {item["item_id"]: item for item in items}


def validate_dataset(directory):
    """Check immutable fixture bytes, rows, relationships, and supervised support.

    This validates a dataset *within* its declared project. It does not grant
    project authorization, implement uploads, or assert a model beats a baseline.
    """
    directory = Path(directory).resolve()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink() or manifest_path.stat().st_size > MAX_METADATA_BYTES:
        raise ContractError("invalid_manifest", "Expected a bounded regular manifest.json file.")
    manifest = _load_json(manifest_path)
    validate_object(manifest, "DatasetManifest")
    files = manifest["files"] + manifest.get("auxiliary_files", [])
    names = [entry["name"] for entry in files]
    if len(set(names)) != len(names) or not {"items.csv", "events.csv"}.issubset(names):
        raise ContractError("invalid_inventory", "Inventory must uniquely include items.csv and events.csv.")
    if {path.name for path in directory.iterdir()} != set(names) | {"manifest.json"}:
        raise ContractError("invalid_inventory", "Dataset directory does not match its manifest inventory.")
    total, csv_total = 0, 0
    for entry in files:
        path = directory / entry["name"]
        if not path.is_file() or path.is_symlink() or path.resolve().parent != directory:
            raise ContractError("invalid_file", "Inventory entries must be regular files inside the dataset.")
        size = path.stat().st_size
        total += size
        if entry["name"].endswith(".csv"):
            csv_total += size
        if (size != entry["byte_length"] or csv_total > MAX_DATASET_BYTES
                or (entry["name"] == "media.json" and size > MAX_METADATA_BYTES)):
            raise ContractError("invalid_size", "File size mismatch or dataset byte limit exceeded.", file=entry["name"])
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ContractError("digest_mismatch", "File digest does not match the inventory.", file=entry["name"])
    raw_items = _read_csv(directory / "items.csv", "Item")
    raw_events = _read_csv(directory / "events.csv", "Event")
    raw_users = _read_csv(directory / "users.csv", "User") if "users.csv" in names else []
    items, item_duplicates = _deduplicate(raw_items, "item_id", "items.csv")
    events, event_duplicates = _deduplicate(raw_events, "event_id", "events.csv")
    users, user_duplicates = _deduplicate(raw_users, "user_id", "users.csv")
    raw_counts = {"items.csv": len(raw_items), "events.csv": len(raw_events), "users.csv": len(raw_users)}
    media = None
    if "media.json" in names:
        media = validate_media(_load_json(directory / "media.json"), check_assets=True)
        raw_counts["media.json"] = len(media)
        if set(items) != set(media):
            raise ContractError("media_mismatch", "Every fixture item must match exactly one playable media entry.")
        if any(item.get("duration_ms") != media[key]["duration_ms"] or item.get("topic") != media[key]["topic"]
               for key, item in items.items()):
            raise ContractError("media_mismatch", "Fixture item metadata must match its playable catalog manifest.")
    for entry in files:
        if raw_counts[entry["name"]] != entry["row_count"]:
            raise ContractError("row_count_mismatch", "Manifest row count does not match the file.", file=entry["name"])
    if not items or not events:
        raise ContractError("empty_dataset", "At least one item and one event are required.")
    impressions, late = {}, 0
    mode, policy = manifest["availability_mode"], manifest["label_policy"]
    for event in events.values():
        if event["item_id"] not in items or ("users.csv" in names and event["user_id"] not in users):
            raise ContractError("unknown_reference", "Event references an unknown item or user.")
        occurred = timestamp(event["occurred_at"])
        if occurred < timestamp(items[event["item_id"]]["published_at"]):
            raise ContractError("before_publication", "An event cannot precede item publication.")
        if "users.csv" in names and "created_at" in users[event["user_id"]] and occurred < timestamp(users[event["user_id"]]["created_at"]):
            raise ContractError("before_creation", "An event cannot precede user creation.")
        if mode == "recorded_arrival" and "received_at" not in event:
            raise ContractError("missing_receipt", "Arrival replay requires received_at on every event.")
        if mode == "event_time_only" and "received_at" in event:
            raise ContractError("mixed_availability", "Event-time-only datasets must omit all receipt timestamps.")
        if "received_at" in event and occurred - timestamp(event["received_at"]) > timedelta(minutes=5):
            raise ContractError("future_event", "Event exceeds the five-minute receipt clock-skew allowance.")
        late += int(is_late(event, policy))
        if event["event_type"] == "impression":
            if event["impression_id"] in impressions:
                raise ContractError("duplicate_impression", "An impression identity must have exactly one impression event.")
            impressions[event["impression_id"]] = event
    for event in events.values():
        if event["event_type"] == "impression":
            continue
        impression = impressions.get(event["impression_id"])
        if not impression or any(event[name] != impression[name] for name in ("user_id", "item_id")):
            raise ContractError("invalid_impression_reference", "Interaction must reference an impression of the same user and item.")
        if timestamp(event["occurred_at"]) <= timestamp(impression["occurred_at"]):
            raise ContractError("invalid_event_order", "Interactions must occur strictly after their impression.")
        if event["event_type"] == "watch" and event["watch_ms"] > items[event["item_id"]].get("duration_ms", 86_400_000):
            raise ContractError("invalid_watch", "Watch duration exceeds the item's declared duration.")
    observed = [timestamp(event["occurred_at"]) for event in events.values()]
    interval = manifest["timestamp_range"]
    if timestamp(interval["start"]) != min(observed) or timestamp(interval["end"]) != max(observed):
        raise ContractError("invalid_range", "Manifest timestamp range must match the event inventory.")
    if timestamp(manifest["extraction_watermark"]) < max(observed):
        raise ContractError("future_inventory", "Event inventory must not extend beyond extraction watermark.")
    labels = label_impressions(list(events.values()), policy, manifest["split_cutoffs"], manifest["extraction_watermark"], mode)
    splits = {name: {"positive": 0, "negative": 0, "total": 0} for name in ("train", "validation", "test")}
    for row in labels:
        count = splits[row["split"]]
        count["positive" if row["label"] else "negative"] += 1
        count["total"] += 1
    eligible = len(labels) >= 1000 and all(count["positive"] >= 20 and count["negative"] >= 20 for count in splits.values())
    return {"schema_version": manifest["schema_version"], "project_id": manifest["project_id"],
            "dataset_id": manifest["dataset_id"], "provenance": manifest["provenance"],
            "availability_mode": mode, "replay_fidelity": "synthetic" if manifest["provenance"]["kind"] == "synthetic" else ("recorded_arrival" if mode == "recorded_arrival" else "not_established"),
            "counts": {"items": len(items), "users": len(users), "events": len(events), "impressions": len(impressions),
                       "mature_training_rows": len(labels), "late_events": late, "duplicates": item_duplicates + event_duplicates + user_duplicates},
            "splits": splits, "eligible": eligible, "bytes": total, "playable_items": len(media) if media else 0}
