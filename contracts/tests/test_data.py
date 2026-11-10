"""Relationship, arrival-time, label, and fixture integrity checks."""
import copy
import csv
import hashlib
import json
import shutil

import pytest

from contracts.data import (ContractError, DEFAULT_POLICY, history_at,
                            label_impressions, timestamp, validate_dataset,
                            validate_object)
from contracts.fixtures.generate import generate


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    output = tmp_path_factory.mktemp("source") / "datasets"
    generate(output, impressions=1200)
    return output


@pytest.fixture
def dataset(generated, tmp_path):
    target = tmp_path / "project"
    shutil.copytree(generated / "project-1", target)
    return target


def rewrite(directory, name, transform):
    path = directory / name
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        rows = list(reader)
    transform(rows)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    entry = next(entry for entry in manifest["files"] if entry["name"] == name)
    entry.update(byte_length=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest(), row_count=len(rows))
    manifest_path.write_text(json.dumps(manifest))


def event(kind="impression", **changes):
    row = {"event_id": "evt_001", "event_type": kind, "user_id": "user_001", "item_id": "clip_001",
           "occurred_at": "2026-10-01T00:00:00Z", "received_at": "2026-10-01T00:00:01Z", "impression_id": "imp_001"}
    row.update(changes)
    return row


SPLITS = {"train_start": "2026-10-01T00:00:00Z", "validation_start": "2026-10-08T00:00:00Z",
          "test_start": "2026-10-10T00:00:00Z", "test_end": "2026-10-12T00:00:00Z"}


def labels(events, watermark="2026-10-15T00:00:00Z"):
    return label_impressions(events, DEFAULT_POLICY, SPLITS, watermark)


def test_default_fixture_has_real_catalog_and_sufficient_classes(tmp_path):
    report = generate(tmp_path / "default")
    for project in report["projects"]:
        assert project["counts"]["impressions"] == 10_000
        assert project["counts"]["items"] == project["playable_items"] == 8
        assert project["counts"]["users"] == 100
        assert project["counts"]["late_events"] == 0
        assert project["eligible"] is True
        assert project["replay_fidelity"] == "synthetic"
        assert all(split["positive"] >= 20 and split["negative"] >= 20 for split in project["splits"].values())


def test_seed_reproduces_all_bytes_and_existing_identical_output(generated, tmp_path):
    same = tmp_path / "same"
    generate(same, impressions=1200)
    before = {path.relative_to(generated): path.read_bytes() for path in generated.rglob("*") if path.is_file()}
    assert before == {path.relative_to(same): path.read_bytes() for path in same.rglob("*") if path.is_file()}
    generate(same, impressions=1200)
    assert before == {path.relative_to(same): path.read_bytes() for path in same.rglob("*") if path.is_file()}
    with pytest.raises(ContractError, match="Existing output differs"):
        generate(same, seed=7102, impressions=1200)
    assert before == {path.relative_to(same): path.read_bytes() for path in same.rglob("*") if path.is_file()}


def test_colliding_external_ids_are_separate_project_fixtures(generated):
    one = json.loads((generated / "project-1/manifest.json").read_text())
    two = json.loads((generated / "project-2/manifest.json").read_text())
    assert one["project_id"] != two["project_id"]
    assert one["dataset_id"] != two["dataset_id"]
    assert (generated / "project-1/items.csv").read_bytes() == (generated / "project-2/items.csv").read_bytes()
    assert (generated / "project-1/users.csv").read_bytes() == (generated / "project-2/users.csv").read_bytes()
    assert (generated / "project-1/events.csv").read_bytes() != (generated / "project-2/events.csv").read_bytes()


def test_unknown_fields_and_server_receipt_cannot_enter_online_body():
    online = event()
    online.pop("received_at")
    validate_object(online, "EventInput")
    for extra in ({"received_at": "2026-10-01T00:00:01Z"}, {"project_id": "another"}, {"label": 1}):
        with pytest.raises(ContractError):
            validate_object({**online, **extra}, "EventInput")


@pytest.mark.parametrize("value", ["", " user", "../item", "a/b", "a" * 129])
def test_invalid_ids_rejected(value):
    with pytest.raises(ContractError):
        validate_object(value, "ExternalId")


@pytest.mark.parametrize("definition,value", [
    ("ExternalId", "viewer_001\n"),
    ("ResourceId", "prj_" + "0" * 32 + "\n"),
    ("Timestamp", "2026-10-01T00:00:00Z\n"),
])
def test_full_value_patterns_reject_terminal_newline(definition, value):
    with pytest.raises(ContractError):
        validate_object(value, definition)


@pytest.mark.parametrize("field", ["project_id", "dataset_id", "digest", "media_path"])
def test_nested_manifest_patterns_reject_terminal_newline(dataset, field):
    if field == "media_path":
        catalog = json.loads((dataset / "media.json").read_text())
        catalog["items"][0]["video_path"] += "\n"
        with pytest.raises(ContractError):
            validate_object(catalog, "MediaCatalog")
        return
    manifest = json.loads((dataset / "manifest.json").read_text())
    if field == "digest":
        manifest["files"][0]["sha256"] += "\n"
    else:
        manifest[field] += "\n"
    with pytest.raises(ContractError):
        validate_object(manifest, "DatasetManifest")


def test_identifiers_preserve_leading_zeros_and_case():
    for value in ["000001", "User_001", "user_001", "commons:1234"]:
        validate_object(value, "ExternalId")
    assert "User_001" != "user_001"


@pytest.mark.parametrize("value", ["2026-10-01", "2026-10-01T00:00:00", "2026-02-30T00:00:00Z", "2026-10-01T00:00:60Z"])
def test_invalid_or_naive_timestamp(value):
    with pytest.raises(ContractError):
        timestamp(value)


def test_timestamp_offsets_represent_same_instant():
    assert timestamp("2026-10-01T00:00:00Z") == timestamp("2026-09-30T17:00:00-07:00")


@pytest.mark.parametrize("value", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"])
def test_utc_normalization_overflow_is_a_structured_contract_error(value):
    with pytest.raises(ContractError) as caught:
        timestamp(value)
    assert caught.value.code == "invalid_timestamp"


def test_label_policy_arithmetic_overflow_is_a_structured_contract_error():
    impression = event(occurred_at="9999-12-31T23:59:59Z", received_at="9999-12-31T23:59:59Z")
    with pytest.raises(ContractError) as caught:
        labels([impression], watermark="9999-12-31T23:59:59Z")
    assert caught.value.code == "invalid_timestamp"


def test_future_click_is_not_a_prior_impression_feature():
    impression = event()
    click = event("click", event_id="evt_click", occurred_at="2026-10-01T00:00:05Z", received_at="2026-10-01T00:00:20Z")
    assert history_at([impression, click], "2026-10-01T00:00:10Z", "2026-10-01T00:00:10Z") == [impression]
    assert history_at([impression, click], "2026-10-01T00:00:21Z", "2026-10-01T00:00:20Z") == [impression, click]
    assert history_at([impression], impression["occurred_at"], impression["occurred_at"]) == []
    with pytest.raises(ContractError, match="Snapshot"):
        history_at([impression], "2026-10-01T00:00:10Z", "2026-10-01T00:00:11Z")


def test_maturity_and_click_attribution_boundaries():
    impression = event()
    boundary_click = event("click", event_id="evt_click", occurred_at="2026-10-01T00:30:00Z", received_at="2026-10-02T00:30:00Z")
    assert labels([impression, boundary_click], "2026-10-02T00:29:59Z") == []
    assert labels([impression, boundary_click], "2026-10-02T00:30:00Z")[0]["label"] == 1
    assert labels([impression, {**boundary_click, "occurred_at": "2026-10-01T00:30:01Z"}])[0]["label"] == 0
    assert labels([impression, {**boundary_click, "occurred_at": impression["occurred_at"], "received_at": impression["received_at"]}])[0]["label"] == 0


def test_late_click_retained_but_not_used_in_labels_or_history():
    impression = event()
    click = event("click", event_id="evt_click", occurred_at="2026-10-01T00:00:05Z", received_at="2026-10-02T00:00:06Z")
    events = [impression, click]
    assert labels(events)[0]["label"] == 0
    assert history_at(events, "2026-10-03T00:00:00Z", "2026-10-03T00:00:00Z") == [impression]
    assert len(events) == 2


def test_label_window_touching_next_split_is_purged_and_no_unexposed_negatives():
    touching = event(occurred_at="2026-10-07T23:30:00Z", received_at="2026-10-07T23:30:01Z")
    before = event(event_id="evt_before", impression_id="imp_before", occurred_at="2026-10-07T23:29:59Z", received_at="2026-10-07T23:30:00Z")
    assert [row["impression_id"] for row in labels([touching, before])] == ["imp_before"]
    assert labels([]) == []


def test_digest_mismatch_detects_unrecorded_changes(dataset):
    with (dataset / "events.csv").open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_size"


def test_foreign_item_relationship_rejected_even_with_correct_checksum(dataset):
    rewrite(dataset, "events.csv", lambda rows: rows[0].update(item_id="foreign_item"))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "unknown_reference"


@pytest.mark.parametrize("change,code", [({"impression_id": "unknown"}, "invalid_impression_reference"),
                                        ({"user_id": "user_099"}, "invalid_impression_reference"),
                                        ({"occurred_at": "2026-10-01T00:00:00Z"}, "invalid_event_order")])
def test_interactions_must_match_exposure(dataset, change, code):
    def alter(rows):
        click = next(row for row in rows if row["event_type"] == "click")
        if "user_id" in change and click["user_id"] == "user_099":
            click["user_id"] = "user_098"
        else:
            click.update(change)
    rewrite(dataset, "events.csv", alter)
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == code


def test_watch_duration_must_fit_media(dataset):
    def alter(rows):
        next(row for row in rows if row["event_type"] == "watch")["watch_ms"] = "100000"
    rewrite(dataset, "events.csv", alter)
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_watch"


def test_deduplicate_only_identical_events(dataset):
    before = validate_dataset(dataset)
    rewrite(dataset, "events.csv", lambda rows: rows.append(copy.deepcopy(rows[0])))
    after = validate_dataset(dataset)
    assert after["counts"]["duplicates"] == before["counts"]["duplicates"] + 1
    assert after["counts"]["events"] == before["counts"]["events"]
    rewrite(dataset, "events.csv", lambda rows: rows[-1].update(item_id="coffee-ritual" if rows[-1]["item_id"] != "coffee-ritual" else "coast-sunset"))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "conflicting_duplicate"


def test_path_traversal_inventory_rejected_before_read(dataset):
    path = dataset / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["files"][0]["name"] = "../items.csv"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_schema"


def test_symlink_inventory_and_unlisted_files_rejected(dataset, tmp_path):
    path = dataset / "items.csv"
    original = tmp_path / "outside.csv"
    path.rename(original)
    path.symlink_to(original)
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_file"


def test_csv_boolean_and_unknown_headers_are_explicit(dataset):
    rewrite(dataset, "items.csv", lambda rows: rows[0].update(active="1"))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_boolean"


def test_recorded_arrival_cannot_mix_missing_receipts(dataset):
    rewrite(dataset, "events.csv", lambda rows: rows[0].update(received_at=""))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "missing_receipt"


def test_receipt_clock_skew_bound(dataset):
    rewrite(dataset, "events.csv", lambda rows: rows[0].update(received_at="2026-09-30T23:54:59Z"))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "future_event"


def test_late_events_remain_in_inventory_but_do_not_enter_training(dataset):
    before = validate_dataset(dataset)
    rewrite(dataset, "events.csv", lambda rows: rows[0].update(received_at="2026-10-02T00:00:01Z"))
    after = validate_dataset(dataset)
    assert after["counts"]["events"] == before["counts"]["events"]
    assert after["counts"]["late_events"] == 1
    assert after["counts"]["mature_training_rows"] == before["counts"]["mature_training_rows"] - 1


def test_missing_historical_receipts_have_explicit_replay_limitation(dataset):
    rewrite(dataset, "events.csv", lambda rows: [row.update(received_at="") for row in rows])
    path = dataset / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["availability_mode"] = "event_time_only"
    manifest["provenance"] = {"kind": "imported", "note": "Historical receipt times unavailable."}
    path.write_text(json.dumps(manifest))
    report = validate_dataset(dataset)
    assert report["replay_fidelity"] == "not_established"
    assert report["counts"]["mature_training_rows"] > 0


def test_fixture_item_metadata_cannot_disagree_with_playback_manifest(dataset):
    rewrite(dataset, "items.csv", lambda rows: rows[0].update(duration_ms="100000"))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "media_mismatch"


def test_auxiliary_media_metadata_has_independent_byte_limit(dataset):
    manifest = json.loads((dataset / "manifest.json").read_text())
    manifest["auxiliary_files"][0]["byte_length"] = 256 * 1024 + 1
    with pytest.raises(ContractError):
        validate_object(manifest, "DatasetManifest")
    (dataset / "media.json").write_bytes(b" " * (256 * 1024 + 1))
    with pytest.raises(ContractError) as caught:
        validate_dataset(dataset)
    assert caught.value.code == "invalid_size"
    assert caught.value.details["file"] == "media.json"


def test_csv_budget_does_not_charge_auxiliary_metadata(dataset, monkeypatch):
    manifest = json.loads((dataset / "manifest.json").read_text())
    csv_bytes = sum(entry["byte_length"] for entry in manifest["files"])
    monkeypatch.setattr("contracts.data.MAX_DATASET_BYTES", csv_bytes)
    report = validate_dataset(dataset)
    assert report["bytes"] > csv_bytes
