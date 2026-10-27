"""Read-only inference provenance, source boundaries and worker protocol."""

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from serving import preview
from training.models import DeepFM
from training.train_deepfm import TrainConfig


@pytest.fixture
def artifacts(tmp_path):
    gold = tmp_path / "gold"
    (gold / "train").mkdir(parents=True)
    (gold / "_metadata").mkdir()
    spec = {
        "label": "is_click",
        "id_columns": ["user_id", "video_id", "time_ms"],
        "categorical": [{"name": "category", "vocab_size": 8}],
        "numeric": ["value"], "flags": ["missing"],
        "sequence": {"name": "history", "vocab_size": 10, "pad": 0, "length": 4},
        "splits": {"train": {"rows": 7}, "val": {"rows": 1}, "test": {"rows": 1}},
    }
    # Unsorted users/times and a repeated item: context at the earliest recorded
    # exposure must be retained rather than synthesizing a new user-item pair.
    values = [(1002, 5, 15, .5), (1001, 9, 30, .3), (1001, 4, 10, .1),
              (1002, 6, 25, -.5), (1001, 8, 20, .2), (1002, 7, 35, .7),
              (1001, 4, 5, -.8)]
    rows = [{"user_id": user, "video_id": video, "time_ms": time,
             "category": video % 8, "value": np.float32(value),
             "missing": video % 2 == 0, "history": [0, 0, video % 9 + 1, 0],
             "label": 999} for user, video, time, value in values]
    # Poisoned targets are deliberately irrelevant to inference and never read.
    pq.write_table(pa.Table.from_pylist(rows), gold / "train/part-00000.parquet")
    (gold / "_metadata/feature_spec.json").write_text(json.dumps(spec))
    for split in ("val", "test", "test_random"):
        (gold / split).mkdir()
        (gold / split / "do-not-read.parquet").write_bytes(b"NEVER READ HELD-OUT DATA")

    config = TrainConfig(epochs=3, embedding_dim=4, hidden_dims=(8,), dropout=.5)
    torch.manual_seed(42)
    torch.set_num_threads(2)
    model = DeepFM(spec, embedding_dim=4, hidden_dims=(8,), dropout=.5)
    batch = {
        "categorical": torch.tensor([[row["category"]] for row in rows]),
        "numeric": torch.tensor([[row["value"], row["missing"]] for row in rows], dtype=torch.float32),
        "sequence": torch.tensor([row["history"] for row in rows]),
    }
    # A real optimizer step provides nontrivial fitted weights, then no training
    # function or optimizer is used by the preview itself.
    optimizer = torch.optim.Adam(model.parameters(), lr=.01)
    optimizer.zero_grad()
    torch.nn.functional.binary_cross_entropy_with_logits(model(batch), torch.tensor([0., 1., 0., 1., 0., 1., 0.])).backward()
    optimizer.step()
    model.eval()
    checkpoint_dir = tmp_path / "selected"
    checkpoint_dir.mkdir()
    checkpoint = checkpoint_dir / "best.pt"
    val = {"auc": .6, "rows": 1}
    torch.save({"model_state_dict": model.state_dict(), "model_config": asdict(config),
                "feature_spec": spec, "epoch": 1, "val_metrics": val}, checkpoint)
    selected = {"run_dir": str(checkpoint_dir), "config": asdict(config),
                "best_epoch": 1, "val": val,
                "checkpoint_sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                "feature_spec_sha256": hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()}
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"models": {"deepfm": selected}}))
    return {"gold": gold, "selection": selection, "checkpoint": checkpoint,
            "model": model, "rows": rows, "spec": spec}


def make_preview(artifacts):
    return preview.DeepFMPreview(artifacts["gold"], artifacts["selection"],
                                 profile_count=2, examples_per_profile=2)


def test_scores_match_independent_checkpoint_forward_on_original_contexts(artifacts):
    engine = make_preview(artifacts)
    status = engine.status()
    assert status["state"] == "ready" and status["source_split"] == "train"
    assert status["media_mapping"] is False and status["scope"] == "historical_training_examples"
    assert status["checkpoint_sha256"] == hashlib.sha256(artifacts["checkpoint"].read_bytes()).hexdigest()
    assert engine.profiles() == [{"id": "1001", "name": "Profile 1", "example_count": 2},
                                 {"id": "1002", "name": "Profile 2", "example_count": 2}]
    # Earliest duplicate video 4 context (-.8 at timestamp5), then video8 at20.
    rows = [artifacts["rows"][6], artifacts["rows"][4]]
    batch = {"categorical": torch.tensor([[row["category"]] for row in rows]),
             "numeric": torch.tensor([[row["value"], row["missing"]] for row in rows], dtype=torch.float32),
             "sequence": torch.tensor([row["history"] for row in rows])}
    with torch.inference_mode():
        logits = artifacts["model"](batch)
    expected = {str(row["video_id"]): (row["time_ms"], logit.item()) for row, logit in zip(rows, logits)}
    result = engine.score("1001")
    assert result["profile_id"] == "1001" and result["source_split"] == "train"
    assert "in-sample" in result["explanation"] and "stock clips" in result["explanation"]
    assert len(result["examples"]) == 2
    assert [row["rank"] for row in result["examples"]] == [1, 2]
    assert [row["probability"] for row in result["examples"]] == sorted([row["probability"] for row in result["examples"]], reverse=True)
    for row in result["examples"]:
        time, logit = expected[row["video_id"]]
        assert row["recorded_at_ms"] == time
        assert row["logit"] == logit
        assert row["probability"] == torch.tensor(logit, dtype=torch.float64).sigmoid().item()
        assert set(row) == {"video_id", "recorded_at_ms", "probability", "logit", "rank"}


def test_data_projection_excludes_targets_and_heldouts(artifacts, monkeypatch):
    original_read = pq.read_table
    seen = []

    def read(path, *args, **kwargs):
        assert Path(path).parent.name == "train"
        columns = kwargs["columns"]
        assert "label" not in columns and "is_click" not in columns
        seen.append(Path(path))
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_table", read)
    engine = make_preview(artifacts)
    assert engine.status()["state"] == "ready"
    engine.score("1001")
    assert seen == [artifacts["gold"] / "train/part-00000.parquet"]


def test_repeated_inference_is_exact_and_cannot_update_weights(artifacts):
    engine = make_preview(artifacts)
    before_hash = hashlib.sha256(artifacts["checkpoint"].read_bytes()).hexdigest()
    first = engine.score("1002")
    weights = {key: value.clone() for key, value in engine._model.state_dict().items()}
    for _ in range(3):
        assert engine.score("1002") == first
    assert engine._model.training is False
    assert all(not parameter.requires_grad for parameter in engine._model.parameters())
    assert all(torch.equal(value, engine._model.state_dict()[key]) for key, value in weights.items())
    assert hashlib.sha256(artifacts["checkpoint"].read_bytes()).hexdigest() == before_hash


def test_profile_collection_can_continue_into_the_next_train_shard(artifacts):
    path = artifacts["gold"] / "train/part-00000.parquet"
    table = pq.read_table(path)
    pq.write_table(table.slice(0, 3), path)
    pq.write_table(table.slice(3), artifacts["gold"] / "train/part-00001.parquet")
    engine = make_preview(artifacts)
    assert engine.status()["state"] == "ready"
    assert [row["id"] for row in engine.profiles()] == ["1001", "1002"]


def test_training_preview_refuses_unbounded_materialization(artifacts, monkeypatch):
    monkeypatch.setattr(preview, "MAX_PREVIEW_ROWS", 6)
    monkeypatch.setattr(pq, "read_table", lambda *a, **k: pytest.fail("Over-budget shard must not be read"))
    assert make_preview(artifacts).status()["state"] == "unavailable"


@pytest.mark.parametrize("field,value", [("value", float("nan")), ("history", [0])])
def test_invalid_historical_tensors_are_unavailable_before_ready(artifacts, field, value):
    path = artifacts["gold"] / "train/part-00000.parquet"
    rows = pq.read_table(path).to_pylist()
    # Earliest video4 occurrence is part of the selected user1001 profile.
    rows[6][field] = value
    pq.write_table(pa.Table.from_pylist(rows), path)
    assert make_preview(artifacts).status()["state"] == "unavailable"


@pytest.mark.parametrize("corruption", ["hash", "spec", "epoch", "config", "val"])
def test_bad_provenance_returns_safe_unavailable_and_is_not_retried(artifacts, monkeypatch, corruption):
    selection = json.loads(artifacts["selection"].read_text())
    selected = selection["models"]["deepfm"]
    if corruption == "hash": selected["checkpoint_sha256"] = "0" * 64
    elif corruption == "spec": selected["feature_spec_sha256"] = "0" * 64
    elif corruption == "epoch": selected["best_epoch"] = 2
    elif corruption == "config": selected["config"]["dropout"] = .25
    else: selected["val"]["auc"] = .9
    artifacts["selection"].write_text(json.dumps(selection))
    engine = make_preview(artifacts)
    status = engine.status()
    assert status["state"] == "unavailable"
    assert str(artifacts["gold"]) not in status["message"]
    monkeypatch.setattr(engine, "_load", lambda: pytest.fail("Failure must be cached"))
    assert engine.status() == status
    with pytest.raises(preview.PreviewError, match="could not be loaded"):
        engine.profiles()


@pytest.mark.parametrize("bad_id", ["9999", "../test", 1001, None, "1001 OR 1=1"])
def test_only_declared_profile_ids_can_be_scored(artifacts, bad_id):
    engine = make_preview(artifacts)
    with pytest.raises(preview.PreviewError) as caught:
        engine.score(bad_id)
    assert caught.value.code == "unknown_profile"


def test_tied_scores_have_stable_video_id_order(artifacts):
    engine = make_preview(artifacts)
    assert engine.status()["state"] == "ready"
    with torch.no_grad():
        for tensor in engine._model.parameters(): tensor.zero_()
    result = engine.score("1001")
    assert [row["video_id"] for row in result["examples"]] == ["4", "8"]
    assert [row["probability"] for row in result["examples"]] == [.5, .5]


def test_missing_artifacts_remain_a_valid_unavailable_worker_status(tmp_path):
    command = [sys.executable, str(preview.ROOT / "serving/preview.py"),
               "--gold-dir", str(tmp_path / "missing-gold"),
               "--selection-manifest", str(tmp_path / "missing-selection.json")]
    messages = [{"id": "s", "method": "status"}, {"id": "p", "method": "profiles"},
                {"id": "x", "method": "score", "profile_id": "0"},
                {"id": "bad", "method": "not-a-method"}]
    process = subprocess.run(command, input="not-json\n" + "\n".join(json.dumps(value) for value in messages) + "\n", text=True, capture_output=True, check=True)
    output = [json.loads(line) for line in process.stdout.splitlines()]
    assert len(output) == 5
    assert output[0]["error"]["code"] == "invalid_request"
    assert output[1]["id"] == "s" and output[1]["result"]["state"] == "unavailable"
    assert output[2]["error"]["code"] == output[3]["error"]["code"] == "model_unavailable"
    assert output[4]["id"] == "bad" and output[4]["error"]["code"] == "invalid_request"
    assert "initialization failed" in process.stderr
    assert str(tmp_path) not in process.stdout


def test_malformed_request_id_cannot_break_json_responses(artifacts):
    engine = make_preview(artifacts)
    for request in (None, [], {"id": float("nan"), "method": "status"},
                    {"id": {}, "method": "score"}, {"id": True, "method": "status"}):
        result = preview.handle_request(engine, request)
        assert result["id"] is None and result["error"]["code"] == "invalid_request"
        json.dumps(result, allow_nan=False)
