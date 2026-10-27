"""Preview the frozen DeepFM checkpoint on historical training examples.

This is inference inspection, not a live recommendation service or evaluation.
Only projected train columns are opened. Targets and held-out splits are never
read. Each example keeps its own recorded user/item/context features; examples
are not presented as candidates from one shared request context.

Run as a persistent JSON-lines worker with the existing training venv. stdout
contains protocol responses only; initialization diagnostics go to stderr.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch

ROOT = Path(__file__).resolve().parents[1]
if __package__ in (None, ""):
    sys.path.insert(0, str(ROOT))

from training.dataset import load_spec
from training.models import DeepFM


SCOPE = "historical_training_examples"
EXPLANATION = (
    "These are historical training examples, scored with their original recorded "
    "contexts. They are in-sample illustrations, not a new evaluation or live "
    "feed recommendations. The stock clips in the scrolling feed are not mapped "
    "to these KuaiRand items. No model training or weight updates occur."
)
UNAVAILABLE = (
    "The saved DeepFM model or training examples could not be loaded. "
    "Check the local checkpoint, gold export and selection manifest."
)
MAX_PREVIEW_ROWS = 500_000


class PreviewError(ValueError):
    """A bounded error with a safe worker/API code and message."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def _canonical(value):
    return json.loads(json.dumps(value, sort_keys=True))


class DeepFMPreview:
    """Load once on first access, then score small read-only profile batches."""

    def __init__(self, gold_dir=ROOT / "data/gold",
                 selection_path=ROOT / "training/results/phase5_selection.json",
                 profile_count=3, examples_per_profile=8):
        for name, value in (("profile_count", profile_count),
                            ("examples_per_profile", examples_per_profile)):
            if type(value) is not int or not 1 <= value <= 20:
                raise ValueError(f"{name} must be an integer from 1 to 20")
        self.gold_dir = Path(gold_dir).resolve()
        self.selection_path = Path(selection_path).resolve()
        self.profile_count = profile_count
        self.examples_per_profile = examples_per_profile
        self._loaded = False
        self._failure = None
        self._profiles = {}

    def _ensure_loaded(self):
        if self._failure is not None:
            raise PreviewError("model_unavailable", UNAVAILABLE) from self._failure
        if self._loaded:
            return
        try:
            self._load()
            self._loaded = True
        except Exception as error:
            self._failure = error
            # Log only to the worker's diagnostic channel, never its JSON API.
            print(f"DeepFM preview initialization failed: {error}", file=sys.stderr, flush=True)
            raise PreviewError("model_unavailable", UNAVAILABLE) from error

    def _load(self):
        selected = json.loads(self.selection_path.read_text())["models"]["deepfm"]
        config = selected["config"]
        if (config["model"] != "deepfm" or config.get("evaluate_test") is not False
                or config.get("max_train_batches") is not None
                or config.get("max_eval_batches") is not None):
            raise ValueError("Selected DeepFM configuration must be a complete pre-test run")
        checkpoint_path = (ROOT / selected["run_dir"] / "best.pt").resolve()
        checkpoint_bytes = checkpoint_path.read_bytes()
        checkpoint_sha = hashlib.sha256(checkpoint_bytes).hexdigest()
        if checkpoint_sha != selected["checkpoint_sha256"]:
            raise ValueError("Selected checkpoint SHA256 does not match the frozen manifest")
        checkpoint = torch.load(io.BytesIO(checkpoint_bytes), map_location="cpu", weights_only=True)
        spec = load_spec(self.gold_dir)
        spec_sha = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        if (spec != checkpoint["feature_spec"] or selected["feature_spec_sha256"] != spec_sha
                or _canonical(checkpoint["model_config"]) != _canonical(config)
                or checkpoint["epoch"] != selected["best_epoch"]
                or checkpoint["val_metrics"] != selected["val"]):
            raise ValueError("Checkpoint config/spec/selection metadata differs from the frozen manifest")
        if (type(selected["best_epoch"]) is not int
                or not 1 <= selected["best_epoch"] <= config["epochs"]):
            raise ValueError("Selected checkpoint epoch is invalid")
        for tensor in checkpoint["model_state_dict"].values():
            if not isinstance(tensor, torch.Tensor) or not torch.isfinite(tensor).all():
                raise ValueError("Checkpoint model state must contain finite tensors")
        torch.set_num_threads(2)
        torch.use_deterministic_algorithms(True)
        with torch.random.fork_rng():
            model = DeepFM(spec, embedding_dim=config["embedding_dim"],
                           hidden_dims=config["hidden_dims"], dropout=config["dropout"])
        model.load_state_dict(checkpoint["model_state_dict"], strict=True)
        model.eval()
        model.requires_grad_(False)
        self._spec = spec
        self._model = model
        self._checkpoint_sha = checkpoint_sha
        self._spec_sha = spec_sha
        self._selected_epoch = selected["best_epoch"]
        self._profiles = self._read_profiles(spec)
        for profile in self._profiles.values():
            self._batch(profile["rows"])

    def _read_profiles(self, spec):
        feature_columns = ([field["name"] for field in spec["categorical"]]
                           + spec["numeric"] + spec["flags"] + [spec["sequence"]["name"]])
        columns = list(dict.fromkeys(["user_id", "video_id", "time_ms", *feature_columns]))
        targets = {"label", spec.get("label")}
        if targets.intersection(columns):
            raise ValueError("Targets cannot be read by the inference preview")
        tables, rows = [], 0
        files = sorted((self.gold_dir / "train").glob("*.parquet"))
        if not files:
            raise ValueError("Train Parquet files are required for the preview")
        for path in files:
            row_count = pq.ParquetFile(path).metadata.num_rows
            if rows + row_count > MAX_PREVIEW_ROWS:
                raise ValueError("Insufficient complete profiles within the bounded training sample")
            tables.append(pq.read_table(path, columns=columns))
            rows += row_count
            table = pa.concat_tables(tables)
            users = table["user_id"].to_numpy()
            videos = table["video_id"].to_numpy()
            times = table["time_ms"].to_numpy()
            if not (np.issubdtype(users.dtype, np.integer)
                    and np.issubdtype(videos.dtype, np.integer)
                    and np.issubdtype(times.dtype, np.integer)):
                raise ValueError("Historical user/video IDs and timestamps must be non-null integers")
            order = np.lexsort((np.arange(len(users)), videos, times, users))
            sorted_users = users[order]
            starts = np.r_[0, np.flatnonzero(sorted_users[1:] != sorted_users[:-1]) + 1]
            ends = np.r_[starts[1:], len(order)]
            profiles = {}
            for start, end in zip(starts, ends):
                indices, seen = [], set()
                for index in order[start:end]:
                    item_id = int(videos[index])
                    if item_id not in seen:
                        seen.add(item_id)
                        indices.append(int(index))
                    if len(indices) == self.examples_per_profile:
                        break
                if len(indices) < self.examples_per_profile:
                    continue
                user_id = str(int(users[indices[0]]))
                selected_rows = table.take(pa.array(indices, type=pa.int64())).to_pylist()
                profiles[user_id] = {"id": user_id, "name": f"Profile {len(profiles) + 1}",
                                     "example_count": len(selected_rows), "rows": selected_rows}
                if len(profiles) == self.profile_count:
                    return profiles
        raise ValueError("Training sample does not contain enough distinct-item profiles")

    def _batch(self, rows):
        spec = self._spec
        categorical = np.asarray([[row[field["name"]] for field in spec["categorical"]]
                                  for row in rows], dtype=np.int64)
        numeric = np.asarray([[row[name] for name in spec["numeric"] + spec["flags"]]
                              for row in rows], dtype=np.float32)
        sequence = np.asarray([row[spec["sequence"]["name"]] for row in rows], dtype=np.int64)
        if not np.isfinite(numeric).all() or sequence.shape != (len(rows), spec["sequence"]["length"]):
            raise ValueError("Training example tensors have invalid values or shapes")
        for index, field in enumerate(spec["categorical"]):
            if ((categorical[:, index] < 0) | (categorical[:, index] >= field["vocab_size"])).any():
                raise ValueError("Categorical indices exceed the saved vocabulary")
        if ((sequence < 0) | (sequence >= spec["sequence"]["vocab_size"])).any():
            raise ValueError("History indices exceed the saved vocabulary")
        return {"categorical": torch.from_numpy(categorical),
                "numeric": torch.from_numpy(numeric), "sequence": torch.from_numpy(sequence)}

    def status(self):
        try:
            self._ensure_loaded()
        except PreviewError:
            return {"state": "unavailable", "model": "DeepFM", "message": UNAVAILABLE,
                    "source_split": "train", "scope": SCOPE, "media_mapping": False}
        return {"state": "ready", "model": "DeepFM", "checkpoint_sha256": self._checkpoint_sha,
                "selected_epoch": self._selected_epoch, "source_split": "train",
                "feature_spec_sha256": self._spec_sha, "profile_count": len(self._profiles),
                "examples_per_profile": self.examples_per_profile, "scope": SCOPE,
                "media_mapping": False}

    def profiles(self):
        self._ensure_loaded()
        return [{key: profile[key] for key in ("id", "name", "example_count")}
                for profile in self._profiles.values()]

    def score(self, profile_id):
        self._ensure_loaded()
        if not isinstance(profile_id, str) or profile_id not in self._profiles:
            raise PreviewError("unknown_profile", "Choose one of the available recorded profiles.")
        rows = self._profiles[profile_id]["rows"]
        with torch.inference_mode():
            logits = self._model(self._batch(rows))
            if logits.shape != (len(rows),) or not torch.isfinite(logits).all():
                raise ValueError("The model returned invalid logits")
            values = logits.double()
            probabilities = values.sigmoid().tolist()
        examples = [{"video_id": str(row["video_id"]), "recorded_at_ms": int(row["time_ms"]),
                     "probability": probability, "logit": float(logit)}
                    for row, probability, logit in zip(rows, probabilities, values.tolist())]
        examples.sort(key=lambda example: (-example["probability"], int(example["video_id"])))
        for rank, example in enumerate(examples, start=1):
            example["rank"] = rank
        return {"profile_id": profile_id, "source_split": "train", "scope": SCOPE,
                "model": "DeepFM", "checkpoint_sha256": self._checkpoint_sha,
                "examples": examples, "explanation": EXPLANATION}


def handle_request(preview, request):
    """One worker request; errors remain structured and safe for the browser."""
    request_id = request.get("id") if isinstance(request, dict) else None
    if type(request_id) not in (str, int):
        request_id = None
    if (not isinstance(request, dict) or request_id is None
            or request.get("method") not in ("status", "profiles", "score")):
        return {"id": request_id, "error": {"code": "invalid_request", "message": "Unknown preview request."}}
    try:
        method = request["method"]
        result = preview.status() if method == "status" else preview.profiles() if method == "profiles" else preview.score(request.get("profile_id"))
        return {"id": request_id, "result": result}
    except PreviewError as error:
        return {"id": request_id, "error": {"code": error.code, "message": str(error)}}
    except Exception as error:
        print(f"DeepFM preview request failed: {error}", file=sys.stderr, flush=True)
        return {"id": request_id, "error": {"code": "model_unavailable", "message": UNAVAILABLE}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-dir", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--selection-manifest", type=Path,
                        default=ROOT / "training/results/phase5_selection.json")
    args = parser.parse_args(argv)
    preview = DeepFMPreview(args.gold_dir, args.selection_manifest)
    for line in sys.stdin:
        try:
            if len(line) > 65_536:
                raise ValueError("Request is too large")
            request = json.loads(line)
            response = handle_request(preview, request)
        except (ValueError, TypeError):
            response = {"id": None, "error": {"code": "invalid_request", "message": "Use a valid JSON preview request."}}
        print(json.dumps(response, allow_nan=False, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
