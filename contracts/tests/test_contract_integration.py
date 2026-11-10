"""Cross-file checks that structural schemas alone cannot guarantee."""
import json

import pytest

from contracts import check
from contracts.data import ContractError


def test_recommendation_examples_point_to_bundled_playable_items():
    report = check.check_catalog_examples()
    assert report["playable_items"] == 8
    assert report["example_references_checked"] > 0


def test_schema_valid_but_nonexistent_recommendation_item_is_rejected(tmp_path, monkeypatch):
    examples = json.loads(check.EXAMPLES_PATH.read_text(encoding="utf-8"))
    example = next(example for example in examples if example["name"] == "deepfm-recommendations")
    example["value"]["items"][0]["item_id"] = "valid-shape-but-missing-item"
    changed = tmp_path / "examples.json"
    changed.write_text(json.dumps(examples), encoding="utf-8")
    monkeypatch.setattr(check, "EXAMPLES_PATH", changed)
    with pytest.raises(ContractError) as caught:
        check.check_catalog_examples()
    assert caught.value.code == "unknown_example_item"
