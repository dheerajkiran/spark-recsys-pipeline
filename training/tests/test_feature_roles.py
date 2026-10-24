"""Target-role checks harden the loader against malformed feature specifications."""

import copy
import json

import pytest

from training.dataset import load_spec, validate_feature_roles


SPEC = {
    "label": "is_click",
    "categorical": [{"name": "category", "vocab_size": 8}],
    "numeric": ["user_hist_ctr"], "flags": ["user_hist_ctr_missing"],
    "sequence": {"name": "history", "vocab_size": 10, "length": 4, "pad": 0},
}


@pytest.mark.parametrize("role", ["categorical", "numeric", "flags", "sequence"])
@pytest.mark.parametrize("target", ["label", "is_click"])
def test_rejects_gold_target_and_source_alias_in_every_feature_role(role, target):
    spec = copy.deepcopy(SPEC)
    if role == "categorical":
        spec[role][0]["name"] = target
    elif role == "sequence":
        spec[role]["name"] = target
    else:
        spec[role].append(target)
    with pytest.raises(ValueError, match="Target column"):
        validate_feature_roles(spec)


def test_load_spec_applies_target_role_guard(tmp_path):
    spec = copy.deepcopy(SPEC)
    spec["numeric"].append("label")
    (tmp_path / "_metadata").mkdir()
    (tmp_path / "_metadata/feature_spec.json").write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="Target column"):
        load_spec(tmp_path)


def test_source_alias_is_spec_driven_and_label_field_is_optional():
    spec = copy.deepcopy(SPEC)
    spec["label"] = "another_target"
    spec["numeric"].append("another_target")
    with pytest.raises(ValueError, match="another_target"):
        validate_feature_roles(spec)
    spec = copy.deepcopy(SPEC)
    del spec["label"]
    validate_feature_roles(spec)
    spec["numeric"].append("label")
    with pytest.raises(ValueError, match="Target column"):
        validate_feature_roles(spec)


def test_valid_history_features_and_normal_roles_load_unchanged(tmp_path):
    spec = copy.deepcopy(SPEC)
    spec["numeric"].append("is_click_history")
    validate_feature_roles(spec)
    (tmp_path / "_metadata").mkdir()
    (tmp_path / "_metadata/feature_spec.json").write_text(json.dumps(spec))
    assert load_spec(tmp_path) == spec
