from pipeline.common import DEFAULT_CONFIG, config_hash, load_config


def test_1k_config_overrides_only_version_and_paths():
    base, big = load_config(), load_config(str(DEFAULT_CONFIG.parent / "pipeline-1k.yaml"))
    assert big["dataset"]["version"] == "1k"
    assert {key: path.split("///")[1] for key, path in big["paths"].items()} == {
        "bronze": "1k/bronze", "silver": "1k/silver", "features": "1k/features",
        "gold": "1k/gold", "metadata": "1k/gold/_metadata"}
    assert "extends" not in big
    assert {**big, "dataset": base["dataset"], "paths": base["paths"]} == base
    assert {**big["dataset"], "version": "pure"} == base["dataset"]
    assert config_hash(big) != config_hash(base)
