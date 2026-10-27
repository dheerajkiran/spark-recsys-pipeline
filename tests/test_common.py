from pipeline.common import DEFAULT_CONFIG, config_hash, load_config


def test_1k_config_overrides_only_its_measured_differences():
    base, big = load_config(), load_config(str(DEFAULT_CONFIG.parent / "pipeline-1k.yaml"))
    assert big["dataset"]["version"] == "1k"
    assert {key: path.split("///")[1] for key, path in big["paths"].items()} == {
        "bronze": "1k/bronze", "silver": "1k/silver", "features": "1k/features",
        "gold": "1k/gold", "metadata": "1k/gold/_metadata"}
    assert "extends" not in big
    assert big["features"] == {**base["features"], "history_tie_slack": 20}
    assert big["join"] == {**base["join"], "item_daily_strategy": "sortmerge", "broadcast_video_static": False}
    assert base["join"]["item_daily_strategy"] == "broadcast" and base["join"]["broadcast_video_static"] is True
    assert big["split"] == {**base["split"], "train": [20220407, 20220429]}
    assert {**big, "dataset": base["dataset"], "paths": base["paths"], "features": base["features"],
            "join": base["join"], "split": base["split"]} == base
    assert {**big["dataset"], "version": "pure"} == base["dataset"]
    assert config_hash(big) != config_hash(base)
