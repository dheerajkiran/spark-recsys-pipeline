"""Config loading, without Spark: also used by the pandas baseline benchmark."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.yaml"


def _merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        merged[key] = (_merge(merged[key], value)
                       if isinstance(value, dict) and isinstance(merged.get(key), dict) else value)
    return merged


def load_config(path: str = str(DEFAULT_CONFIG)) -> dict:
    """Read a YAML config. A file with `extends: <file>` overrides only the keys
    it sets in that base file (path relative to it), e.g. config/pipeline-1k.yaml."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    base = cfg.pop("extends", None)
    if base is None:
        return cfg
    return _merge(load_config(str(Path(path).parent / base)), cfg)
