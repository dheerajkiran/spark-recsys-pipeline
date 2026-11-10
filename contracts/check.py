"""Check the API, examples, and complete seeded datasets without AWS or Spark."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from contracts.api import validate_api_contract
from contracts.data import ContractError, validate_dataset, validate_media
from contracts.fixtures.generate import CATALOG_PATH, generate

EXAMPLES_PATH = Path(__file__).parent / "examples" / "examples.json"


def check_catalog_examples():
    """Ensure published ranking examples refer to real bundled playable IDs."""
    catalog = validate_media(json.loads(CATALOG_PATH.read_text(encoding="utf-8")), check_assets=True)
    examples = json.loads(EXAMPLES_PATH.read_text(encoding="utf-8"))
    checked = 0
    for example in examples:
        if example["kind"] != "response" or not example["path"].endswith("/recommendations"):
            continue
        for item in example["value"]["items"]:
            if item["item_id"] not in catalog:
                raise ContractError("unknown_example_item", "Recommendation example does not reference the playable catalog.",
                                    example=example["name"])
            checked += 1
    return {"playable_items": len(catalog), "example_references_checked": checked}


def check(fixtures=None):
    api = validate_api_contract()
    catalog = check_catalog_examples()
    if fixtures is None:
        with tempfile.TemporaryDirectory(prefix="phase71-check-") as directory:
            data = generate(Path(directory) / "fixtures")
    else:
        root = Path(fixtures)
        directories = [root] if (root / "manifest.json").is_file() else sorted(root.glob("project-*"))
        if not directories:
            raise ContractError("missing_datasets", "No generated project datasets were found.")
        data = {"projects": [validate_dataset(directory) for directory in directories]}
    return {"checkpoint": "7.1", "scope": "local contract and synthetic fixture verification",
            "api": api, "catalog": catalog, "fixtures": data}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, help="Validate existing fixtures instead of generating temporary ones.")
    args = parser.parse_args()
    try:
        result = check(args.fixtures)
    except ContractError as exc:
        print(json.dumps({"error": exc.as_dict()}, sort_keys=True))
        raise SystemExit(1) from exc
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
