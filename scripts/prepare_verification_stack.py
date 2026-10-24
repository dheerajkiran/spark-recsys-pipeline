#!/usr/bin/env python3
"""Generate an isolated clean-slate Compose file and runbook; never run Docker."""

import argparse
import copy
from pathlib import Path
import re
import shlex

import yaml


ROOT = Path(__file__).resolve().parents[1]


def prepare_stack(output_dir, project="phase55-verify", prefix="phase55"):
    for name, value in (("project", project), ("prefix", prefix)):
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", value):
            raise ValueError(f"{name} must contain lowercase letters, digits, underscores or hyphens")
    output = Path(output_dir).resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError(f"Output directory must be empty: {output}")
    source = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    volumes = {name: copy.deepcopy(definition or {}) for name, definition in source["volumes"].items()}
    # Let Compose scope every new volume to the selected project.
    for definition in volumes.values():
        definition.pop("name", None)
        definition.pop("external", None)
    proposal = {"name": project, "services": {}, "volumes": volumes}
    for name, original in source["services"].items():
        service = copy.deepcopy(original)
        service["container_name"] = f"{prefix}-{name}"
        service.pop("ports", None)
        if isinstance(service.get("build"), str):
            service["build"] = str((ROOT / service["build"]).resolve())
        for index, mount in enumerate(service.get("volumes", [])):
            host, *rest = mount.split(":")
            if host.startswith("."):
                service["volumes"][index] = ":".join([str((ROOT / host).resolve()), *rest])
        proposal["services"][name] = service

    # Fail if isolation accidentally changes commands, DNS, images or sizing.
    assert set(proposal["services"]) == set(source["services"])
    assert set(proposal["volumes"]) == set(source["volumes"])
    excluded = {"container_name", "ports", "build", "volumes"}
    for name, service in proposal["services"].items():
        assert {k: v for k, v in service.items() if k not in excluded} == {
            k: v for k, v in source["services"][name].items() if k not in excluded}
        assert service["hostname"] == name and "ports" not in service
        assert service["container_name"] == f"{prefix}-{name}"
        assert Path(service["build"]).is_absolute()
        for mount in service.get("volumes", []):
            host = mount.split(":")[0]
            assert host in volumes or Path(host).is_absolute()

    compose = output / "compose.yaml"
    q = shlex.quote
    command = f"docker compose -p {q(project)} -f {q(str(compose))}"
    namenode, master = f"{prefix}-namenode", f"{prefix}-spark-master"
    export = output / "gold"
    stages = "\n".join(f"docker exec {master} spark-submit /opt/project/pipeline/{stage}.py"
                       for stage in ("ingest", "clean", "features", "build_dataset"))
    volume_names = ", ".join(f"`{project}_{name}`" for name in volumes)
    runbook = f'''# Isolated clean-slate verification

Generated from `{ROOT / 'docker-compose.yml'}`. All service DNS names, image
tags, commands, environments, resources, and health checks are preserved.
Container names use `{prefix}-`; no host ports are published. Bind/build paths
are absolute, and volumes/network are scoped to project `{project}`.

Confirm these volumes do not exist before the first start: {volume_names}.
If they exist, regenerate with a new `--project` and `--prefix`; do not delete
existing volumes or reuse them as evidence of a clean slate. Keep the generated
Compose file unchanged. This utility has not contacted Docker.

From the repository root, start with already built project images:

```bash
{command} config --quiet
{command} up -d --no-build --wait
{command} ps
```

The repository scripts hard-code normal container names. Equivalent isolated
load, smoke, pipeline, and test commands follow. Run each command sequentially
and stop at the first nonzero exit code.

```bash
docker exec {namenode} test -d /data/raw/KuaiRand-Pure/data
docker exec {namenode} hdfs dfsadmin -safemode wait
docker exec {namenode} hdfs dfs -mkdir -p /raw/kuairand/pure /bronze /silver /features /gold/_metadata
docker exec {namenode} bash -c 'hdfs dfs -put -f /data/raw/KuaiRand-Pure/data/*.csv /raw/kuairand/pure/'
docker exec {master} spark-submit /opt/project/scripts/smoke_test.py
{stages}
docker exec -e PYTHONPATH=/opt/project:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip -e PYTHONDONTWRITEBYTECODE=1 {master} python3 -m pytest -p no:cacheprovider -q /opt/project/tests
```

Export rebuilt gold to a fresh local directory; existing `data/gold` stays intact.
Both export paths must be unused before these commands:

```bash
docker exec {namenode} hdfs dfs -test -e /gold/_metadata/feature_spec.json
docker exec {namenode} hdfs dfs -get /gold /tmp/verification-gold-export
docker cp {namenode}:/tmp/verification-gold-export {q(str(export))}
```

Run the phase entry point on train/val only, with unused output run directories:

```bash
.venv/bin/python training/train_deepfm.py --gold-dir {q(str(export))} --model lr --epochs 1 --max-train-batches 4 --max-eval-batches 2 --run-dir {q(str(output / 'lr-smoke'))}
.venv/bin/python training/train_deepfm.py --gold-dir {q(str(export))} --model deepfm --epochs 1 --max-train-batches 4 --max-eval-batches 2 --run-dir {q(str(output / 'deepfm-smoke'))}
```

Compare rebuilt decoded values, row order, schemas, vocabularies, and deterministic
metadata with the prior export. Ignore elapsed times and physical Parquet footer
encoding ordering. Record changed full-config hashes explicitly if only training
settings changed. Model comparison must use identical configuration and seeds;
leave `--evaluate-test` disabled during verification.

This exercises an empty HDFS namespace and block storage without destroying the
normal cluster. It covers documented S6/S7 loading, smoke, stages, tests and phase
entry points; it does not test deletion or builds from an empty image cache.
The stack retains the original approximately 11 GiB resource budget.
Stop only this stack afterward to release resources and retain evidence:

```bash
{command} stop
```
'''
    output.mkdir(parents=True, exist_ok=True)
    compose.write_text("# Isolated verification stack; generated without Docker execution.\n" +
                       yaml.safe_dump(proposal, sort_keys=False))
    (output / "RUNBOOK.md").write_text(runbook)
    return compose


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "checkpoints/phase55-verification/stack")
    parser.add_argument("--project", default="phase55-verify")
    parser.add_argument("--prefix", default="phase55")
    args = parser.parse_args(argv)
    try:
        print(prepare_stack(args.output_dir, args.project, args.prefix))
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
