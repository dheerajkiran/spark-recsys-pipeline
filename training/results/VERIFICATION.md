# Phase 5 verification — 2026-10-23

**Model results and implementation checks are verified. Phase acceptance remains
failed on the original shuffled-label AUC gate.** This report does not waive or
replace that requirement. Final Git commit/PR checks remain pending.

## Results and selection

Defaults from checkpoint 5.4 were used without a hyperparameter search:
CPU, two threads, seed 42, batch size 4,096, Adam learning rate 0.001,
20-epoch maximum, validation-AUC patience 3. DeepFM embeddings are size 16,
MLP widths 128/64, dropout 0. LR selects epoch 16/19; DeepFM selects epoch 1/4.
All epochs read the full 1,262,136 training rows.

The models were trained twice using train/validation only. Histories, model and
Adam states, checkpoint bytes, and validation predictions match exactly. The
selected checkpoint hashes, configurations and validation metrics were frozen in
[phase5_selection.json](phase5_selection.json) before final evaluation. Each
selected checkpoint then evaluated `test` and `test_random` once. Independent
verification checks the freeze/evaluation timestamps and completion markers.
No model choices or calibration were changed using held-out results.

Full-precision metrics and histories: [phase5_results.json](phase5_results.json).
Run artifacts are local and ignored by Git:

| Model | Selected run | Repeat run |
|---|---|---|
| LR | `checkpoints/phase55-lr-default` | `checkpoints/phase55-lr-repeat` |
| DeepFM | `checkpoints/phase55-deepfm-default` | `checkpoints/phase55-deepfm-repeat` |

## Standard phase protocol

| Check | Outcome | Evidence |
|---|---|---|
| S1 unit tests | PASS | 66 training tests (31.45 s); 47 Spark tests (12.30 s) |
| S2 mutation checks | PASS | Omitting checkpoint load, row checks, or exclusive evaluation guards makes targeted tests fail; production tests pass |
| S3 stage checks | PASS | 86 PASS, 0 FAIL, 0 quality WARN across ingest/clean/features/build_dataset |
| S4 independent recomputation | PASS | All six metric sets match sklearn and manual tied-rank AUC/BCE; saved labels match independently read gold order |
| S5 determinism | PASS | Full CPU repeats exact; decoded gold rows/schemas/order/vocabularies exact across old and fresh HDFS builds |
| S6 clean slate | PASS, isolated namespace | New project volumes started empty, raw reloaded, smoke and all stages run, rebuilt gold exported separately, fresh-env smokes pass |
| S7 earlier phases | PASS | Smoke reads 1,436,609 rows; 3 executors; Parquet roundtrip; replication 2; all Spark tests and pipeline checks pass |
| S8 documentation | Verified with failure retained | README numbers match full-precision results; control failure and random-log miscalibration are explicit |
| S9 Git finalization | PENDING | Branch `phase5-results` is based on `main` at `6986567`; user commit and PR remain |

Clean-slate verification used isolated project `phase55-verify`, container names
`phase55-*`, fresh HDFS/event volumes, and no published ports. Images, service
DNS/hostnames, resources, commands and source were unchanged. Existing HDFS
volumes and `data/gold` were preserved. This exercises empty namespace/block
storage without deleting the original stack; it does not test a cold image build.

All **1,940,143 gold rows × 74 columns**, their order, schemas and 35
vocabularies (48,523 rows) match. Deterministic JSON metadata also matches except
for the explicitly explained run-config provenance: `5e508b6d655c` became
`7eca52447ee9` because checkpoint 5.4 added the `training` config section.
The old config was reconstructed from Git; every non-training section matches
the current config and each full-config hash matches its recorded provenance.
Elapsed timings and physical Parquet footer encoding-list order are excluded.

A fresh Python 3.10.21 environment was created from `requirements-train.txt`.
Both models completed one-epoch smoke runs on the freshly rebuilt gold; each
uses 8,192 train and 4,096 validation rows. These reproduce checkpoint 5.4
smoke metrics, predictions and model weights bit-for-bit. The fresh smokes never
evaluate held-out data.

Expected runtime warnings are native-Hadoop fallback to Java, truncated Spark
plan display, and global windows for vocabulary indexing/previous-day rates.
Sandbox OpenMP shared-memory restrictions required running multiprocessing
tests outside the sandbox; all 66 tests pass there.

## Phase-specific acceptance

| Check | Outcome |
|---|---|
| LR val AUC at least 0.696 | PASS: 0.757196 |
| DeepFM val AUC greater than LR | PASS: 0.759087, gain 0.001891 |
| Plausible validation AUC | PASS: both within 0.70–0.85 |
| Test discipline | PASS: selection frozen first; one evaluation per selected checkpoint/split |
| Complete train epochs | PASS: 1,262,136 rows in every full-run epoch |
| Metric correctness | PASS: sklearn plus independent rank-sum AUC/manual log loss |
| CPU reproducibility | PASS: both full runs repeated exactly |
| Fresh environment | PASS: pinned fresh env plus newly exported gold and both smokes |
| Calibration glance | REVIEWED: mean predictions/click rates reported; random-log overprediction remains |
| Shuffled-label AUC 0.49–0.51 | **FAIL: DeepFM 0.526577, LR diagnostic 0.552329** |

### Shuffled-label failure investigation

The utility globally permutes all training labels once with seed 42. Independent
reconstruction reproduced the permutation digest, all 1,262,136 labels, unchanged
positive count, every non-label training column and unchanged validation files.
627,830 labels changed. Permutation digest:
`b701ef720c14a47ad0825790e30012607395e332405745fbdb07a0da013c384a`.

DeepFM control validation AUC by epoch: **0.526577, 0.503216, 0.492765,
0.512752**. Best-AUC selection chooses epoch 1, and that original failed result
is retained. The subsequent fixed-default LR diagnostic also fails the gate:
**0.552329, 0.477679, 0.470256, 0.514372**. No control epoch or seed was chosen
afterward to create a passing result; neither control evaluates held-out data.

An untrained seed-42 DeepFM scores 0.454215 validation AUC. On the same data,
tiny scores proportional to plus/minus scaled `user_hist_ctr` rank at
0.678828/0.321172 despite arbitrarily small score magnitude. Thus one random
function of predictive features need not rank at exactly 0.50. The trained
control's initial-versus-final logit correlation is only -0.0343; the audit does
not claim it simply preserves initialization. These diagnostics explain why
the narrow single-run threshold warrants review; they do **not** establish that
leakage is absent or turn the failed gate into a pass. Phase 5 remains open.

### Random-log calibration

Random-log click rate is 18.17%, whereas mean predictions are 40.07% for LR and
41.44% for DeepFM. This exposure shift was anticipated; results are reported
without test-driven adjustment. Standard test mean predictions are 41.40% LR
and 44.10% DeepFM against 44.51% observed clicks.

## Reproducing the artifact audit

After reproducing the full runs and controls in fresh directories, this command
reads the saved predictions/checkpoints and never runs model inference:

```bash
.venv/bin/python scripts/verify_training_results.py \
  --run lr checkpoints/phase55-lr-default \
  --run deepfm checkpoints/phase55-deepfm-default \
  --reproduce checkpoints/phase55-lr-default checkpoints/phase55-lr-repeat \
  --reproduce checkpoints/phase55-deepfm-default checkpoints/phase55-deepfm-repeat \
  --include-holdouts \
  --control checkpoints/phase55-shuffled-control \
  --control checkpoints/phase55-shuffled-lr-diagnostic \
  --output checkpoints/phase55-verification/phase-acceptance.json
```

The audit correctly exits **1** because of the two control thresholds; all
selected-model integrity/reproducibility checks pass. Omitting controls produces
a model-integrity report, not a declaration that full phase acceptance passed.

To generate a reproducible isolated stack/runbook without contacting Docker:

```bash
.venv/bin/python scripts/prepare_verification_stack.py \
  --output-dir checkpoints/verification-recheck \
  --project phase55-recheck --prefix phase55-recheck
```

Use an unused project/prefix; follow its generated `RUNBOOK.md`. Do not reuse
existing volumes as clean-slate evidence. Detailed local logs/audits remain in
`checkpoints/phase55-verification/`, including `phase-acceptance.json`,
`selection-provenance-audit.json`, `gold-comparison.json`,
`config-provenance.json`, `fresh-smoke-comparison.json`, the test logs and all
four stage logs. Model predictions/checkpoints and datasets remain ignored.
