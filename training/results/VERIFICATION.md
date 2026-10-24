# Phase 5 verification — 2026-10-23

**Model results and the corrected randomized-target sanity check pass.** The
original train-only control failed and remains recorded. Its interpretation was
corrected explicitly under `permutation-null-v1`, described below; the old
outcomes were not changed. Commit and merge provenance is recorded in the pull
request associated with this checkpoint.

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

Original full-precision metrics, histories and failed-control snapshot:
[phase5_results.json](phase5_results.json). Corrected protocol and results:
[phase5_control_fix.json](phase5_control_fix.json). Both original JSON reports,
selected checkpoints and all selected-model predictions remain byte-identical.
Run artifacts are local and ignored by Git:

| Model | Selected run | Repeat run |
|---|---|---|
| LR | `checkpoints/phase55-lr-default` | `checkpoints/phase55-lr-repeat` |
| DeepFM | `checkpoints/phase55-deepfm-default` | `checkpoints/phase55-deepfm-repeat` |

## Standard phase protocol

| Check | Outcome | Evidence |
|---|---|---|
| S1 unit tests | PASS | 123 training tests (32.43 s) after the fix; original verification had 47 Spark tests (12.30 s) |
| S2 mutation checks | PASS | Omitting checkpoint load, row checks, or exclusive evaluation guards makes targeted tests fail; production tests pass |
| S3 stage checks | PASS | 86 PASS, 0 FAIL, 0 quality WARN across ingest/clean/features/build_dataset |
| S4 independent recomputation | PASS | All six original metric sets and both new control metrics match sklearn/manual AUC/BCE; full train/val permutations and every non-label column independently checked |
| S5 determinism | PASS | Original full CPU repeats and new fixed-budget control repeats exact; decoded gold rows/schemas/order/vocabularies exact across old and fresh HDFS builds |
| S6 clean slate | PASS, isolated namespace | New project volumes started empty, raw reloaded, smoke and all stages run, rebuilt gold exported separately, fresh-env smokes pass |
| S7 earlier phases | PASS | Smoke reads 1,436,609 rows; 3 executors; Parquet roundtrip; replication 2; all Spark tests and pipeline checks pass |
| S8 documentation | Verified with history retained | README matches results; revised control method, historical failures and random-log miscalibration are explicit |
| S9 Git finalization | See associated PR | Branch `phase5-results` is based on `main` at `6986567`; original results are recorded in `01cee94`; PR metadata records final commits and merge |

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
Sandbox OpenMP shared-memory restrictions require running multiprocessing
tests outside the sandbox. The fix reruns the complete training suite. Spark,
pipeline and clean-slate results above are from the original verification;
their code is unchanged and those container jobs were not repeated for this fix.

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
| Randomized-target pipeline sanity, `permutation-null-v1` | **PASS: LR 0.500252, DeepFM 0.499360**, same 0.49–0.51 band |
| Historical train-only shuffled-label diagnostic | **Original FAIL retained:** DeepFM 0.526577, LR diagnostic 0.552329 |

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
the original experiment does not define a randomized-target validation null;
they do **not** establish that leakage is absent or turn its failure into a pass.

### Corrected protocol: `permutation-null-v1`

The old experiment randomized fitting targets while scoring against predictable
real validation targets, then maximized validation AUC over epochs. A random
learned function of predictive features need not rank those original targets at
0.50, and best-epoch selection introduces further optimism. The corrected
experiment randomizes targets for both fitting and scoring. This adapts the
feature/target-independence principle in
[scikit-learn 1.5's permutation test](https://scikit-learn.org/1.5/modules/generated/sklearn.model_selection/permutation_test_score.html)
to the existing fixed temporal train/validation split. It changes the question
being tested; it is not a rerun of the original diagnostic.

Before training, `training/permutation_control.py` writes a protocol and an exact
copy of the original pre-test selection manifest. It validates the original
checkpoint hashes/configs/specs/selected epochs, then initializes fresh models.
Architecture, Adam settings, batch size and model seed stay fixed. Training
budgets come from that original manifest: **LR 16 epochs, DeepFM 1**. No
validation is scored until the saved terminal checkpoint, with no early stopping
or best-control selection. The control seed stays 42, and the original
**0.49–0.51** band is unchanged.

Targets are globally permuted once within each split using independent streams:
train `default_rng(42)`, validation `default_rng([42, 1])`. Per-split class counts,
all features, schemas and row order are preserved. Only train/validation are
read or copied. The training permutation matches the old control. Validation
permutation SHA256 (little-endian int64 indices, sorted Parquet file order):
`327af9193150470c0061cf5cff5190087f5d2e93022f2b92bdd9571198603a76`.

| Model | Fixed epochs | Train rows per epoch | Control val rows | AUC | Log loss |
|---|---:|---:|---:|---:|---:|
| LR | 16 | 1,262,136 | 78,444 | 0.5002521106925855 | 0.6973312045819866 |
| DeepFM | 1 | 1,262,136 | 78,444 | 0.49935984889439367 | 0.6913575372086852 |

Both fresh runs pass. A complete repeated control reproduces histories,
model/Adam states, checkpoint bytes and predictions exactly. Independent audit
reconstructs both permutations, compares every non-label value and row order,
and recomputes metrics with sklearn and manual rank-sum/BCE. Hash checks confirm
all 14 original selected-run/report artifacts are unchanged. No original model
is retuned and no held-out inference is repeated.

Tests make the interpretation explicit:

- Both model scores are bit-identical when current batch labels are flipped or
  removed; target/source-target aliases are rejected in every feature role.
- Deliberately returning current-label scores yields AUC 1 and fails the new
  gate, including an independent audit of the saved scores.
- Changing control-validation targets cannot change the trained model state.
- The auditor rejects changed features/labels, wrong terminal epochs, widened
  bands, false pass claims, NaN metrics, invalid tolerances and nonfinite or empty
  checkpoint states.

This is a **pipeline sanity check**, not a permutation significance test: one
permutation provides no formal p-value. Randomizing validation targets can hide
a feature derived from the original target, so this check does not certify
precomputed features against leakage. Existing tests for strictly earlier
timestamps, same-timestamp exclusion and previous-day statistics remain
necessary. Saved-artifact auditing checks integrity and recomputes metrics;
it does not rerun inference to prove predictions came from the saved weights.

### Random-log calibration

Random-log click rate is 18.17%, whereas mean predictions are 40.07% for LR and
41.44% for DeepFM. This exposure shift was anticipated; results are reported
without test-driven adjustment. Standard test mean predictions are 41.40% LR
and 44.10% DeepFM against 44.51% observed clicks.

## Reproducing the artifact audit

Run the corrected control in a fresh directory after obtaining the selected
reference checkpoints:

```bash
.venv/bin/python training/permutation_control.py --gold-dir data/gold \
  --run-dir checkpoints/phase55-permutation-control \
  --selection-manifest training/results/phase5_selection.json --seed 42
```

This combined revised-protocol audit reads saved predictions/checkpoints and
never runs model inference:

```bash
.venv/bin/python scripts/verify_training_results.py \
  --run lr checkpoints/phase55-lr-default \
  --run deepfm checkpoints/phase55-deepfm-default \
  --reproduce checkpoints/phase55-lr-default checkpoints/phase55-lr-repeat \
  --reproduce checkpoints/phase55-deepfm-default checkpoints/phase55-deepfm-repeat \
  --include-holdouts \
  --permutation-control checkpoints/phase55-permutation-control \
  --output checkpoints/phase55-control-fix/revised-phase-acceptance.json
```

The revised-protocol audit exits **0**. The legacy `--control` option is unchanged:
using the two old train-only controls still correctly exits **1**. The original
failed audit is retained at `checkpoints/phase55-verification/phase-acceptance.json`.
Omitting all controls produces a model-integrity report only.

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
