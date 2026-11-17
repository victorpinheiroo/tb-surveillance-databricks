# ADR-001 — From pandas to PySpark + Delta on Databricks Free Edition

- Status: accepted
- Date: 2026-11-16
- Baseline: [`tb-surveillance-datasus@cdfcd29`](https://github.com/victorpinheiroo/tb-surveillance-datasus/tree/cdfcd297fed92ade2dba524fcb807cfc8508aa26)

## Context

The original pipeline is a set of pandas scripts orchestrated by GitHub Actions: it downloads SINAN-TB, SIM and IBGE data, writes bronze, silver and gold as Parquet files committed to the repository, and rewrites everything on each monthly run.

This project rebuilds it on Databricks with PySpark and Delta, under two constraints:

1. **The output must be provably the same.** A migration that "looks right" is not a result.
2. **Zero cost.** Databricks Free Edition only, no paid cloud.

## What Free Edition imposes

| Limit | Consequence for the design |
|---|---|
| Serverless compute only; outbound internet restricted to trusted domains | No download from DATASUS inside the workspace. GitHub Actions prepares the files and uploads them to a Volume. |
| Serverless runs on Spark Connect | No RDD API, no `sparkContext`, no `input_file_name()`. The code uses DataFrame functions only and `_metadata.file_path` for lineage. |
| ANSI SQL mode is on | Malformed values raise instead of becoming NULL. Every cast of source data goes through `try_cast`, `try_to_timestamp` or `try_divide`. |
| Daily quota; exceeding it stops compute for the day | A `years` job parameter allows a one-year first run. Each check is one aggregation. Gold is small enough to rebuild fully. |
| At most five concurrent job tasks per account | One job, four sequential tasks, `max_concurrent_runs: 1`. |
| Idle accounts may be deleted | The repository must stand alone: local runner, CI parity job, committed report. |

## Decisions

### 1. Ingestion outside Databricks, with a manifest as the contract

The workflow checks out the original repository at a pinned commit, copies the raw bronze Parquet files unchanged into a landing folder and writes `manifest.jsonl` with the row count and SHA-256 of each file. The bronze task compares what it read against the manifest, per file, and fails on any difference.

SIM coverage (`status_cobertura_sim`) is derived from this manifest, i.e. from which files were landed, as the original derives it from which files exist. A state and year with a file and zero deaths is different from one with no file.

*Alternative considered:* download inside Databricks. Rejected: Free Edition restricts outbound internet to a list of trusted domains, and the download is slow (in the original project the SIM download alone ran past 46 minutes), which is a poor use of a daily compute quota.

### 2. Pinned baseline

The original repository re-runs on the first of each month and commits new data. Input and expected output are both taken from commit `cdfcd29`, so the comparison is deterministic. Moving the baseline is a deliberate change to one constant and two workflow variables.

### 3. Transformations as pure functions

Each step of the original scripts became a function from DataFrame to DataFrame in `src/tb_pipeline/`, with no I/O. The Databricks notebooks are thin wrappers. This is what makes the same code run in three places: unit tests, the local runner, and the job.

### 4. Silver is incremental with `MERGE`; the key is derived from the row

SINAN-TB has no trustworthy case identifier, and the original project (its ADR-002) rejected building one from quasi-identifiers after measuring false positives. This project keeps that decision, which rules out a business key for `MERGE`.

- `_row_key` = hash of every source column plus the year of the source file, with an occurrence number so that fully identical rows are all preserved.
- `_record_hash` = hash of the transformed output, excluding lineage columns that change on every re-download.
- `MERGE ... WHEN MATCHED AND hashes differ THEN UPDATE / WHEN NOT MATCHED THEN INSERT / WHEN NOT MATCHED BY SOURCE AND <scope> THEN DELETE`, where the scope is the set of yearly files being reprocessed.

Consequences:

- A re-run with unchanged data performs zero writes.
- A record revised upstream is one delete and one insert, not an update, because its identity is its content. There is no way to say "this is the same case, changed" without a business key. This is stated here so that nobody reads the `merge_log` counts as business events.
- Deletes never leave the reprocessed files, so loading one year cannot remove rows of another.

Reference data (IBGE population and the municipality dimension) uses its natural key and is always processed for the whole window: a notification in the 2015 file can have `NU_ANO = 2016` and needs the 2016 population, so population cannot be scoped to the requested years.

*Alternative considered:* overwrite silver partitions by notification year. Rejected: a yearly file contributes to several notification years, so partition overwrite by `NU_ANO` would drop rows contributed by files not being reprocessed.

### 5. Gold is rebuilt in full

The gold tables hold a few tens of thousands of rows. Incremental logic there would add risk for no saving.

### 6. Quality checks block before the write

Checks run on staged DataFrames; results are appended to `tb_ops.quality_results`; then the task raises if any blocking check failed. A failed task stops the downstream tasks of the job. Diagnostics that are informative but not errors (rows dropped by a rule, rows with unknown municipality) are recorded with severity `info`.

## What changed from pandas to Spark

These are the places where a line-by-line translation would have produced a different or fragile result.

### Row order does not exist

`fct_incidencia_tb.status_maturidade` and the two abandonment tables used `groupby(...).agg("first")`. In pandas, "first" follows row order; rows are concatenated file by file, so the result is the status carried by the oldest yearly file that contributed to the group. Example: notifications with `NU_ANO = 2024` appear both in the 2024 file (`provisorio`) and in older, closed files (`fechado`); a municipality gets `fechado` if any of its 2024 notifications arrived in an older file.

Spark gives no ordering guarantee, and `first()` there is non-deterministic. The rule is now explicit: `min_by(_status_maturidade_estimado, _particao_ano)`. It reproduces the original exactly and does not depend on physical layout.

Whether this rule is the *right* business definition is a separate question, deliberately left as is: the goal here is parity.

### The yearly file is provenance, not the notification year

Bronze files are partitioned by the year requested from DATASUS, but `NU_ANO` inside a file spans many years. The original filters by `NU_ANO` when writing silver, which drops 4,903 rows outside 2015–2024 as a side effect of a partitioned write. In Spark this is `filter_notification_window`, a named step, with the dropped count recorded as a quality metric. The file year is kept as `_particao_ano`.

### NULL is not a string

pandas compares `str(value)`: a missing duplicate flag becomes `"None"`, is not equal to `"2"`, and the row is kept. In Spark `NULL = '2'` is NULL and the negated filter drops the row. Filters use `coalesce` or `eqNullSafe`. pandas `groupby` also drops NULL keys silently; the Spark version filters them explicitly.

### Coercion must be requested

pandas `errors="coerce"` has no default equivalent under ANSI mode. Dates (`yyyyMMdd` in SINAN, `ddMMyyyy` in SIM), years and population go through `try_*` functions. The SIDRA placeholder `"..."` becomes a NULL population, and the incidence of that row is NULL, never zero.

### A latent join fan-out

The original joins incidence to the SCD2 municipality dimension on `codigo_municipio` only. With one version per municipality (the case today) that is harmless; with two it would duplicate fact rows. The Spark version joins `is_current` only. The output is identical on current data and stays correct when a municipality is renamed.

### Schema drift across files

Column sets differ between years: three legacy columns in older SINAN files, `CRM` only in SIM files up to 2018. pandas `concat` unions them silently. Spark reads with `mergeSchema`, and the `MERGE` helper aligns staged data to the target table (adding new columns, filling missing ones with NULL) so that a partial run does not fail on a narrower schema.

### Types

Dates are `DATE` instead of nanosecond timestamps, the SCD2 `valid_to` is a nullable integer instead of an untyped all-NULL column, and population in silver SINAN is `BIGINT` instead of the raw string. Gold column names and meanings are unchanged.

## How parity is established

1. `scripts/build_landing.py` flattens the original gold into one Parquet file per table.
2. `parity.compare` full-outer-joins new and original on the business key and counts rows on one side only and differing values per column (floats within 1e-9, NaN treated as NULL).
3. A full-window run fails if anything differs. A partial run only reports, because a subset of yearly files cannot reproduce a full-window result (a year receives notifications from several files).

The comparison runs in two places: on Databricks as the last task of the job, and in GitHub Actions with local Spark on every push, which keeps the proof valid even without a workspace.

## Not validated yet

- Deploying the Asset Bundle and running the job on a Free Edition workspace. Community reports say bundles with serverless jobs work there; this project has not confirmed it.
- Quota consumed by a one-year run and by a full-window run.
- `MERGE` on serverless specifically. The same SQL is tested against open-source Delta in CI.
