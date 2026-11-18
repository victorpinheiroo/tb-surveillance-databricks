# TB Surveillance on Databricks — the same pipeline, rebuilt in PySpark + Delta

A migration project. The [original pipeline](https://github.com/victorpinheiroo/tb-surveillance-datasus) processes real Brazilian tuberculosis surveillance data (SINAN-TB notifications, SIM death records, IBGE population) with pandas and Parquet files. This repository rebuilds it in **PySpark with Delta Lake on Databricks Free Edition** and **proves the result is identical**: every gold table is compared, row by row and column by column, against the output of the original.

Same sources, same gold tables, no new domain, no dashboard, no ML. The point is the migration and the proof.

## Parity report

<!-- parity:start -->
Result: **PASS** — every row and every column of every gold table matches the original.

- Engine: Databricks serverless, Spark 4.2.0
- Baseline: original pandas pipeline at commit `cdfcd29`
- Years processed by this run: `2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024`
- Generated at: 2026-11-18 01:06 (UTC)
- Float tolerance: 1e-09 (absolute); everything else compared exactly

| Table | Key | Rows (Spark) | Rows (original) | Only in Spark | Only in original | Columns compared | Cells differing | Status |
|---|---|---:|---:|---:|---:|---:|---:|:---:|
| `dim_uf` | uf_codigo | 27 | 27 | 0 | 0 | 2 | 0 | PASS |
| `fct_incidencia_tb` | ID_MN_RESI, NU_ANO | 41,074 | 41,074 | 0 | 0 | 6 | 0 | PASS |
| `fct_taxa_abandono` | ID_MN_RESI, NU_ANO | 39,710 | 39,710 | 0 | 0 | 4 | 0 | PASS |
| `fct_taxa_abandono_uf` | uf_codigo, ano | 270 | 270 | 0 | 0 | 5 | 0 | PASS |
| `fct_reconciliacao_sinan_sim` | uf_sigla, ano | 270 | 270 | 0 | 0 | 5 | 0 | PASS |

Aggregates (sum of each numeric column over the whole table):

| Table | Column | Spark | Original | Equal |
|---|---|---:|---:|:---:|
| `fct_incidencia_tb` | casos | 951,035 | 951,035 | yes |
| `fct_incidencia_tb` | populacao | 1,993,917,761 | 1,993,917,761 | yes |
| `fct_incidencia_tb` | incidencia_por_100k | 1,393,310.015543 | 1,393,310.015543 | yes |
| `fct_taxa_abandono` | n_casos_encerrados | 911,257 | 911,257 | yes |
| `fct_taxa_abandono` | n_abandono | 144,533 | 144,533 | yes |
| `fct_taxa_abandono` | taxa_abandono_pct | 371,301.804836 | 371,301.804836 | yes |
| `fct_taxa_abandono_uf` | n_casos_encerrados | 911,148 | 911,148 | yes |
| `fct_taxa_abandono_uf` | n_abandono | 144,520 | 144,520 | yes |
| `fct_taxa_abandono_uf` | taxa_abandono_pct | 3,907.110337 | 3,907.110337 | yes |
| `fct_reconciliacao_sinan_sim` | obitos_sim | 40,205 | 40,205 | yes |
| `fct_reconciliacao_sinan_sim` | obitos_sinan | 37,727 | 37,727 | yes |
| `fct_reconciliacao_sinan_sim` | diferenca_sim_menos_sinan | 11,138 | 11,138 | yes |
<!-- parity:end -->

How to read it: each gold table is full-outer-joined with the original on its business key. "Only in Spark" and "Only in original" count rows missing from one side; "Cells differing" counts values that differ in rows present on both. Floats are compared with an absolute tolerance of 1e-9, everything else exactly. The report is produced by [`src/tb_pipeline/parity.py`](src/tb_pipeline/parity.py); a full-window run fails if anything differs.

The baseline is pinned to commit [`cdfcd29`](https://github.com/victorpinheiroo/tb-surveillance-datasus/tree/cdfcd297fed92ade2dba524fcb807cfc8508aa26) of the original repository. That repository re-runs monthly and rewrites its data, so the input (bronze files) and the expected output (gold files) are both taken from that one commit.

## Status

| Piece | State |
|---|---|
| Transformations, quality checks, parity | Done. Run end to end on the full 2015–2024 data with local Spark: parity passes (report above). |
| Unit tests | 43 tests with local PySpark + Delta in ANSI mode, run by the CI workflow on every push. They include the Delta `MERGE` behaviour (idempotent re-run, scoped delete, update on derived change). |
| Parity in CI | The CI workflow rebuilds the landing folder from the pinned commit and runs the whole pipeline with Spark's own file reader on Linux, with a strict parity check. |
| Asset Bundle, job, upload workflow | Written, **not yet deployed**: no run on a Databricks workspace has happened yet. The bundle configuration parses with the Databricks CLI. |
| Run screenshots | Pending the first Databricks run (`docs/screenshots/`). |

The "Engine" line of the report says where it was produced. Until the first Databricks run, it is local Spark running the same transformation code without Delta.

## Architecture

```
GitHub Actions                                Databricks Free Edition (serverless)
──────────────                                ────────────────────────────────────
checkout original repo @ pinned commit
build landing folder + manifest  ── upload ─▶ Volume  tb_bronze.landing
databricks bundle deploy                        │
databricks bundle run                           ▼
                                              bronze   raw copy as Delta, verified against the manifest
                                                ▼
                                              silver   typed, decoded, deduplicated — incremental MERGE
                                                ▼
                                              gold     incidence, abandonment, SINAN × SIM reconciliation
                                                ▼
download parity report, commit  ◀─────────── parity   gold vs original gold, fails the job on any difference
```

One job, four tasks in sequence (`bronze → silver → gold → parity`), deployed with a Databricks Asset Bundle.

### Why ingestion happens outside Databricks

Free Edition offers only serverless compute, with outbound internet restricted to a short list of trusted domains, so downloading from DATASUS inside the workspace is not an option. GitHub Actions prepares the raw files and uploads them to a Unity Catalog Volume, together with a **manifest** (row count and SHA-256 of every file). The bronze task refuses to load unless each file it reads has exactly the row count in the manifest. That manifest is the contract between the two systems.

### Tables

| Layer | Table | Load |
|---|---|---|
| bronze | `sinan_tb`, `sim_tb_deaths`, `ibge_population`, `landing_manifest` | Replace the yearly partitions being loaded |
| silver | `stg_sinan__tuberculose`, `stg_sim__obitos_tb` | `MERGE`, scoped to the yearly files being reprocessed |
| silver | `stg_ibge__populacao_municipio`, `dim_municipio` (SCD2) | `MERGE`, always the whole window (reference data) |
| gold | `dim_uf`, `fct_incidencia_tb`, `fct_taxa_abandono`, `fct_taxa_abandono_uf`, `fct_reconciliacao_sinan_sim` | Full rebuild (small aggregates) |
| ops | `quality_results`, `merge_log`, `parity_results` | Append, one batch per run |

### Incremental silver without a business key

SINAN-TB has no reliable case identifier (the original project documents why it refuses to build one). The `MERGE` key is therefore derived from the row: a hash of all source columns plus the source file's year, with an occurrence number so identical rows are all kept. A second hash fingerprints the transformed output.

- Re-running the same data writes nothing.
- Loading one more year inserts its rows and leaves the other years untouched.
- A record revised at the source appears as one delete plus one insert, because its content hash changed. This is the honest cost of having no business key.
- A change in a derived value only (a revised population figure) is an in-place update.

## Quality gates

Checks run on the staged data **before** anything is written. A failed check raises, the task fails, and the remaining tasks of the job do not run. Results are appended to `tb_ops.quality_results` first, so a failed run leaves a record. Examples:

- **bronze** — row count of every landed file equals the manifest; every requested year has a file.
- **silver** — `(ano, codigo_municipio)` is unique; exactly one SIDRA label row per year was removed; no source-confirmed duplicate survives; no outcome code is missing from the dictionary; merge keys are unique.
- **gold** — the sum of cases equals the number of silver notifications (nothing lost or invented by aggregation); rates are within 0–100; the reconciliation difference is NULL exactly where SIM has no coverage.

## What the migration surfaced

Rebuilding the pipeline in an engine with no row order and strict typing exposed behaviour the pandas version relied on implicitly. Each is now explicit and tested; details in [ADR-001](docs/adrs/ADR-001-pandas-to-spark-on-free-edition.md).

1. **A yearly file is not a clean year.** The 2015 SINAN file carries notifications whose `NU_ANO` ranges from 1987 to 2022. The original partitions silver by `NU_ANO` and silently drops what falls outside 2015–2024 (4,903 rows). Here it is a named step with a recorded count.
2. **An aggregation that depended on row order.** `status_maturidade` used pandas `first`, which in practice meant "the value from the oldest file contributing to the group". Spark has no row order, so it is written as `min_by(status, source_file_year)`.
3. **A join that would fan out.** The incidence table joins the SCD2 municipality dimension on the code alone. It is correct today only because no municipality has more than one version. The Spark version joins the current version only.
4. **NULL semantics.** pandas keeps a row whose duplicate flag is missing; a naive Spark filter drops it. The filters are NULL-safe and covered by tests.

## Running it

**Locally, no Databricks needed** (Java 17 and Python 3.10+):

```bash
pip install -e ".[dev]"
```

```bash
pytest
```

```bash
git clone https://github.com/victorpinheiroo/tb-surveillance-datasus _source
```

```bash
git -C _source checkout cdfcd297fed92ade2dba524fcb807cfc8508aa26
```

```bash
python scripts/build_landing.py --source _source --out landing --source-commit cdfcd297fed92ade2dba524fcb807cfc8508aa26
```

```bash
python scripts/run_local.py --landing landing --years all --report docs/parity-report.md
```

`run_local.py` uses the same transformation, quality and parity code as the job, with DataFrames in memory instead of Delta tables. On Windows without `winutils` it reads the files through PyArrow, and the tests that need the local filesystem or Delta are skipped; they run in CI.

**On Databricks Free Edition:**

1. Create a personal access token in the workspace and add two repository secrets: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.
2. Run the **Databricks pipeline** workflow with `years = 2015` first, to measure quota use.
3. Run it again with `years = all`. The parity check is strict only for the whole window: a single yearly file cannot reproduce a full-window baseline (see finding 1), so partial runs report differences without failing.

## Repository structure

```
src/tb_pipeline/     transformations (bronze, silver, gold), merge, checks, parity, job entry points
notebooks/           one thin notebook per job task
resources/           Asset Bundle resources: schemas, volumes, job
scripts/             build_landing.py (runs in CI), run_local.py, publish_parity.py
tests/               unit tests (PySpark local, ANSI mode)
docs/adrs/           architecture decision records
.github/workflows/   ci.yml (tests + parity), databricks.yml (upload, deploy, run)
```

## Free Edition limits that shaped the design

- **Serverless only, restricted egress** — ingestion moved to GitHub Actions.
- **Daily compute quota** — the job takes a `years` parameter so a first run can process a single year; checks are single aggregations to keep the number of Spark actions low.
- **Five concurrent job tasks per account** — the job runs four tasks in sequence with `max_concurrent_runs: 1`.
- **Inactive accounts may be deleted** — nothing here depends on the workspace surviving: code, tests, parity report and the local runner all live in the repository.

## Known limitations

- The bundle, the job and the upload workflow have not been exercised on a real workspace yet.
- The Delta `MERGE` is tested against open-source Delta in CI, not yet on Databricks serverless. On Windows without `winutils` those tests are skipped.
- Dates are stored as `DATE` and population in silver SINAN as `BIGINT`, where the original kept timestamps and a raw string. Silver is therefore not type-identical to the original; the parity claim is about gold.
- Limitations of the data itself (case identity, SIM coverage gaps, unstable municipal rates) are unchanged and documented in the original repository.

---

Victor Pinheiro — [linkedin.com/in/victorpinheiroo](https://linkedin.com/in/victorpinheiroo) · [github.com/victorpinheiroo](https://github.com/victorpinheiroo)
