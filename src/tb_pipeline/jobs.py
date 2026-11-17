"""Task entry points: one function per Databricks job task.

These wire the pure transformations to Delta tables. Each task runs its
checks on the staged data *before* writing, so data that fails a check never
reaches the next layer, and the failed task stops the rest of the job.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tb_pipeline import bronze, checks, gold, merge, parity, quality, silver
from tb_pipeline.config import SOURCES, WINDOW_YEARS, PipelineConfig
from tb_pipeline.reference import BASELINE_COMMIT

MANIFEST_SCHEMA = (
    "source string, path string, uf string, ano int, rows bigint, sha256 string, source_commit string"
)
QUALITY_TABLE = "quality_results"
MERGE_LOG_TABLE = "merge_log"


def _years_in(years) -> str:
    return ", ".join(str(int(y)) for y in years)


def _ensure_schemas(spark: SparkSession, cfg: PipelineConfig) -> None:
    for schema in (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema, cfg.ops_schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")


def _overwrite(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def _replace_partitions(spark: SparkSession, df: DataFrame, table: str, years) -> None:
    """Idempotent bronze load: replace only the yearly partitions being loaded."""
    writer = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(table):
        writer = writer.option("replaceWhere", f"_particao_ano IN ({_years_in(years)})").option(
            "mergeSchema", "true"
        )
    writer.saveAsTable(table)


def _log_merge(spark: SparkSession, cfg: PipelineConfig, table: str, metrics: dict) -> None:
    print(f"MERGE {table}: {metrics}")
    row = [
        (
            datetime.now(timezone.utc).replace(tzinfo=None),
            table,
            _years_in(cfg.years),
            bool(metrics["created"]),
            metrics["num_inserted_rows"],
            metrics["num_updated_rows"],
            metrics["num_deleted_rows"],
        )
    ]
    schema = (
        "run_ts timestamp, table_name string, years string, created boolean, "
        "inserted bigint, updated bigint, deleted bigint"
    )
    spark.createDataFrame(row, schema).write.format("delta").mode("append").saveAsTable(
        cfg.table("ops", MERGE_LOG_TABLE)
    )


# ------------------------------------------------------------------ bronze

def run_bronze(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Landing Volume -> bronze Delta tables, verified against the manifest."""
    _ensure_schemas(spark, cfg)
    manifest = spark.read.schema(MANIFEST_SCHEMA).json(cfg.manifest_path)
    gate = quality.QualityGate("bronze")

    staged = []
    for source in SOURCES:
        years = cfg.years if source.incremental else WINDOW_YEARS
        landed = bronze.add_bronze_metadata(
            bronze.read_landing(spark, f"{cfg.landing_path}/{source.name}"), source.partition_cols
        ).where(F.col("_particao_ano").isin([int(y) for y in years]))
        if source.name != "sim_tb_deaths":
            # SIM legitimately lacks some state x year files (ADR-001); SINAN
            # and IBGE must have every requested year.
            checks.manifest_covers_years(gate, manifest, source.name, years)
        checks.bronze_matches_manifest(gate, landed, manifest, source.name, years)
        staged.append((source, years, landed))

    quality.finish(spark, gate, cfg.table("ops", QUALITY_TABLE))

    _overwrite(manifest, cfg.table("bronze", "landing_manifest"))
    for source, years, landed in staged:
        _replace_partitions(spark, landed, cfg.table("bronze", source.name), years)


# ------------------------------------------------------------------ silver

def run_silver(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Bronze -> silver, incrementally with MERGE."""
    gate = quality.QualityGate("silver")
    years = [int(y) for y in cfg.years]

    # Reference data: always the whole window.
    pop_bronze = spark.table(cfg.table("bronze", "ibge_population"))
    pop_staged = merge.with_record_hash(
        merge.with_natural_key(silver.transform_population(pop_bronze), ["ano", "codigo_municipio"])
    )
    checks.population(gate, pop_bronze, pop_staged, WINDOW_YEARS)

    # Event data: only the yearly source files requested.
    sinan_bronze = merge.with_content_key(
        spark.table(cfg.table("bronze", "sinan_tb")).where(F.col("_particao_ano").isin(years)),
        scope_columns=["_particao_ano"],
    )
    sinan_staged = merge.with_record_hash(
        silver.transform_sinan(sinan_bronze, pop_staged.drop(*merge.AUDIT_COLUMNS), WINDOW_YEARS)
    )
    checks.sinan(gate, sinan_bronze, sinan_staged, WINDOW_YEARS)

    sim_bronze = merge.with_content_key(
        spark.table(cfg.table("bronze", "sim_tb_deaths")).where(F.col("_particao_ano").isin(years)),
        scope_columns=["_particao_uf", "_particao_ano"],
    )
    sim_staged = merge.with_record_hash(silver.transform_sim(sim_bronze))
    checks.sim(gate, sim_staged)

    dim_staged = merge.with_record_hash(
        merge.with_natural_key(
            silver.build_dim_municipio(pop_staged, WINDOW_YEARS), ["codigo_municipio", "valid_from"]
        )
    )
    checks.dim_municipio(gate, dim_staged)

    # Nothing is written unless every staged dataset passed.
    quality.finish(spark, gate, cfg.table("ops", QUALITY_TABLE))

    scope = f"t._particao_ano IN ({_years_in(years)})"
    for name, staged, delete_scope in (
        ("stg_ibge__populacao_municipio", pop_staged, None),
        ("dim_municipio", dim_staged, None),
        ("stg_sinan__tuberculose", sinan_staged, scope),
        ("stg_sim__obitos_tb", sim_staged, scope),
    ):
        table = cfg.table("silver", name)
        _log_merge(spark, cfg, table, merge.merge_into(spark, staged, table, delete_scope))


# -------------------------------------------------------------------- gold

def run_gold(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Silver -> gold. Small aggregates, fully rebuilt on every run."""
    gate = quality.QualityGate("gold")
    sinan = spark.table(cfg.table("silver", "stg_sinan__tuberculose"))
    sim = spark.table(cfg.table("silver", "stg_sim__obitos_tb"))
    population = spark.table(cfg.table("silver", "stg_ibge__populacao_municipio"))
    dim_municipio = spark.table(cfg.table("silver", "dim_municipio"))
    manifest = spark.table(cfg.table("bronze", "landing_manifest"))

    # Years actually loaded so far: with a partial load the reconciliation
    # grid must not report never-loaded years as "SIM coverage missing".
    loaded_years = sorted(
        r[0]
        for r in spark.table(cfg.table("bronze", "sinan_tb")).select("_particao_ano").distinct().collect()
    )

    dim_uf = gold.build_dim_uf(spark)
    incidencia = gold.build_fct_incidencia(sinan, population, dim_municipio)
    abandono = gold.build_fct_taxa_abandono(sinan)
    abandono_uf = gold.build_fct_taxa_abandono_uf(sinan, dim_uf)
    coverage = gold.build_sim_coverage(
        dim_uf, manifest.where(F.col("source") == "sim_tb_deaths"), loaded_years
    )
    reconciliacao = gold.build_fct_reconciliacao(sinan, sim, dim_uf, coverage)

    checks.gold(gate, sinan, dim_uf, incidencia, abandono, abandono_uf, reconciliacao, loaded_years)
    quality.finish(spark, gate, cfg.table("ops", QUALITY_TABLE))

    for name, df in (
        ("dim_uf", dim_uf),
        ("fct_incidencia_tb", incidencia),
        ("fct_taxa_abandono", abandono),
        ("fct_taxa_abandono_uf", abandono_uf),
        ("fct_reconciliacao_sinan_sim", reconciliacao),
    ):
        _overwrite(df, cfg.table("gold", name))


# ------------------------------------------------------------------ parity

def run_parity(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Compare the new gold against the original gold and publish the report.

    Fails the job on any difference when the whole window was processed. A
    partial run cannot match a full-window baseline, so it only reports.
    """
    results = []
    for table, keys in parity.PARITY_KEYS.items():
        new = spark.table(cfg.table("gold", table))
        baseline = spark.read.parquet(cfg.baseline_gold_path(table))
        result = parity.compare(table, new, baseline, keys)
        print(
            f"{table}: rows new={result.rows_new} baseline={result.rows_baseline} "
            f"only_new={result.only_in_new} only_baseline={result.only_in_baseline} "
            f"mismatches={result.column_mismatches} -> {'PASS' if result.passed else 'FAIL'}"
        )
        results.append(result)

    strict = cfg.is_full_window
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    report = parity.render_markdown(
        results,
        baseline_commit=BASELINE_COMMIT,
        years=_years_in(cfg.years),
        generated_at=generated_at,
        strict=strict,
        engine=f"Databricks serverless, Spark {spark.version}",
    )
    path = Path(cfg.parity_report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
    print(report)

    rows = [
        (
            datetime.now(timezone.utc).replace(tzinfo=None),
            r.table,
            r.rows_new,
            r.rows_baseline,
            r.only_in_new,
            r.only_in_baseline,
            sum(r.column_mismatches.values()),
            r.passed,
            _years_in(cfg.years),
        )
        for r in results
    ]
    schema = (
        "run_ts timestamp, table_name string, rows_new bigint, rows_baseline bigint, only_in_new bigint, "
        "only_in_baseline bigint, cells_differing bigint, passed boolean, years string"
    )
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").saveAsTable(
        cfg.table("ops", "parity_results")
    )

    failed = [r.table for r in results if not r.passed]
    if failed and strict:
        raise quality.QualityGateError(f"Parity failed for: {', '.join(failed)}")
