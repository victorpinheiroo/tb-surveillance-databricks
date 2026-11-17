"""Run the whole pipeline with local Spark and check parity, no Databricks needed.

Uses exactly the same transformation, quality-check and parity code as the
Databricks job; only the storage differs (DataFrames in memory instead of
Delta tables, so there is no MERGE here). This is what lets the repository
prove its result on its own: CI runs it on every push.

Usage:
    python scripts/build_landing.py --source _source --out landing --source-commit <sha>
    python scripts/run_local.py --landing landing --years all --report docs/parity-report.md
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from tb_pipeline import bronze, checks, gold, parity, quality, silver  # noqa: E402
from tb_pipeline.config import SOURCES, WINDOW_YEARS, parse_years  # noqa: E402
from tb_pipeline.jobs import MANIFEST_SCHEMA  # noqa: E402
from tb_pipeline.merge import with_content_key  # noqa: E402
from tb_pipeline.reference import BASELINE_COMMIT  # noqa: E402


def _session() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.master("local[*]")
        .appName("tb-pipeline-local")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def _finish(gate: quality.QualityGate) -> None:
    print(gate.summary())
    gate.raise_if_failed()


class SparkReader:
    """Reads the landing folder with Spark itself, as the Databricks job does."""

    def __init__(self, spark: SparkSession, landing: Path):
        self.spark, self.uri = spark, landing.resolve().as_uri()

    def manifest(self):
        return self.spark.read.schema(MANIFEST_SCHEMA).json(f"{self.uri}/manifest/manifest.jsonl")

    def source(self, source, years):
        return bronze.read_landing(self.spark, f"{self.uri}/{source.name}")

    def baseline(self, table: str):
        return self.spark.read.parquet(f"{self.uri}/baseline_gold/{table}.parquet")


class ArrowReader:
    """Reads the files with PyArrow and hands the tables to Spark.

    Spark cannot list local folders on Windows without Hadoop's native
    libraries (winutils). This reader produces the same columns as
    `bronze.read_landing` (partition columns and `_source_file` included), so
    everything downstream is identical.
    """

    def __init__(self, spark: SparkSession, landing: Path):
        self.spark, self.landing = spark, landing.resolve()

    def manifest(self):
        import json

        lines = (self.landing / "manifest" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
        fields = [f.split()[0] for f in MANIFEST_SCHEMA.split(", ")]
        rows = [tuple(json.loads(line).get(f) for f in fields) for line in lines if line.strip()]
        return self.spark.createDataFrame(rows, MANIFEST_SCHEMA)

    def source(self, source, years):
        import pyarrow as pa
        import pyarrow.parquet as pq

        root = self.landing / source.name
        tables = []
        for file in sorted(root.rglob("data.parquet")):
            partition = dict(p.split("=", 1) for p in file.relative_to(root).parts if "=" in p)
            if int(partition["ano"]) not in years:
                continue
            table = pq.ParquetFile(file).read().replace_schema_metadata(None)
            for col in source.partition_cols:
                value, dtype = (int(partition[col]), pa.int32()) if col == "ano" else (partition[col], pa.string())
                table = table.append_column(col, pa.array([value] * table.num_rows, dtype))
            table = table.append_column(
                "_source_file", pa.array([file.as_uri()] * table.num_rows, pa.string())
            )
            tables.append(table)
        return self.spark.createDataFrame(pa.concat_tables(tables, promote_options="default"))

    def baseline(self, table: str):
        import pyarrow.parquet as pq

        return self.spark.createDataFrame(pq.read_table(self.landing / "baseline_gold" / f"{table}.parquet"))


def make_reader(spark: SparkSession, landing: Path, mode: str):
    if mode == "auto":
        mode = "arrow" if os.name == "nt" and not os.environ.get("HADOOP_HOME") else "spark"
    print(f"reader: {mode}")
    return ArrowReader(spark, landing) if mode == "arrow" else SparkReader(spark, landing)


def run(spark: SparkSession, reader, years: tuple[int, ...]) -> list[parity.TableParity]:
    manifest = reader.manifest().cache()

    # ---- bronze
    gate = quality.QualityGate("bronze")
    landed = {}
    for source in SOURCES:
        source_years = years if source.incremental else WINDOW_YEARS
        df = bronze.add_bronze_metadata(
            reader.source(source, source_years), source.partition_cols
        ).where(F.col("_particao_ano").isin(list(source_years)))
        if source.name != "sim_tb_deaths":
            checks.manifest_covers_years(gate, manifest, source.name, source_years)
        checks.bronze_matches_manifest(gate, df, manifest, source.name, source_years)
        landed[source.name] = df
    _finish(gate)

    # ---- silver
    gate = quality.QualityGate("silver")
    population = silver.transform_population(landed["ibge_population"]).cache()
    checks.population(gate, landed["ibge_population"], population, WINDOW_YEARS)
    sinan_bronze = with_content_key(landed["sinan_tb"], scope_columns=["_particao_ano"])
    sinan = silver.transform_sinan(sinan_bronze, population, WINDOW_YEARS).cache()
    checks.sinan(gate, sinan_bronze, sinan, WINDOW_YEARS)
    sim_bronze = with_content_key(landed["sim_tb_deaths"], scope_columns=["_particao_uf", "_particao_ano"])
    sim = silver.transform_sim(sim_bronze).cache()
    checks.sim(gate, sim)
    dim_municipio = silver.build_dim_municipio(population, WINDOW_YEARS).cache()
    checks.dim_municipio(gate, dim_municipio)
    _finish(gate)

    # ---- gold
    gate = quality.QualityGate("gold")
    dim_uf = gold.build_dim_uf(spark)
    tables = {
        "dim_uf": dim_uf,
        "fct_incidencia_tb": gold.build_fct_incidencia(sinan, population, dim_municipio).cache(),
        "fct_taxa_abandono": gold.build_fct_taxa_abandono(sinan).cache(),
        "fct_taxa_abandono_uf": gold.build_fct_taxa_abandono_uf(sinan, dim_uf).cache(),
    }
    coverage = gold.build_sim_coverage(dim_uf, manifest.where(F.col("source") == "sim_tb_deaths"), years)
    tables["fct_reconciliacao_sinan_sim"] = gold.build_fct_reconciliacao(sinan, sim, dim_uf, coverage).cache()
    checks.gold(
        gate,
        sinan,
        dim_uf,
        tables["fct_incidencia_tb"],
        tables["fct_taxa_abandono"],
        tables["fct_taxa_abandono_uf"],
        tables["fct_reconciliacao_sinan_sim"],
        years,
    )
    _finish(gate)

    # ---- parity
    results = []
    for table, keys in parity.PARITY_KEYS.items():
        results.append(parity.compare(table, tables[table], reader.baseline(table), keys))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--landing", type=Path, default=Path("landing"))
    parser.add_argument("--years", default="all")
    parser.add_argument("--report", type=Path, default=None, help="write the Markdown report here")
    parser.add_argument(
        "--reader",
        choices=("auto", "spark", "arrow"),
        default="auto",
        help="how to read the landing files; 'auto' uses PyArrow on Windows without winutils",
    )
    args = parser.parse_args()

    years = parse_years(args.years)
    strict = years == WINDOW_YEARS
    spark = _session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        results = run(spark, make_reader(spark, args.landing, args.reader), years)
        report = parity.render_markdown(
            results,
            baseline_commit=BASELINE_COMMIT,
            years=", ".join(str(y) for y in years),
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            strict=strict,
            engine=f"local Spark {spark.version} (same transformation code as the Databricks job, no Delta)",
        )
    finally:
        spark.stop()

    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8", newline="\n")
    failed = [r.table for r in results if not r.passed]
    if failed and strict:
        print(f"PARITY FAILED: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
