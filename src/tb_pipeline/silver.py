"""Bronze -> silver transformations, as pure DataFrame -> DataFrame functions.

Each function ports one step of the original pandas pipeline. Everything is
written for ANSI mode (the default on Databricks serverless and Spark 4):
malformed values become NULL through `try_*` functions instead of raising,
which is the Spark equivalent of pandas `errors="coerce"`.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from tb_pipeline.reference import SINAN_DATE_COLUMNS, SINAN_LEGACY_COLUMNS, SITUA_ENCE_MAP


def _try_int(col: str) -> Column:
    return F.expr(f"try_cast(try_cast(trim(`{col}`) AS DOUBLE) AS INT)")


def _try_bigint(col: str) -> Column:
    return F.expr(f"try_cast(try_cast(trim(`{col}`) AS DOUBLE) AS BIGINT)")


def _try_date(col: str, fmt: str) -> Column:
    return F.expr(f"CAST(try_to_timestamp(trim(`{col}`), '{fmt}') AS DATE)")


# --------------------------------------------------------------------------
# IBGE population
# --------------------------------------------------------------------------

def is_sidra_data_row() -> Column:
    """SIDRA returns one label row per request ("Município (Código)") mixed
    with the data; real rows have a numeric municipality code."""
    return F.expr("try_cast(trim(D1C) AS DOUBLE) IS NOT NULL")


def transform_population(bronze: DataFrame) -> DataFrame:
    """stg_ibge__populacao_municipio: one row per (ano, codigo_municipio).

    `populacao` is NULL where SIDRA publishes "..." (no figure available);
    `status_fonte_populacao` carries the 2022/2023 census-proxy flag (ADR-003).
    """
    return bronze.where(is_sidra_data_row()).select(
        F.col("_particao_ano").cast("int").alias("ano"),
        F.trim(F.col("D1C")).alias("codigo_municipio"),
        F.col("D1N").alias("nome_municipio"),
        _try_bigint("V").alias("populacao"),
        F.col("_status_fonte_populacao").alias("status_fonte_populacao"),
    )


def build_dim_municipio(population: DataFrame, years: Iterable[int]) -> DataFrame:
    """SCD Type 2 municipality dimension.

    A new version is opened only when `nome_municipio` changes between years
    (attribute change). A municipality missing from a year is a coverage gap of
    the source, recorded in `anos_ausentes`, and never opens a new version.
    """
    base = population.select("codigo_municipio", "ano", "nome_municipio").dropDuplicates(
        ["codigo_municipio", "ano"]
    )
    by_code = Window.partitionBy("codigo_municipio").orderBy("ano")
    flagged = (
        base.withColumn("_prev_nome", F.lag("nome_municipio").over(by_code))
        .withColumn("_rn", F.row_number().over(by_code))
        .withColumn(
            "_changed",
            F.when(
                (F.col("_rn") == 1) | ~F.col("nome_municipio").eqNullSafe(F.col("_prev_nome")), 1
            ).otherwise(0),
        )
        .withColumn("_version", F.sum("_changed").over(by_code))
    )
    versions = flagged.groupBy("codigo_municipio", "_version").agg(
        F.first("nome_municipio").alias("nome_municipio"),
        F.min("ano").alias("valid_from"),
    )
    by_version = Window.partitionBy("codigo_municipio").orderBy("valid_from")
    versions = versions.withColumn(
        "valid_to", (F.lead("valid_from").over(by_version) - 1).cast("int")
    ).withColumn("is_current", F.col("valid_to").isNull())

    expected_years = F.array(*[F.lit(int(y)) for y in sorted(years)])
    gaps = base.groupBy(F.col("codigo_municipio").alias("_gap_codigo")).agg(
        F.array_sort(F.array_except(expected_years, F.collect_set("ano"))).alias("anos_ausentes")
    )
    return (
        versions.join(gaps, F.col("codigo_municipio") == F.col("_gap_codigo"), "left")
        .select(
            "codigo_municipio",
            "nome_municipio",
            F.col("valid_from").cast("int").alias("valid_from"),
            "valid_to",
            "is_current",
            "anos_ausentes",
        )
    )


# --------------------------------------------------------------------------
# SINAN-TB
# --------------------------------------------------------------------------

def drop_legacy_columns(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in SINAN_LEGACY_COLUMNS if c in df.columns])


def cast_sinan_types(df: DataFrame) -> DataFrame:
    casts = {c: _try_date(c, "yyyyMMdd") for c in SINAN_DATE_COLUMNS if c in df.columns}
    for col in ("ANO_NASC", "NU_ANO"):
        if col in df.columns:
            casts[col] = _try_int(col)
    return df.withColumns(casts)


def normalized_situa_ence() -> Column:
    """Outcome code without zero padding: "03" (seen only in 2018) is "3"."""
    return F.regexp_replace(F.trim(F.col("SITUA_ENCE")), "^0+", "")


def decode_situa_ence(df: DataFrame) -> DataFrame:
    code = normalized_situa_ence()
    decoded = None
    for key, label in SITUA_ENCE_MAP.items():
        decoded = F.when(code == key, label) if decoded is None else decoded.when(code == key, label)
    return df.withColumn("situacao_encerramento_desc", decoded)


def is_confirmed_duplicate() -> Column:
    """`NDUPLIC_N == '2'` is the only duplicate flag asserted by the source
    itself (ADR-002). NULL-safe: a missing flag is not a duplicate."""
    return F.coalesce(F.trim(F.col("NDUPLIC_N")), F.lit("")) == "2"


def remove_confirmed_duplicates(df: DataFrame) -> DataFrame:
    return df.where(~is_confirmed_duplicate())


def join_population(df: DataFrame, population: DataFrame) -> DataFrame:
    """Left join: a notification with no matching population keeps a NULL
    population and is never dropped (e.g. Boa Esperança do Norte, ADR-003)."""
    lookup = population.select(
        F.col("codigo_municipio").alias("_pop_codigo"),
        F.col("ano").alias("_pop_ano"),
        "populacao",
        "status_fonte_populacao",
    )
    return df.join(
        lookup,
        (F.col("ID_MN_RESI") == F.col("_pop_codigo")) & (F.col("NU_ANO") == F.col("_pop_ano")),
        "left",
    ).drop("_pop_codigo", "_pop_ano")


def filter_notification_window(df: DataFrame, window_years: Iterable[int]) -> DataFrame:
    """Keep notifications whose `NU_ANO` is inside the validated window.

    A yearly SINAN file is not a clean year: the 2015 file carries rows with
    `NU_ANO` from 1987 to 2022. The original pipeline partitions silver by
    `NU_ANO` and silently drops what falls outside the window; here it is an
    explicit, tested step.
    """
    return df.where(F.col("NU_ANO").isin([int(y) for y in window_years]))


def transform_sinan(bronze: DataFrame, population: DataFrame, window_years: Iterable[int]) -> DataFrame:
    """stg_sinan__tuberculose, same step order as the original pipeline."""
    df = drop_legacy_columns(bronze)
    df = cast_sinan_types(df)
    df = decode_situa_ence(df)
    df = remove_confirmed_duplicates(df)
    df = join_population(df, population)
    return filter_notification_window(df, window_years)


# --------------------------------------------------------------------------
# SIM (TB deaths)
# --------------------------------------------------------------------------

def transform_sim(bronze: DataFrame) -> DataFrame:
    """stg_sim__obitos_tb.

    `CODMUNRES` stays at 6 digits (DATASUS convention, no check digit); joins
    must truncate the 7-digit IBGE side. `UF + 0000` is the SIM sentinel for
    "municipality unknown". `DTOBITO` is ddMMyyyy, unlike SINAN's yyyyMMdd.
    """
    code = F.trim(F.col("CODMUNRES"))
    return (
        bronze.withColumn("_ano", F.col("_particao_ano").cast("int"))
        .withColumn("_uf", F.col("_particao_uf"))
        .withColumn("codigo_municipio_residencia", code)
        .withColumn(
            "municipio_residencia_ignorado", F.coalesce(code.rlike(r"^\d{2}0000$"), F.lit(False))
        )
        .withColumn("data_obito", _try_date("DTOBITO", "ddMMyyyy"))
    )
