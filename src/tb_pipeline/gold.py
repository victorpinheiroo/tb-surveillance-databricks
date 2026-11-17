"""Silver -> gold aggregates. Same tables and column names as the original.

Two places where pandas behaviour had to be made explicit in Spark:

* `groupby(...).agg(first)` on `status_maturidade` depends on row order in
  pandas. Rows are ordered by source file, so "first" means "the value from
  the oldest yearly file that contributed to the group". Spark has no row
  order, so this is written as `min_by(status, _particao_ano)`.
* pandas `groupby` silently drops NULL keys; here it is an explicit filter.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from tb_pipeline.reference import (
    ABANDONO_CATEGORIAS,
    OBITO_TB,
    SIM_PROVISIONAL_FROM_YEAR,
    UF_MAPPING,
)


def build_dim_uf(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(UF_MAPPING, "uf_codigo string, uf_sigla string, uf_nome string")


def _status_maturidade() -> Column:
    return F.min_by("_status_maturidade_estimado", "_particao_ano").alias("status_maturidade")


def _is_abandono() -> Column:
    return F.col("situacao_encerramento_desc").isin(list(ABANDONO_CATEGORIAS))


def build_fct_incidencia(sinan: DataFrame, population: DataFrame, dim_municipio: DataFrame) -> DataFrame:
    """TB incidence per 100k inhabitants by municipality x year.

    Numerator: every notification of the year. Missing population yields a
    NULL incidence, never zero.
    """
    agg = (
        sinan.where(F.col("ID_MN_RESI").isNotNull() & F.col("NU_ANO").isNotNull())
        .groupBy("ID_MN_RESI", "NU_ANO")
        .agg(F.count(F.lit(1)).alias("casos"), _status_maturidade())
    )
    pop = population.select(
        F.col("codigo_municipio").alias("_pop_codigo"),
        F.col("ano").alias("_pop_ano"),
        "populacao",
        "status_fonte_populacao",
    )
    # Only the current SCD2 version: with more than one version per code a
    # plain join would fan out the fact (latent in the original, where every
    # municipality still has a single version).
    nomes = dim_municipio.where(F.col("is_current")).select(
        F.col("codigo_municipio").alias("_dim_codigo"), "nome_municipio"
    )
    return (
        agg.join(
            pop,
            (F.col("ID_MN_RESI") == F.col("_pop_codigo")) & (F.col("NU_ANO") == F.col("_pop_ano")),
            "left",
        )
        .withColumn("incidencia_por_100k", F.expr("try_divide(casos, populacao) * 100000"))
        .join(nomes, F.col("ID_MN_RESI") == F.col("_dim_codigo"), "left")
        .select(
            "ID_MN_RESI",
            "NU_ANO",
            "casos",
            "status_maturidade",
            "populacao",
            "status_fonte_populacao",
            "incidencia_por_100k",
            "nome_municipio",
        )
    )


def _encerrados(sinan: DataFrame) -> DataFrame:
    """Denominator of the abandonment rate: cases with a known outcome."""
    return sinan.where(F.col("situacao_encerramento_desc").isNotNull())


def build_fct_taxa_abandono(sinan: DataFrame) -> DataFrame:
    """Treatment abandonment rate by municipality x year."""
    return (
        _encerrados(sinan)
        .where(F.col("ID_MN_RESI").isNotNull() & F.col("NU_ANO").isNotNull())
        .groupBy("ID_MN_RESI", "NU_ANO")
        .agg(
            F.count(F.lit(1)).alias("n_casos_encerrados"),
            F.sum(F.when(_is_abandono(), 1).otherwise(0)).alias("n_abandono"),
            _status_maturidade(),
        )
        .withColumn("taxa_abandono_pct", (F.col("n_abandono") / F.col("n_casos_encerrados")) * 100)
    )


def build_fct_taxa_abandono_uf(sinan: DataFrame, dim_uf: DataFrame) -> DataFrame:
    """State-level rollup (the recommended view). The inner join with `dim_uf`
    removes the `SG_UF = '0'` sentinel, which is not a real state."""
    ufs = dim_uf.select(F.col("uf_codigo"), F.col("uf_sigla"))
    return (
        _encerrados(sinan)
        .where(F.col("NU_ANO").isNotNull())
        .join(ufs, F.col("SG_UF") == F.col("uf_codigo"), "inner")
        .groupBy(F.col("NU_ANO").alias("ano"), "uf_codigo", "uf_sigla")
        .agg(
            F.count(F.lit(1)).alias("n_casos_encerrados"),
            F.sum(F.when(_is_abandono(), 1).otherwise(0)).alias("n_abandono"),
            _status_maturidade(),
        )
        .withColumn("taxa_abandono_pct", (F.col("n_abandono") / F.col("n_casos_encerrados")) * 100)
        .select(
            "ano",
            "n_casos_encerrados",
            "n_abandono",
            "status_maturidade",
            "taxa_abandono_pct",
            "uf_codigo",
            "uf_sigla",
        )
    )


def build_sim_coverage(dim_uf: DataFrame, sim_files: DataFrame, years: Iterable[int]) -> DataFrame:
    """One row per state x year saying whether a SIM file was landed.

    `sim_files` has one row per landed file (`uf`, `ano`), taken from the
    landing manifest. Coverage is decided by file presence, not by the absence
    of rows after aggregation: zero deaths and no data are different things.
    """
    landed = sim_files.select(
        F.col("uf").alias("_file_uf"), F.col("ano").cast("int").alias("_file_ano")
    ).distinct()
    grid = dim_uf.select(
        "uf_sigla", F.explode(F.array(*[F.lit(int(y)) for y in sorted(years)])).alias("ano")
    )
    return grid.join(
        landed, (F.col("uf_sigla") == F.col("_file_uf")) & (F.col("ano") == F.col("_file_ano")), "left"
    ).select(
        "uf_sigla",
        "ano",
        F.when(F.col("_file_uf").isNotNull(), "disponivel").otherwise("ausente").alias(
            "status_cobertura_sim"
        ),
    )


def build_fct_reconciliacao(
    sinan: DataFrame, sim: DataFrame, dim_uf: DataFrame, coverage: DataFrame
) -> DataFrame:
    """TB deaths: SINAN (notified outcome) vs SIM (registered death), by state x year.

    The gap between the two counts is the finding, not an error. Where SIM has
    no coverage the difference is NULL, never zero.
    """
    ufs = dim_uf.select(F.col("uf_codigo").alias("_uf_codigo"), F.col("uf_sigla").alias("_uf_sigla"))
    sinan_agg = (
        sinan.where(F.col("situacao_encerramento_desc") == OBITO_TB)
        .join(ufs, F.col("SG_UF") == F.col("_uf_codigo"), "inner")
        .groupBy(F.col("_uf_sigla").alias("_sinan_uf"), F.col("NU_ANO").alias("_sinan_ano"))
        .agg(F.count(F.lit(1)).alias("_obitos_sinan"))
    )
    sim_agg = sim.groupBy(F.col("_uf").alias("_sim_uf"), F.col("_ano").alias("_sim_ano")).agg(
        F.count(F.lit(1)).cast("double").alias("obitos_sim")
    )
    disponivel = F.col("status_cobertura_sim") == "disponivel"
    return (
        coverage.join(
            sim_agg, (F.col("uf_sigla") == F.col("_sim_uf")) & (F.col("ano") == F.col("_sim_ano")), "left"
        )
        .join(
            sinan_agg,
            (F.col("uf_sigla") == F.col("_sinan_uf")) & (F.col("ano") == F.col("_sinan_ano")),
            "left",
        )
        .withColumn("obitos_sinan", F.coalesce(F.col("_obitos_sinan"), F.lit(0)).cast("bigint"))
        .withColumn(
            "diferenca_sim_menos_sinan",
            F.when(disponivel, F.col("obitos_sim") - F.col("obitos_sinan")),
        )
        .withColumn(
            "status_maturidade_sim",
            F.when(F.col("ano") >= SIM_PROVISIONAL_FROM_YEAR, "provisorio").otherwise("fechado"),
        )
        .select(
            "uf_sigla",
            "ano",
            "status_cobertura_sim",
            "obitos_sim",
            "obitos_sinan",
            "diferenca_sim_menos_sinan",
            "status_maturidade_sim",
        )
    )
