"""The concrete data checks of each layer.

Every function takes DataFrames and a `QualityGate` and records results; none
of them raises. Each check is a single small aggregation, to keep the number
of Spark actions (and the serverless quota) low.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from tb_pipeline import silver
from tb_pipeline.merge import ROW_KEY
from tb_pipeline.quality import QualityGate
from tb_pipeline.reference import SITUA_ENCE_MAP, UF_MAPPING


def _count_if(condition) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def _duplicates(df: DataFrame, columns: list[str]) -> int:
    """Number of rows beyond the first for each repeated key."""
    row = df.groupBy(*columns).count().agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0))).collect()[0]
    return int(row[0])


# ------------------------------------------------------------------ bronze

def manifest_covers_years(gate: QualityGate, manifest: DataFrame, source: str, years: Iterable[int]) -> None:
    """Every requested year must have at least one landed file for the source."""
    landed = {
        r["ano"] for r in manifest.where(F.col("source") == source).select("ano").distinct().collect()
    }
    missing = sorted({int(y) for y in years} - landed)
    gate.expect_equal(source, "landed_files_cover_requested_years", str(missing), "[]")


def bronze_matches_manifest(
    gate: QualityGate, landed: DataFrame, manifest: DataFrame, source: str, years: Iterable[int]
) -> None:
    """Row count per landed file must equal the count recorded at upload time.

    This is the hand-off contract between GitHub Actions (which uploads the
    files) and Databricks (which loads them).
    """
    has_uf = "_particao_uf" in landed.columns
    actual = landed.groupBy(
        *([F.col("_particao_uf").alias("_a_uf")] if has_uf else []), F.col("_particao_ano").alias("_a_ano")
    ).agg(F.count(F.lit(1)).alias("_actual_rows"))
    expected = manifest.where(
        (F.col("source") == source) & F.col("ano").isin([int(y) for y in years])
    ).select(F.col("uf").alias("_e_uf"), F.col("ano").alias("_e_ano"), F.col("rows").alias("_expected_rows"))
    on = F.col("_e_ano") == F.col("_a_ano")
    if has_uf:
        on = on & (F.col("_e_uf") == F.col("_a_uf"))
    mismatched = (
        expected.join(actual, on, "full_outer")
        .where(~F.col("_expected_rows").eqNullSafe(F.col("_actual_rows")))
        .count()
    )
    gate.expect_zero(source, "row_count_matches_manifest_per_file", mismatched)
    gate.metric(source, "rows_landed", landed.count())


# ------------------------------------------------------------------ silver

def population(gate: QualityGate, bronze: DataFrame, staged: DataFrame, years: Iterable[int]) -> None:
    table = "stg_ibge__populacao_municipio"
    label_rows = bronze.where(~silver.is_sidra_data_row()).count()
    # SIDRA returns exactly one label row per yearly request. Any other number
    # means the API response format changed.
    gate.expect_equal(table, "one_sidra_label_row_per_year", label_rows, len(list(years)))
    gate.expect_zero(table, "unique_ano_codigo_municipio", _duplicates(staged, ["ano", "codigo_municipio"]))
    row = staged.agg(
        F.count(F.lit(1)).alias("n"), _count_if(F.col("populacao").isNull()).alias("sem_populacao")
    ).collect()[0]
    gate.expect_true(table, "not_empty", row["n"] > 0, row["n"], "> 0")
    gate.metric(table, "rows_with_null_population", row["sem_populacao"])


def dim_municipio(gate: QualityGate, dim: DataFrame) -> None:
    table = "dim_municipio"
    row = dim.agg(
        F.countDistinct("codigo_municipio").alias("codes"),
        _count_if(F.col("is_current")).alias("current"),
        F.count(F.lit(1)).alias("versions"),
    ).collect()[0]
    gate.expect_equal(table, "exactly_one_current_version_per_code", row["current"], row["codes"])
    gate.metric(table, "attribute_changes_detected", row["versions"] - row["codes"])


def sinan(gate: QualityGate, bronze: DataFrame, staged: DataFrame, window_years: Iterable[int]) -> None:
    table = "stg_sinan__tuberculose"
    window = [int(y) for y in window_years]
    known = list(SITUA_ENCE_MAP)
    code = silver.normalized_situa_ence()
    row = staged.agg(
        F.count(F.lit(1)).alias("n"),
        _count_if(silver.is_confirmed_duplicate()).alias("confirmed_duplicates"),
        _count_if(F.col("NU_ANO").isNull() | ~F.col("NU_ANO").isin(window)).alias("outside_window"),
        # '' and '0' are the documented "outcome not informed" values; anything
        # else that fails to decode is a new code the dictionary does not know.
        _count_if(F.col("SITUA_ENCE").isNotNull() & (code != "") & ~code.isin(known)).alias("unmapped"),
        _count_if(F.col("ID_MN_RESI").isNull()).alias("null_municipio"),
    ).collect()[0]
    gate.expect_true(table, "not_empty", row["n"] > 0, row["n"], "> 0")
    gate.expect_zero(table, "no_source_confirmed_duplicates", row["confirmed_duplicates"])
    gate.expect_zero(table, "nu_ano_inside_window", row["outside_window"])
    gate.expect_zero(table, "no_unmapped_situa_ence_codes", row["unmapped"])
    gate.expect_zero(table, "id_mn_resi_not_null", row["null_municipio"])
    gate.expect_zero(table, "row_key_unique", _duplicates(staged, [ROW_KEY]))
    gate.metric(table, "rows_removed_duplicates_and_out_of_window", bronze.count() - row["n"])


def sim(gate: QualityGate, staged: DataFrame) -> None:
    table = "stg_sim__obitos_tb"
    row = staged.agg(
        F.count(F.lit(1)).alias("n"),
        _count_if(F.col("data_obito").isNull()).alias("bad_dates"),
        _count_if(~F.coalesce(F.col("codigo_municipio_residencia").rlike(r"^\d{6}$"), F.lit(False))).alias(
            "bad_codes"
        ),
        _count_if(F.col("municipio_residencia_ignorado")).alias("ignorado"),
        _count_if(~F.coalesce(F.col("CAUSABAS").rlike(r"^A1[5-9]"), F.lit(False))).alias("not_tb"),
    ).collect()[0]
    gate.expect_true(table, "not_empty", row["n"] > 0, row["n"], "> 0")
    gate.expect_zero(table, "dtobito_parses_as_ddMMyyyy", row["bad_dates"])
    gate.expect_zero(table, "codmunres_has_six_digits", row["bad_codes"])
    gate.expect_zero(table, "causabas_is_icd10_a15_a19", row["not_tb"])
    gate.expect_zero(table, "row_key_unique", _duplicates(staged, [ROW_KEY]))
    gate.metric(table, "municipio_residencia_ignorado", row["ignorado"])


# -------------------------------------------------------------------- gold

def gold(
    gate: QualityGate,
    sinan_silver: DataFrame,
    dim_uf: DataFrame,
    incidencia: DataFrame,
    abandono: DataFrame,
    abandono_uf: DataFrame,
    reconciliacao: DataFrame,
    coverage_years: Iterable[int],
) -> None:
    n_ufs = len(UF_MAPPING)
    gate.expect_equal("dim_uf", "has_27_states", dim_uf.count(), n_ufs)

    table = "fct_incidencia_tb"
    row = incidencia.agg(
        F.coalesce(F.sum("casos"), F.lit(0)).alias("casos"),
        _count_if(F.col("incidencia_por_100k") < 0).alias("negative"),
        _count_if(F.col("populacao").isNull() != F.col("incidencia_por_100k").isNull()).alias("null_mismatch"),
    ).collect()[0]
    # Conservation: aggregating must neither lose nor invent notifications.
    gate.expect_equal(table, "sum_casos_equals_silver_rows", int(row["casos"]), sinan_silver.count())
    gate.expect_zero(table, "incidencia_not_negative", row["negative"])
    gate.expect_zero(table, "incidencia_null_iff_population_null", row["null_mismatch"])
    gate.expect_zero(table, "unique_municipio_ano", _duplicates(incidencia, ["ID_MN_RESI", "NU_ANO"]))

    table = "fct_taxa_abandono"
    row = abandono.agg(
        _count_if((F.col("taxa_abandono_pct") < 0) | (F.col("taxa_abandono_pct") > 100)).alias("out_of_range"),
        _count_if(F.col("n_abandono") > F.col("n_casos_encerrados")).alias("numerator_gt_denominator"),
    ).collect()[0]
    gate.expect_zero(table, "taxa_between_0_and_100", row["out_of_range"])
    gate.expect_zero(table, "n_abandono_lte_n_casos_encerrados", row["numerator_gt_denominator"])
    gate.expect_zero(table, "unique_municipio_ano", _duplicates(abandono, ["ID_MN_RESI", "NU_ANO"]))

    table = "fct_taxa_abandono_uf"
    row = abandono_uf.agg(
        _count_if((F.col("taxa_abandono_pct") < 0) | (F.col("taxa_abandono_pct") > 100)).alias("out_of_range"),
        _count_if(F.col("uf_sigla").isNull()).alias("unknown_uf"),
    ).collect()[0]
    gate.expect_zero(table, "taxa_between_0_and_100", row["out_of_range"])
    gate.expect_zero(table, "no_sentinel_uf", row["unknown_uf"])
    gate.expect_zero(table, "unique_uf_ano", _duplicates(abandono_uf, ["uf_codigo", "ano"]))

    table = "fct_reconciliacao_sinan_sim"
    ausente = F.col("status_cobertura_sim") == "ausente"
    row = reconciliacao.agg(
        F.count(F.lit(1)).alias("n"),
        _count_if(ausente != F.col("diferenca_sim_menos_sinan").isNull()).alias("null_rule_broken"),
        _count_if(ausente & F.col("obitos_sim").isNotNull()).alias("deaths_without_coverage"),
    ).collect()[0]
    gate.expect_equal(table, "one_row_per_uf_and_year", row["n"], n_ufs * len(list(coverage_years)))
    gate.expect_zero(table, "diferenca_null_iff_sim_ausente", row["null_rule_broken"])
    gate.expect_zero(table, "no_sim_deaths_where_coverage_ausente", row["deaths_without_coverage"])
