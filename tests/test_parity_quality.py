import pytest

from tb_pipeline import checks, parity
from tb_pipeline.quality import QualityGate, QualityGateError

SCHEMA = "ID_MN_RESI string, NU_ANO bigint, casos bigint, taxa double, nome string"


def table(spark, rows, schema=SCHEMA):
    return spark.createDataFrame(rows, schema)


BASE = [
    ("3550308", 2020, 10, 12.5, "São Paulo"),
    ("1100015", 2020, 1, None, None),
]


# ------------------------------------------------------------------ parity

def test_parity_passes_on_identical_tables_with_different_integer_width(spark):
    new = table(spark, BASE, "ID_MN_RESI string, NU_ANO int, casos bigint, taxa double, nome string")
    baseline = table(spark, BASE).withColumn("__index_level_0__", parity.F.lit(1))
    result = parity.compare("t", new, baseline, ["ID_MN_RESI", "NU_ANO"])

    assert result.passed
    assert (result.rows_new, result.rows_baseline, result.matched) == (2, 2, 2)
    assert result.column_mismatches == {"casos": 0, "taxa": 0, "nome": 0}  # pandas index ignored
    assert result.sums["casos"] == (11.0, 11.0)


def test_parity_reports_changed_cells_and_missing_rows(spark):
    new = table(
        spark,
        [
            ("3550308", 2020, 11, 12.5, "São Paulo"),  # casos differs
            ("9999999", 2020, 1, 1.0, "Only in new"),
        ],
    )
    result = parity.compare("t", new, table(spark, BASE), ["ID_MN_RESI", "NU_ANO"])

    assert not result.passed
    assert result.only_in_new == 1
    assert result.only_in_baseline == 1
    assert result.matched == 1
    assert result.column_mismatches == {"casos": 1, "taxa": 0, "nome": 0}


def test_parity_float_tolerance_and_null_handling(spark):
    baseline = table(spark, [("a", 1, 1, 0.1 + 0.2, "x"), ("b", 1, 1, None, "x"), ("c", 1, 1, float("nan"), "x")])
    new = table(spark, [("a", 1, 1, 0.3, "x"), ("b", 1, 1, 0.0, "x"), ("c", 1, 1, None, "x")])
    result = parity.compare("t", new, baseline, ["ID_MN_RESI", "NU_ANO"])

    # 0.1 + 0.2 vs 0.3 is float noise; NaN in the baseline equals NULL;
    # NULL vs 0.0 is a real difference.
    assert result.column_mismatches["taxa"] == 1


def test_parity_rejects_baseline_columns_missing_from_new(spark):
    new = table(spark, BASE).drop("nome")
    with pytest.raises(ValueError, match="nome"):
        parity.compare("t", new, table(spark, BASE), ["ID_MN_RESI", "NU_ANO"])


def test_render_markdown(spark):
    result = parity.compare("fct_x", table(spark, BASE), table(spark, BASE), ["ID_MN_RESI", "NU_ANO"])
    report = parity.render_markdown(
        [result],
        baseline_commit="cdfcd297fed9",
        years="2015, 2016",
        generated_at="2026-11-16 12:00",
        strict=True,
        engine="test",
    )
    assert "**PASS**" in report
    assert "`cdfcd29`" in report
    assert "| `fct_x` | ID_MN_RESI, NU_ANO | 2 | 2 | 0 | 0 | 3 | 0 | PASS |" in report


# ----------------------------------------------------------------- quality

def test_quality_gate_raises_only_on_blocking_failures():
    gate = QualityGate("silver")
    gate.expect_zero("t", "no_duplicates", 0)
    gate.metric("t", "rows_dropped", 123)
    gate.raise_if_failed()  # nothing failed

    gate.expect_equal("t", "has_27_states", 26, 27)
    assert [r.check for r in gate.failures] == ["has_27_states"]
    with pytest.raises(QualityGateError, match="has_27_states"):
        gate.raise_if_failed()
    assert "[FAIL] t.has_27_states" in gate.summary()


def test_bronze_check_detects_row_count_mismatch_and_missing_file(spark):
    manifest = spark.createDataFrame(
        [("sinan_tb", None, 2015, 2), ("sinan_tb", None, 2016, 1), ("sim_tb_deaths", "SP", 2015, 9)],
        "source string, uf string, ano int, rows bigint",
    )
    landed_ok = spark.createDataFrame([(2015,), (2015,), (2016,)], "_particao_ano int")
    landed_short = spark.createDataFrame([(2015,), (2016,)], "_particao_ano int")

    ok = QualityGate("bronze")
    checks.bronze_matches_manifest(ok, landed_ok, manifest, "sinan_tb", [2015, 2016])
    checks.manifest_covers_years(ok, manifest, "sinan_tb", [2015, 2016])
    assert ok.failures == []

    bad = QualityGate("bronze")
    checks.bronze_matches_manifest(bad, landed_short, manifest, "sinan_tb", [2015, 2016])
    checks.manifest_covers_years(bad, manifest, "sinan_tb", [2015, 2016, 2017])
    assert [r.check for r in bad.failures] == [
        "row_count_matches_manifest_per_file",
        "landed_files_cover_requested_years",
    ]


def test_population_check_fails_on_duplicate_key(spark):
    bronze = spark.createDataFrame(
        [("Município (Código)", 2015), ("3550308", 2015), ("3550308", 2015)], "D1C string, _particao_ano int"
    )
    staged = spark.createDataFrame(
        [(2015, "3550308", 10), (2015, "3550308", 10)], "ano int, codigo_municipio string, populacao bigint"
    )
    gate = QualityGate("silver")
    checks.population(gate, bronze, staged, [2015])
    assert [r.check for r in gate.failures] == ["unique_ano_codigo_municipio"]
