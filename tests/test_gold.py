import pytest

from tb_pipeline import gold

SINAN_SCHEMA = (
    "ID_MN_RESI string, NU_ANO int, SG_UF string, situacao_encerramento_desc string, "
    "_status_maturidade_estimado string, _particao_ano int"
)


def sinan_df(spark, rows):
    return spark.createDataFrame(rows, SINAN_SCHEMA)


def by_key(df, *keys):
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r for r in df.collect()}


def test_dim_uf_has_27_unique_states(spark):
    rows = gold.build_dim_uf(spark).collect()
    assert len(rows) == 27
    assert len({r["uf_codigo"] for r in rows}) == 27
    assert len({r["uf_sigla"] for r in rows}) == 27
    assert {r["uf_codigo"]: r["uf_sigla"] for r in rows}["35"] == "SP"


def test_incidencia(spark):
    sinan = sinan_df(
        spark,
        [
            ("3550308", 2024, "35", "Cura", "provisorio", 2024),
            ("3550308", 2024, "35", None, "provisorio", 2024),
            # Same notification year, but landed in the older (already closed)
            # 2023 file: the group takes the status of the oldest file.
            ("3550308", 2024, "35", "Cura", "fechado", 2023),
            ("0", 2024, "0", None, "provisorio", 2024),  # unknown municipality
        ],
    )
    population = spark.createDataFrame(
        [(2024, "3550308", "São Paulo - SP", 1_000_000, "estimativa_direta")],
        "ano int, codigo_municipio string, nome_municipio string, populacao bigint, "
        "status_fonte_populacao string",
    )
    dim_municipio = spark.createDataFrame(
        [
            ("3550308", "Old Name", 2015, 2019, False),
            ("3550308", "São Paulo - SP", 2020, None, True),
        ],
        "codigo_municipio string, nome_municipio string, valid_from int, valid_to int, is_current boolean",
    )
    out = by_key(gold.build_fct_incidencia(sinan, population, dim_municipio), "ID_MN_RESI")

    assert len(out) == 2  # two SCD2 versions must not duplicate the fact row
    sp = out["3550308"]
    assert sp["casos"] == 3
    assert sp["incidencia_por_100k"] == pytest.approx(0.3)
    assert sp["status_maturidade"] == "fechado"
    assert sp["nome_municipio"] == "São Paulo - SP"
    # No population: incidence is NULL, never zero, and the row is kept.
    assert out["0"]["casos"] == 1
    assert out["0"]["populacao"] is None
    assert out["0"]["incidencia_por_100k"] is None
    assert out["0"]["nome_municipio"] is None


def test_taxa_abandono_uses_closed_cases_as_denominator(spark):
    sinan = sinan_df(
        spark,
        [
            ("3550308", 2020, "35", "Cura", "fechado", 2020),
            ("3550308", 2020, "35", "Abandono", "fechado", 2020),
            ("3550308", 2020, "35", "Abandono Primário", "fechado", 2020),
            ("3550308", 2020, "35", "Óbito por Tuberculose", "fechado", 2020),
            ("3550308", 2020, "35", None, "fechado", 2020),  # no outcome yet: not in the denominator
            ("1100015", 2020, "11", None, "fechado", 2020),  # no closed case at all: no row
        ],
    )
    out = by_key(gold.build_fct_taxa_abandono(sinan), "ID_MN_RESI")

    assert set(out) == {"3550308"}
    assert out["3550308"]["n_casos_encerrados"] == 4
    assert out["3550308"]["n_abandono"] == 2  # Abandono + Abandono Primário
    assert out["3550308"]["taxa_abandono_pct"] == pytest.approx(50.0)


def test_taxa_abandono_uf_excludes_sentinel_state(spark):
    sinan = sinan_df(
        spark,
        [
            ("3550308", 2020, "35", "Cura", "fechado", 2020),
            ("3509502", 2020, "35", "Abandono", "fechado", 2020),
            ("0", 2020, "0", "Abandono", "fechado", 2020),  # SG_UF '0' is not a state
        ],
    )
    out = gold.build_fct_taxa_abandono_uf(sinan, gold.build_dim_uf(spark)).collect()

    assert len(out) == 1
    assert (out[0]["uf_sigla"], out[0]["uf_codigo"], out[0]["ano"]) == ("SP", "35", 2020)
    assert out[0]["n_casos_encerrados"] == 2
    assert out[0]["taxa_abandono_pct"] == pytest.approx(50.0)


def test_sim_coverage_is_decided_by_landed_files(spark):
    dim_uf = gold.build_dim_uf(spark)
    files = spark.createDataFrame([("SP", 2020), ("SP", 2021), ("AC", 2020)], "uf string, ano int")
    out = by_key(gold.build_sim_coverage(dim_uf, files, [2020, 2021]), "uf_sigla", "ano")

    assert len(out) == 27 * 2
    assert out[("SP", 2020)]["status_cobertura_sim"] == "disponivel"
    assert out[("AC", 2020)]["status_cobertura_sim"] == "disponivel"
    assert out[("AC", 2021)]["status_cobertura_sim"] == "ausente"
    assert out[("RJ", 2020)]["status_cobertura_sim"] == "ausente"


def test_reconciliacao_keeps_missing_coverage_distinct_from_zero(spark):
    dim_uf = gold.build_dim_uf(spark).where("uf_sigla IN ('SP', 'AC', 'RJ')")
    sinan = sinan_df(
        spark,
        [
            ("3550308", 2024, "35", "Óbito por Tuberculose", "provisorio", 2024),
            ("3550308", 2024, "35", "Óbito por Tuberculose", "provisorio", 2024),
            ("3550308", 2024, "35", "Cura", "provisorio", 2024),  # not a TB death
            ("1200401", 2024, "12", "Óbito por Tuberculose", "provisorio", 2024),
            ("0", 2024, "0", "Óbito por Tuberculose", "provisorio", 2024),  # sentinel state
        ],
    )
    sim = spark.createDataFrame([("SP", 2024)] * 5, "_uf string, _ano int")
    files = spark.createDataFrame([("SP", 2024), ("RJ", 2024)], "uf string, ano int")
    coverage = gold.build_sim_coverage(dim_uf, files, [2024])
    out = by_key(gold.build_fct_reconciliacao(sinan, sim, dim_uf, coverage), "uf_sigla")

    assert set(out) == {"SP", "AC", "RJ"}
    assert out["SP"]["obitos_sim"] == 5.0
    assert out["SP"]["obitos_sinan"] == 2
    assert out["SP"]["diferenca_sim_menos_sinan"] == 3.0
    assert out["SP"]["status_maturidade_sim"] == "provisorio"
    # No SIM file for AC: the difference is NULL, not "0 - 1".
    assert out["AC"]["status_cobertura_sim"] == "ausente"
    assert out["AC"]["obitos_sim"] is None
    assert out["AC"]["obitos_sinan"] == 1
    assert out["AC"]["diferenca_sim_menos_sinan"] is None
    # RJ has a file but no SINAN deaths: SINAN side is a real zero.
    assert out["RJ"]["status_cobertura_sim"] == "disponivel"
    assert out["RJ"]["obitos_sinan"] == 0


def test_reconciliacao_marks_years_before_2024_as_closed(spark):
    dim_uf = gold.build_dim_uf(spark).where("uf_sigla = 'SP'")
    sinan = sinan_df(spark, [("3550308", 2023, "35", "Óbito por Tuberculose", "fechado", 2023)])
    sim = spark.createDataFrame([("SP", 2023)], "_uf string, _ano int")
    files = spark.createDataFrame([("SP", 2023)], "uf string, ano int")
    coverage = gold.build_sim_coverage(dim_uf, files, [2023])
    out = gold.build_fct_reconciliacao(sinan, sim, dim_uf, coverage).collect()

    assert [r["status_maturidade_sim"] for r in out] == ["fechado"]
