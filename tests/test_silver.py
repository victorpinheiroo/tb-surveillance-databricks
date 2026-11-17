import datetime as dt

from tb_pipeline import silver

SINAN_SCHEMA = (
    "ID_MN_RESI string, NU_ANO string, SG_UF string, NDUPLIC_N string, SITUA_ENCE string, "
    "DT_NOTIFIC string, ANO_NASC string, AGRAVOUTDE string, "
    "_status_maturidade_estimado string, _particao_ano int"
)


def sinan_row(**overrides):
    row = {
        "ID_MN_RESI": "3550308",
        "NU_ANO": "2015",
        "SG_UF": "35",
        "NDUPLIC_N": "",
        "SITUA_ENCE": "1",
        "DT_NOTIFIC": "20150310",
        "ANO_NASC": "1980",
        "AGRAVOUTDE": None,
        "_status_maturidade_estimado": "fechado",
        "_particao_ano": 2015,
    }
    row.update(overrides)
    return tuple(row.values())


def sinan_df(spark, *rows):
    return spark.createDataFrame(list(rows), SINAN_SCHEMA)


def population_df(spark, rows):
    return spark.createDataFrame(
        rows,
        "ano int, codigo_municipio string, nome_municipio string, populacao bigint, "
        "status_fonte_populacao string",
    )


# ------------------------------------------------------------ population

def test_population_drops_sidra_label_row_and_types_values(spark):
    bronze = spark.createDataFrame(
        [
            ("Município (Código)", "Município", "Valor", "estimativa_direta", 2015),
            ("3550308", "São Paulo - SP", "11967825", "estimativa_direta", 2015),
            (" 1100015 ", "Alta Floresta D'Oeste - RO", "...", "estimativa_direta", 2015),
        ],
        "D1C string, D1N string, V string, _status_fonte_populacao string, _particao_ano int",
    )
    out = {r["codigo_municipio"]: r for r in silver.transform_population(bronze).collect()}

    assert set(out) == {"3550308", "1100015"}  # label row gone, code trimmed
    assert out["3550308"]["populacao"] == 11967825
    assert out["3550308"]["ano"] == 2015
    # "..." is SIDRA's "no figure available": NULL, never zero.
    assert out["1100015"]["populacao"] is None


# ----------------------------------------------------------------- SINAN

def test_decode_situa_ence_handles_zero_padding_and_sentinels(spark):
    df = sinan_df(
        spark,
        sinan_row(ID_MN_RESI="a", SITUA_ENCE="03"),
        sinan_row(ID_MN_RESI="b", SITUA_ENCE="10"),
        sinan_row(ID_MN_RESI="c", SITUA_ENCE="0"),
        sinan_row(ID_MN_RESI="d", SITUA_ENCE=""),
        sinan_row(ID_MN_RESI="e", SITUA_ENCE=None),
        sinan_row(ID_MN_RESI="f", SITUA_ENCE=" 2 "),
    )
    out = {
        r["ID_MN_RESI"]: r["situacao_encerramento_desc"] for r in silver.decode_situa_ence(df).collect()
    }
    assert out == {
        "a": "Óbito por Tuberculose",  # "03" is the same code as "3"
        "b": "Abandono Primário",  # "10" must not lose its trailing zero
        "c": None,
        "d": None,
        "e": None,
        "f": "Abandono",
    }


def test_only_source_confirmed_duplicates_are_removed(spark):
    df = sinan_df(
        spark,
        sinan_row(ID_MN_RESI="keep-empty", NDUPLIC_N=""),
        sinan_row(ID_MN_RESI="keep-null", NDUPLIC_N=None),
        sinan_row(ID_MN_RESI="keep-1", NDUPLIC_N="1"),
        sinan_row(ID_MN_RESI="drop", NDUPLIC_N=" 2 "),
    )
    kept = {r["ID_MN_RESI"] for r in silver.remove_confirmed_duplicates(df).collect()}
    assert kept == {"keep-empty", "keep-null", "keep-1"}


def test_cast_types_coerces_malformed_values_to_null(spark):
    df = sinan_df(
        spark,
        sinan_row(ID_MN_RESI="ok"),
        sinan_row(ID_MN_RESI="bad", DT_NOTIFIC="2015", ANO_NASC="abc", NU_ANO=""),
    )
    out = {r["ID_MN_RESI"]: r for r in silver.cast_sinan_types(df).collect()}
    assert out["ok"]["DT_NOTIFIC"] == dt.date(2015, 3, 10)
    assert out["ok"]["ANO_NASC"] == 1980
    assert out["ok"]["NU_ANO"] == 2015
    assert out["bad"]["DT_NOTIFIC"] is None
    assert out["bad"]["ANO_NASC"] is None
    assert out["bad"]["NU_ANO"] is None


def test_transform_sinan_end_to_end(spark):
    bronze = sinan_df(
        spark,
        sinan_row(ID_MN_RESI="3550308", NU_ANO="2015"),
        # A 2015 file also carries notifications of other years: kept when
        # inside the window, with the population of *their* year.
        sinan_row(ID_MN_RESI="3550308", NU_ANO="2016"),
        sinan_row(ID_MN_RESI="3550308", NU_ANO="1998"),  # outside the window
        sinan_row(ID_MN_RESI="3550308", NU_ANO="2015", NDUPLIC_N="2"),  # confirmed duplicate
        sinan_row(ID_MN_RESI="0", NU_ANO="2015"),  # "municipality unknown" sentinel
    )
    pop = population_df(
        spark,
        [
            (2015, "3550308", "São Paulo - SP", 100, "estimativa_direta"),
            (2016, "3550308", "São Paulo - SP", 200, "estimativa_direta"),
        ],
    )
    out = silver.transform_sinan(bronze, pop, range(2015, 2025))
    rows = {(r["ID_MN_RESI"], r["NU_ANO"]): r for r in out.collect()}

    assert set(rows) == {("3550308", 2015), ("3550308", 2016), ("0", 2015)}
    assert rows[("3550308", 2015)]["populacao"] == 100
    assert rows[("3550308", 2016)]["populacao"] == 200
    # No population match: the row survives with a NULL population.
    assert rows[("0", 2015)]["populacao"] is None
    assert "AGRAVOUTDE" not in out.columns  # legacy column dropped
    assert rows[("3550308", 2015)]["situacao_encerramento_desc"] == "Cura"


# ------------------------------------------------------------------- SIM

def test_transform_sim(spark):
    bronze = spark.createDataFrame(
        [
            ("355030", "16062022", "A162", "SP", 2022),
            ("350000", "01012022", "A150", "SP", 2022),  # UF + 0000: unknown municipality
            (None, "31022022", "A169", "SP", 2022),  # impossible date, missing code
        ],
        "CODMUNRES string, DTOBITO string, CAUSABAS string, _particao_uf string, _particao_ano int",
    )
    out = {r["CAUSABAS"]: r for r in silver.transform_sim(bronze).collect()}

    assert out["A162"]["data_obito"] == dt.date(2022, 6, 16)  # ddMMyyyy, not yyyyMMdd
    assert out["A162"]["codigo_municipio_residencia"] == "355030"  # stays at 6 digits
    assert out["A162"]["municipio_residencia_ignorado"] is False
    assert out["A150"]["municipio_residencia_ignorado"] is True
    assert out["A169"]["data_obito"] is None
    assert out["A169"]["municipio_residencia_ignorado"] is False
    assert (out["A162"]["_uf"], out["A162"]["_ano"]) == ("SP", 2022)


# --------------------------------------------------------- dim_municipio

def test_dim_municipio_versions_on_name_change_only(spark):
    pop = population_df(
        spark,
        [
            # renamed in 2017 -> two versions
            (2015, "1", "Old Name", 10, "x"),
            (2016, "1", "Old Name", 10, "x"),
            (2017, "1", "New Name", 10, "x"),
            # missing from 2016 -> coverage gap, still a single version
            (2015, "2", "Stable", 10, "x"),
            (2017, "2", "Stable", 10, "x"),
        ],
    )
    dim = silver.build_dim_municipio(pop, [2015, 2016, 2017])
    rows = sorted(dim.collect(), key=lambda r: (r["codigo_municipio"], r["valid_from"]))

    assert [(r["codigo_municipio"], r["nome_municipio"], r["valid_from"], r["valid_to"], r["is_current"]) for r in rows] == [
        ("1", "Old Name", 2015, 2016, False),
        ("1", "New Name", 2017, None, True),
        ("2", "Stable", 2015, None, True),
    ]
    assert rows[0]["anos_ausentes"] == []
    assert rows[2]["anos_ausentes"] == [2016]
