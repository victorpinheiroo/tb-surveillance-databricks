import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import build_landing
import publish_parity
from tb_pipeline import bronze
from tb_pipeline.config import WINDOW_YEARS, PipelineConfig, parse_years

# ------------------------------------------------------------------ config

@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2015", (2015,)),
        ("2015, 2017", (2015, 2017)),
        ("2015-2017", (2015, 2016, 2017)),
        ("2020,2015-2016", (2015, 2016, 2020)),
        ("all", WINDOW_YEARS),
        ("", WINDOW_YEARS),
    ],
)
def test_parse_years(text, expected):
    assert parse_years(text) == expected


@pytest.mark.parametrize("text", ["2014", "2025", "2015-2030", "2017-2015"])
def test_parse_years_rejects_years_outside_the_validated_window(text):
    with pytest.raises(ValueError):
        parse_years(text)


def test_config_names_and_paths():
    cfg = PipelineConfig.from_params(catalog="workspace", years="2015")
    assert cfg.table("silver", "stg_sinan__tuberculose") == "workspace.tb_silver.stg_sinan__tuberculose"
    assert cfg.manifest_path == "/Volumes/workspace/tb_bronze/landing/manifest/manifest.jsonl"
    assert cfg.parity_report_path == "/Volumes/workspace/tb_ops/reports/parity_report.md"
    assert not cfg.is_full_window
    assert PipelineConfig.from_params("workspace", "all").is_full_window


# ----------------------------------------------------------------- landing

def _write(path, table):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


@pytest.fixture
def source_repo(tmp_path):
    """A miniature copy of the original repository layout."""
    root = tmp_path / "source"
    _write(
        root / "bronze/sinan_tb/ano=2015/data.parquet",
        pa.table({"NU_ANO": ["2015", "2016"], "AGRAVOUTDE": ["x", None]}),
    )
    _write(root / "bronze/sinan_tb/ano=2016/data.parquet", pa.table({"NU_ANO": ["2016"]}))
    _write(root / "bronze/sim_tb_deaths/uf=SP/ano=2015/data.parquet", pa.table({"CAUSABAS": ["A162"]}))
    _write(root / "bronze/ibge_population/ano=2015/data.parquet", pa.table({"D1C": ["3550308"], "V": ["1"]}))
    for table in build_landing.GOLD_TABLES:
        for year in (2015, 2016):
            _write(
                root / f"gold/{table}/ano={year}/data.parquet",
                pa.table({"ano": [year], "valor": [1.5], "__index_level_0__": [7]}),
            )
    return root


def test_build_landing_copies_raw_files_and_writes_manifest(source_repo, tmp_path):
    out = tmp_path / "landing"
    summary = build_landing.build(source_repo, out, "abc123")

    assert summary["raw"]["sinan_tb"] == {"files": 2, "rows": 3}
    raw = out / "sinan_tb/ano=2015/data.parquet"
    assert raw.read_bytes() == (source_repo / "bronze/sinan_tb/ano=2015/data.parquet").read_bytes()

    entries = [json.loads(line) for line in (out / "manifest/manifest.jsonl").read_text().splitlines()]
    assert len(entries) == 4
    sim = next(e for e in entries if e["source"] == "sim_tb_deaths")
    assert (sim["uf"], sim["ano"], sim["rows"], sim["source_commit"]) == ("SP", 2015, 1, "abc123")
    assert len(sim["sha256"]) == 64
    assert next(e for e in entries if e["source"] == "ibge_population")["uf"] is None

    gold = pq.read_table(out / "baseline_gold/fct_incidencia_tb.parquet")
    assert gold.num_rows == 2
    assert gold.column_names == ["ano", "valor"]  # pandas index dropped, years concatenated


def test_build_landing_fails_loudly_when_a_source_is_missing(tmp_path):
    with pytest.raises(FileNotFoundError, match="sinan_tb"):
        build_landing.build(tmp_path / "empty", tmp_path / "out", "abc123")


@pytest.mark.localfs
def test_spark_reads_landing_with_schema_drift_and_partition_columns(spark, source_repo, tmp_path):
    out = tmp_path / "landing"
    build_landing.build(source_repo, out, "abc123")

    df = bronze.add_bronze_metadata(
        bronze.read_landing(spark, (out / "sinan_tb").resolve().as_uri()), ("ano",)
    )
    rows = sorted(df.collect(), key=lambda r: (r["_particao_ano"], r["NU_ANO"]))

    # AGRAVOUTDE only exists in the 2015 file: merged in, NULL for 2016.
    assert [(r["_particao_ano"], r["NU_ANO"], r["AGRAVOUTDE"]) for r in rows] == [
        (2015, "2015", "x"),
        (2015, "2016", None),
        (2016, "2016", None),
    ]
    assert rows[0]["_source_file"].endswith("sinan_tb/ano=2015/data.parquet")
    assert "ano" not in df.columns


# ------------------------------------------------------------------ README

def test_publish_parity_replaces_only_the_marked_block():
    readme = "intro\n<!-- parity:start -->\nold\n<!-- parity:end -->\noutro\n"
    updated = publish_parity.replace_block(readme, "new report\n")
    assert updated == "intro\n<!-- parity:start -->\nnew report\n<!-- parity:end -->\noutro\n"
    with pytest.raises(ValueError):
        publish_parity.replace_block("no markers", "x")
