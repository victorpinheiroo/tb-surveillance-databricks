"""Build the landing folder that GitHub Actions uploads to the Databricks Volume.

Runs outside Databricks (Free Edition restricts outbound internet, so nothing
is downloaded from DATASUS there). Input is a checkout of the original
repository at the pinned baseline commit:

    landing/
      sinan_tb/ano=YYYY/data.parquet            raw bronze files, byte-identical
      sim_tb_deaths/uf=XX/ano=YYYY/data.parquet
      ibge_population/ano=YYYY/data.parquet
      manifest/manifest.jsonl                   one line per raw file: rows + sha256
      baseline_gold/<table>.parquet             original gold, one file per table

The manifest is the hand-off contract: the bronze task refuses to load unless
every file it reads has exactly the row count recorded here.

Usage:
    python scripts/build_landing.py --source _source --out landing --source-commit <sha>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SOURCES = ("sinan_tb", "sim_tb_deaths", "ibge_population")
GOLD_TABLES = (
    "dim_uf",
    "fct_incidencia_tb",
    "fct_taxa_abandono",
    "fct_taxa_abandono_uf",
    "fct_reconciliacao_sinan_sim",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _partition_values(relative: Path) -> dict[str, str]:
    """`sim_tb_deaths/uf=SP/ano=2022/data.parquet` -> {"uf": "SP", "ano": "2022"}."""
    return dict(part.split("=", 1) for part in relative.parts if "=" in part)


def copy_raw_files(source_root: Path, out_root: Path, source_commit: str) -> list[dict]:
    """Copy raw bronze Parquet files unchanged and describe each in the manifest."""
    entries = []
    for source in SOURCES:
        files = sorted((source_root / "bronze" / source).rglob("data.parquet"))
        if not files:
            raise FileNotFoundError(f"No Parquet files for source {source!r} under {source_root}")
        for file in files:
            relative = file.relative_to(source_root / "bronze")
            target = out_root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file, target)
            partition = _partition_values(relative)
            entries.append(
                {
                    "source": source,
                    "path": relative.as_posix(),
                    "uf": partition.get("uf"),
                    "ano": int(partition["ano"]),
                    "rows": pq.read_metadata(file).num_rows,
                    "sha256": _sha256(file),
                    "source_commit": source_commit,
                }
            )
    return entries


def write_manifest(entries: list[dict], out_root: Path) -> Path:
    path = out_root / "manifest" / "manifest.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return path


def consolidate_gold(source_root: Path, out_root: Path) -> dict[str, int]:
    """Write the original gold as one plain Parquet file per table.

    The original is partitioned by `ano=` folders and carries the pandas
    index; flattening it here keeps the parity task a simple file read.
    """
    counts = {}
    for table in GOLD_TABLES:
        files = sorted((source_root / "gold" / table).rglob("data.parquet"))
        if not files:
            raise FileNotFoundError(f"No gold files for table {table!r} under {source_root}")
        parts = []
        for file in files:
            part = pq.ParquetFile(file).read()
            drop = [c for c in part.column_names if c.startswith("__index_level_")]
            parts.append(part.drop_columns(drop).replace_schema_metadata(None))
        combined = pa.concat_tables(parts, promote_options="default")
        target = out_root / "baseline_gold" / f"{table}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(combined, target)
        counts[table] = combined.num_rows
    return counts


def build(source_root: Path, out_root: Path, source_commit: str) -> dict:
    if out_root.exists():
        shutil.rmtree(out_root)
    entries = copy_raw_files(source_root, out_root, source_commit)
    write_manifest(entries, out_root)
    gold_rows = consolidate_gold(source_root, out_root)
    by_source: dict[str, dict[str, int]] = {}
    for entry in entries:
        stats = by_source.setdefault(entry["source"], {"files": 0, "rows": 0})
        stats["files"] += 1
        stats["rows"] += entry["rows"]
    return {"raw": by_source, "baseline_gold_rows": gold_rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", type=Path, required=True, help="checkout of the original repository")
    parser.add_argument("--out", type=Path, default=Path("landing"))
    parser.add_argument("--source-commit", required=True, help="commit SHA of the original repository")
    args = parser.parse_args()
    summary = build(args.source, args.out, args.source_commit)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
