"""Parity between the gold rebuilt in Spark and the original pandas gold.

For each table the two versions are full-outer-joined on the business key and
compared column by column, so the report says not only whether totals match
but exactly how many rows and which columns differ.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Table -> business key. Every other column of the baseline is compared.
PARITY_KEYS: dict[str, list[str]] = {
    "dim_uf": ["uf_codigo"],
    "fct_incidencia_tb": ["ID_MN_RESI", "NU_ANO"],
    "fct_taxa_abandono": ["ID_MN_RESI", "NU_ANO"],
    "fct_taxa_abandono_uf": ["uf_codigo", "ano"],
    "fct_reconciliacao_sinan_sim": ["uf_sigla", "ano"],
}

FLOAT_TOLERANCE = 1e-9
_NUMERIC = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
_FLOAT = (T.FloatType, T.DoubleType)


@dataclass
class TableParity:
    table: str
    keys: list[str]
    rows_new: int
    rows_baseline: int
    only_in_new: int
    only_in_baseline: int
    matched: int
    column_mismatches: dict[str, int] = field(default_factory=dict)
    sums: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return (
            self.only_in_new == 0
            and self.only_in_baseline == 0
            and self.rows_new == self.rows_baseline
            and all(v == 0 for v in self.column_mismatches.values())
        )


def _clean(df: DataFrame, column: str, dtype: T.DataType) -> Column:
    """Cast to the type of the new table; NaN is treated as NULL (pandas wrote
    missing floats as either)."""
    col = F.col(f"`{column}`").cast(dtype)
    if isinstance(dtype, _FLOAT):
        col = F.when(F.isnan(col), F.lit(None).cast(dtype)).otherwise(col)
    return col


def compare(table: str, new: DataFrame, baseline: DataFrame, keys: list[str]) -> TableParity:
    """Compare `new` against `baseline` on `keys`. Keys must be non-NULL."""
    columns = [c for c in baseline.columns if c not in keys and not c.startswith("__")]
    missing = [c for c in keys + columns if c not in new.columns]
    if missing:
        raise ValueError(f"{table}: columns in the baseline but not in the new table: {missing}")
    types = {f.name: f.dataType for f in new.schema.fields}

    n = new.select(
        *[_clean(new, k, types[k]).alias(k) for k in keys],
        *[_clean(new, c, types[c]).alias(f"n__{c}") for c in columns],
        F.lit(True).alias("_in_new"),
    )
    b = baseline.select(
        *[_clean(baseline, k, types[k]).alias(k) for k in keys],
        *[_clean(baseline, c, types[c]).alias(f"b__{c}") for c in columns],
        F.lit(True).alias("_in_baseline"),
    )
    joined = n.join(b, keys, "full_outer")
    in_new = F.col("_in_new").isNotNull()
    in_base = F.col("_in_baseline").isNotNull()
    both = in_new & in_base

    def count_if(cond):
        return F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))

    aggs = [
        count_if(in_new).alias("rows_new"),
        count_if(in_base).alias("rows_baseline"),
        count_if(in_new & ~in_base).alias("only_in_new"),
        count_if(in_base & ~in_new).alias("only_in_baseline"),
        count_if(both).alias("matched"),
    ]
    numeric = [c for c in columns if isinstance(types[c], _NUMERIC)]
    for c in columns:
        a, e = F.col(f"n__{c}"), F.col(f"b__{c}")
        if isinstance(types[c], _FLOAT):
            equal = (a.isNull() & e.isNull()) | F.coalesce(F.abs(a - e) <= FLOAT_TOLERANCE, F.lit(False))
        else:
            equal = a.eqNullSafe(e)
        aggs.append(count_if(both & ~equal).alias(f"diff__{c}"))
    for c in numeric:
        aggs.append(F.sum(F.col(f"n__{c}").cast("double")).alias(f"sum_new__{c}"))
        aggs.append(F.sum(F.col(f"b__{c}").cast("double")).alias(f"sum_base__{c}"))

    row = joined.agg(*aggs).collect()[0]
    return TableParity(
        table=table,
        keys=keys,
        rows_new=int(row["rows_new"]),
        rows_baseline=int(row["rows_baseline"]),
        only_in_new=int(row["only_in_new"]),
        only_in_baseline=int(row["only_in_baseline"]),
        matched=int(row["matched"]),
        column_mismatches={c: int(row[f"diff__{c}"]) for c in columns},
        sums={c: (row[f"sum_new__{c}"], row[f"sum_base__{c}"]) for c in numeric},
    )


def _fmt(value) -> str:
    if value is None:
        return "NULL"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:,.6f}"


def render_markdown(
    results: list[TableParity],
    *,
    baseline_commit: str,
    years: str,
    generated_at: str,
    strict: bool,
    engine: str,
) -> str:
    """Render the parity report published in the README."""
    all_passed = all(r.passed for r in results)
    if all_passed:
        verdict = "**PASS** — every row and every column of every gold table matches the original."
    elif strict:
        verdict = "**FAIL** — differences found; the job run was failed."
    else:
        verdict = (
            "**PARTIAL RUN** — only part of the year window was processed, so differences against "
            "the full-window baseline are expected and do not fail the job."
        )
    lines = [
        f"Result: {verdict}",
        "",
        f"- Engine: {engine}",
        f"- Baseline: original pandas pipeline at commit `{baseline_commit[:7]}`",
        f"- Years processed by this run: `{years}`",
        f"- Generated at: {generated_at} (UTC)",
        f"- Float tolerance: {FLOAT_TOLERANCE:g} (absolute); everything else compared exactly",
        "",
        "| Table | Key | Rows (Spark) | Rows (original) | Only in Spark | Only in original | Columns compared | Cells differing | Status |",
        "|---|---|---:|---:|---:|---:|---:|---:|:---:|",
    ]
    for r in results:
        cells = sum(r.column_mismatches.values())
        lines.append(
            f"| `{r.table}` | {', '.join(r.keys)} | {r.rows_new:,} | {r.rows_baseline:,} | "
            f"{r.only_in_new:,} | {r.only_in_baseline:,} | {len(r.column_mismatches)} | {cells:,} | "
            f"{'PASS' if r.passed else 'FAIL'} |"
        )
    lines += ["", "Aggregates (sum of each numeric column over the whole table):", ""]
    lines += ["| Table | Column | Spark | Original | Equal |", "|---|---|---:|---:|:---:|"]
    for r in results:
        for column, (new, base) in r.sums.items():
            equal = (new is None and base is None) or (
                new is not None and base is not None and abs(new - base) <= 1e-6 * max(1.0, abs(base))
            )
            lines.append(f"| `{r.table}` | {column} | {_fmt(new)} | {_fmt(base)} | {'yes' if equal else 'NO'} |")
    failing = [(r.table, c, n) for r in results for c, n in r.column_mismatches.items() if n]
    if failing:
        lines += ["", "Columns with differences:", ""]
        lines += [f"- `{t}.{c}`: {n:,} row(s)" for t, c, n in failing]
    return "\n".join(lines) + "\n"
