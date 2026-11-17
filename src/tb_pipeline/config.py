"""Pipeline configuration: table names, Volume paths and the year window."""

from __future__ import annotations

from dataclasses import dataclass

# Fixed, validated window (ADR-004 of the original project): extending it is a
# deliberate manual decision, never derived from the current date.
WINDOW_YEARS: tuple[int, ...] = tuple(range(2015, 2025))


@dataclass(frozen=True)
class Source:
    """A raw source landed in the Volume as hive-partitioned Parquet."""

    name: str  # landing folder name and bronze table name
    partition_cols: tuple[str, ...]  # hive partition columns in the landing path
    incremental: bool  # True: processed per requested year; False: always the full window


SOURCES: tuple[Source, ...] = (
    Source("sinan_tb", ("ano",), incremental=True),
    Source("sim_tb_deaths", ("uf", "ano"), incremental=True),
    # Reference data: tiny, and SINAN rows of one file join population of
    # other years, so it is always refreshed for the whole window.
    Source("ibge_population", ("ano",), incremental=False),
)


def parse_years(text: str) -> tuple[int, ...]:
    """Parse the `years` job parameter.

    Accepts "all", single years, comma lists and inclusive ranges:
    "2015", "2015,2017", "2015-2018", "2015-2016,2020".
    """
    text = (text or "").strip().lower()
    if text in ("", "all"):
        return WINDOW_YEARS
    years: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = (int(p) for p in part.split("-", 1))
            if start > end:
                raise ValueError(f"Invalid year range: {part!r}")
            years.update(range(start, end + 1))
        else:
            years.add(int(part))
    outside = sorted(years - set(WINDOW_YEARS))
    if outside:
        raise ValueError(
            f"Years outside the validated window {WINDOW_YEARS[0]}-{WINDOW_YEARS[-1]}: {outside}"
        )
    return tuple(sorted(years))


@dataclass(frozen=True)
class PipelineConfig:
    years: tuple[int, ...] = WINDOW_YEARS
    catalog: str = "workspace"
    bronze_schema: str = "tb_bronze"
    silver_schema: str = "tb_silver"
    gold_schema: str = "tb_gold"
    ops_schema: str = "tb_ops"
    landing_volume: str = "landing"
    reports_volume: str = "reports"

    @classmethod
    def from_params(cls, catalog: str, years: str) -> PipelineConfig:
        return cls(years=parse_years(years), catalog=catalog.strip() or "workspace")

    @property
    def is_full_window(self) -> bool:
        """Parity is only asserted strictly when the whole window was processed."""
        return tuple(self.years) == WINDOW_YEARS

    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "ops": self.ops_schema,
        }[layer]
        return f"{self.catalog}.{schema}.{name}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"

    @property
    def manifest_path(self) -> str:
        return f"{self.landing_path}/manifest/manifest.jsonl"

    def baseline_gold_path(self, table: str) -> str:
        return f"{self.landing_path}/baseline_gold/{table}.parquet"

    @property
    def parity_report_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.ops_schema}/{self.reports_volume}/parity_report.md"

    def years_sql(self) -> str:
        return ", ".join(str(y) for y in self.years)
