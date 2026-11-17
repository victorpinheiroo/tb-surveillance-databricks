"""Quality gate: checks are collected, persisted, and then fail the task.

Results are written to a Delta table *before* raising, so a failed run leaves
a record of what failed. Raising makes the Databricks task fail, which stops
every downstream task of the job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

RESULT_SCHEMA = (
    "run_ts timestamp, layer string, table_name string, check_name string, "
    "passed boolean, observed string, expected string, severity string"
)


class QualityGateError(RuntimeError):
    """Raised when at least one blocking check fails."""


@dataclass(frozen=True)
class CheckResult:
    layer: str
    table: str
    check: str
    passed: bool
    observed: str
    expected: str
    severity: str = "error"  # "error" blocks the job; "info" is a recorded metric


@dataclass
class QualityGate:
    layer: str
    results: list[CheckResult] = field(default_factory=list)

    def expect_equal(self, table: str, check: str, observed, expected) -> bool:
        passed = observed == expected
        self.results.append(CheckResult(self.layer, table, check, passed, str(observed), str(expected)))
        return passed

    def expect_zero(self, table: str, check: str, observed) -> bool:
        return self.expect_equal(table, check, int(observed or 0), 0)

    def expect_true(self, table: str, check: str, condition: bool, observed, expected: str) -> bool:
        self.results.append(
            CheckResult(self.layer, table, check, bool(condition), str(observed), expected)
        )
        return bool(condition)

    def metric(self, table: str, name: str, value) -> None:
        """Record a diagnostic that never blocks (e.g. rows dropped by a rule)."""
        self.results.append(CheckResult(self.layer, table, name, True, str(value), "n/a", "info"))

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "error"]

    def summary(self) -> str:
        lines = [f"Quality gate [{self.layer}]: {len(self.results)} results, {len(self.failures)} failed"]
        for r in self.results:
            mark = "info" if r.severity == "info" else ("PASS" if r.passed else "FAIL")
            lines.append(f"  [{mark}] {r.table}.{r.check}: observed={r.observed} expected={r.expected}")
        return "\n".join(lines)

    def raise_if_failed(self) -> None:
        if self.failures:
            details = "; ".join(
                f"{r.table}.{r.check} (observed={r.observed}, expected={r.expected})" for r in self.failures
            )
            raise QualityGateError(f"{len(self.failures)} quality check(s) failed in {self.layer}: {details}")


def persist(spark: SparkSession, gate: QualityGate, table: str) -> None:
    """Append the gate results to the quality log table."""
    if not gate.results:
        return
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (run_ts, r.layer, r.table, r.check, r.passed, r.observed, r.expected, r.severity)
        for r in gate.results
    ]
    spark.createDataFrame(rows, RESULT_SCHEMA).write.format("delta").mode("append").saveAsTable(table)


def finish(spark: SparkSession, gate: QualityGate, table: str) -> None:
    """Print, persist, then fail the task if any blocking check failed."""
    print(gate.summary())
    persist(spark, gate, table)
    gate.raise_if_failed()
