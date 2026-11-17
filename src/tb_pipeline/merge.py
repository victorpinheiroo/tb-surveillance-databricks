"""Incremental silver loads with Delta MERGE.

Neither SINAN-TB nor SIM has a trustworthy business key (ADR-002 of the
original project rejected building a synthetic one from quasi-identifiers).
So the merge key is derived from the row itself:

* `_row_key`     identifies a row. For event data it is a hash of every source
                 column plus the source partition, with an occurrence number so
                 that genuinely identical rows are all kept. For reference data
                 it is a hash of the natural key.
* `_record_hash` fingerprints the transformed output, to tell "same row, same
                 result" (no write) from "same row, derived values changed"
                 (update).

Consequence, stated plainly: when the source revises a record, its content
hash changes, so the revision shows up as one delete plus one insert rather
than an update. Updates only happen when derived columns change, e.g. a
revised population figure. Deletes are scoped to the partitions being
reprocessed, so loading one year never touches the others.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ROW_KEY = "_row_key"
RECORD_HASH = "_record_hash"
UPDATED_AT = "_silver_updated_at"
AUDIT_COLUMNS = (ROW_KEY, RECORD_HASH, UPDATED_AT)

# Lineage columns that change on every upstream re-download even when the
# record itself is identical; they must not trigger an update.
VOLATILE_COLUMNS = ("_ingested_at", "_bronze_loaded_at", "_source_file", "__index_level_0__")

_SEP = "\u001f"
_NULL = "␀"


def _hash_of(columns: Iterable[str]) -> Column:
    parts = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit(_NULL)) for c in columns]
    return F.sha2(F.concat_ws(_SEP, *parts), 256)


def with_natural_key(df: DataFrame, key_columns: Iterable[str]) -> DataFrame:
    """`_row_key` from a natural key (must be unique; checked before merging)."""
    return df.withColumn(ROW_KEY, _hash_of(list(key_columns)))


def with_content_key(df: DataFrame, scope_columns: Iterable[str] = ()) -> DataFrame:
    """`_row_key` from the full source content of the row.

    Source columns are the ones not prefixed with `_` (provenance columns are
    excluded on purpose). `scope_columns` adds the source partition, so the
    same content landed in two partitions is two different rows.
    """
    scope = list(scope_columns)
    content = sorted(c for c in df.columns if not c.startswith("_")) + scope
    tiebreak = [c for c in ("_source_file", "__index_level_0__") if c in df.columns] or ["_content_hash"]
    occurrence = Window.partitionBy("_content_hash").orderBy(*tiebreak)
    return (
        df.withColumn("_content_hash", _hash_of(content))
        .withColumn("_occurrence", F.row_number().over(occurrence))
        .withColumn(
            ROW_KEY,
            F.sha2(F.concat_ws(":", F.col("_content_hash"), F.col("_occurrence").cast("string")), 256),
        )
        .drop("_content_hash", "_occurrence")
    )


def with_record_hash(df: DataFrame) -> DataFrame:
    """`_record_hash` over every output column except audit and volatile ones."""
    columns = sorted(c for c in df.columns if c not in AUDIT_COLUMNS and c not in VOLATILE_COLUMNS)
    return df.withColumn(RECORD_HASH, _hash_of(columns))


def _align_to_target(spark: SparkSession, staged: DataFrame, target: str) -> DataFrame:
    """Make `staged` match the target column list.

    A subset run can legitimately carry fewer columns (SIM files from 2019 on
    have no `CRM`), and a wider one more. New columns are added to the table;
    missing ones are filled with NULL.
    """
    existing = {f.name for f in spark.table(target).schema.fields}
    new_fields = [f for f in staged.schema.fields if f.name not in existing]
    if new_fields:
        ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields)
        spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({ddl})")
    staged_columns = set(staged.columns)
    return staged.select(
        *[
            (
                F.col(f"`{f.name}`").cast(f.dataType)
                if f.name in staged_columns
                else F.lit(None).cast(f.dataType)
            ).alias(f.name)
            for f in spark.table(target).schema.fields
        ]
    )


def merge_into(
    spark: SparkSession, staged: DataFrame, target: str, delete_scope: str | None = None
) -> dict:
    """MERGE `staged` into `target` on `_row_key`. Returns the row metrics.

    `delete_scope` is a predicate over the target alias `t` limiting which
    unmatched target rows may be deleted, e.g. `t._particao_ano IN (2015)`.
    With no scope the staged data is the complete new state of the table.
    """
    missing = [c for c in (ROW_KEY, RECORD_HASH) if c not in staged.columns]
    if missing:
        raise ValueError(f"staged DataFrame for {target} lacks {missing}")
    staged = staged.withColumn(UPDATED_AT, F.current_timestamp())

    if not spark.catalog.tableExists(target):
        staged.write.format("delta").saveAsTable(target)
        return {
            "created": True,
            "num_inserted_rows": spark.table(target).count(),
            "num_updated_rows": 0,
            "num_deleted_rows": 0,
        }

    view = f"_staged_{uuid.uuid4().hex}"
    _align_to_target(spark, staged, target).createOrReplaceTempView(view)
    scope = f" AND {delete_scope}" if delete_scope else ""
    try:
        result = spark.sql(
            f"""
            MERGE INTO {target} AS t
            USING {view} AS s
            ON t.{ROW_KEY} = s.{ROW_KEY}
            WHEN MATCHED AND t.{RECORD_HASH} <> s.{RECORD_HASH} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE{scope} THEN DELETE
            """
        )
        rows = result.collect()
    finally:
        spark.catalog.dropTempView(view)

    metrics = rows[0].asDict() if rows else {}
    return {
        "created": False,
        "num_inserted_rows": int(metrics.get("num_inserted_rows", 0) or 0),
        "num_updated_rows": int(metrics.get("num_updated_rows", 0) or 0),
        "num_deleted_rows": int(metrics.get("num_deleted_rows", 0) or 0),
    }
