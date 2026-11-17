"""MERGE behaviour against real Delta tables (runs in CI; needs delta-spark)."""

import uuid

import pytest

from tb_pipeline import merge

SCHEMA = "CASO string, SITUA string, _particao_ano int, populacao bigint"


def staged(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    return merge.with_record_hash(merge.with_content_key(df, scope_columns=["_particao_ano"]))


def test_content_key_keeps_identical_rows_and_is_stable(spark):
    rows = [("a", "1", 2015, 10), ("a", "1", 2015, 10), ("b", "1", 2015, 10)]
    first = [r[merge.ROW_KEY] for r in staged(spark, rows).collect()]
    second = [r[merge.ROW_KEY] for r in staged(spark, list(reversed(rows))).collect()]

    assert len(set(first)) == 3  # two identical source rows get two distinct keys
    assert sorted(first) == sorted(second)  # independent of input order


def test_content_key_ignores_provenance_but_not_partition(spark):
    df = spark.createDataFrame(
        [("a", "x", 2015), ("a", "y", 2015), ("a", "x", 2016)], "CASO string, _ingested_at string, _particao_ano int"
    )
    keys = [r[merge.ROW_KEY] for r in merge.with_content_key(df, scope_columns=["_particao_ano"]).collect()]
    # Rows 1 and 2 differ only in a provenance column: same content, so they
    # are occurrences 1 and 2 of it. Row 3 is the same content in another file.
    assert len(set(keys)) == 3


def test_record_hash_ignores_volatile_lineage(spark):
    df = spark.createDataFrame(
        [("k", "v", "2026-08-01"), ("k", "v", "2026-11-01")], "_row_key string, valor string, _ingested_at string"
    )
    hashes = {r[merge.RECORD_HASH] for r in merge.with_record_hash(df).collect()}
    assert len(hashes) == 1


@pytest.mark.delta
def test_merge_is_incremental_and_idempotent(spark):
    target = f"default.merge_{uuid.uuid4().hex[:8]}"
    y2015 = [("a", "1", 2015, 10), ("b", "", 2015, 10)]
    y2016 = [("c", "1", 2016, 20)]

    created = merge.merge_into(spark, staged(spark, y2015), target, "t._particao_ano IN (2015)")
    assert created == {"created": True, "num_inserted_rows": 2, "num_updated_rows": 0, "num_deleted_rows": 0}

    # Loading another year adds its rows and leaves 2015 untouched.
    added = merge.merge_into(spark, staged(spark, y2016), target, "t._particao_ano IN (2016)")
    assert (added["num_inserted_rows"], added["num_updated_rows"], added["num_deleted_rows"]) == (1, 0, 0)
    assert spark.table(target).count() == 3

    # Re-running the same data writes nothing.
    again = merge.merge_into(spark, staged(spark, y2015), target, "t._particao_ano IN (2015)")
    assert (again["num_inserted_rows"], again["num_updated_rows"], again["num_deleted_rows"]) == (0, 0, 0)

    # The source revised record "b" (outcome filled in) and dropped nothing else:
    # with a content-derived key that is one delete plus one insert.
    revised = [("a", "1", 2015, 10), ("b", "2", 2015, 10)]
    changed = merge.merge_into(spark, staged(spark, revised), target, "t._particao_ano IN (2015)")
    assert (changed["num_inserted_rows"], changed["num_updated_rows"], changed["num_deleted_rows"]) == (1, 0, 1)

    # Only a derived value changed (population revised): an in-place update.
    repop = [("a", "1", 2015, 11), ("b", "2", 2015, 10)]
    updated = merge.merge_into(spark, staged(spark, repop), target, "t._particao_ano IN (2015)")
    assert (updated["num_inserted_rows"], updated["num_updated_rows"], updated["num_deleted_rows"]) == (0, 1, 0)

    final = {(r["CASO"], r["SITUA"], r["_particao_ano"], r["populacao"]) for r in spark.table(target).collect()}
    assert final == {("a", "1", 2015, 11), ("b", "2", 2015, 10), ("c", "1", 2016, 20)}


@pytest.mark.delta
def test_merge_aligns_schema_drift_between_runs(spark):
    target = f"default.merge_{uuid.uuid4().hex[:8]}"
    wide = spark.createDataFrame([("a", "crm-1", 2018)], "CASO string, CRM string, _particao_ano int")
    narrow = spark.createDataFrame([("b", 2019)], "CASO string, _particao_ano int")
    wider = spark.createDataFrame([("c", "x", 2020)], "CASO string, NOVA string, _particao_ano int")

    def prepare(df):
        return merge.with_record_hash(merge.with_content_key(df, scope_columns=["_particao_ano"]))

    merge.merge_into(spark, prepare(wide), target, "t._particao_ano IN (2018)")
    merge.merge_into(spark, prepare(narrow), target, "t._particao_ano IN (2019)")  # lacks CRM
    merge.merge_into(spark, prepare(wider), target, "t._particao_ano IN (2020)")  # brings NOVA

    rows = {r["CASO"]: r for r in spark.table(target).collect()}
    assert rows["a"]["CRM"] == "crm-1"
    assert rows["b"]["CRM"] is None
    assert rows["c"]["NOVA"] == "x"
    assert rows["a"]["NOVA"] is None


def test_merge_requires_keys(spark):
    df = spark.createDataFrame([("a",)], "CASO string")
    with pytest.raises(ValueError, match="_row_key"):
        merge.merge_into(spark, df, "default.never_created")
