"""Landing (raw Parquet in a Unity Catalog Volume) -> bronze Delta tables.

Bronze stays a faithful copy of the source: every source column is kept as
landed (all strings). Only provenance columns are added, all prefixed `_`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    """Read a hive-partitioned landing folder (`ano=2015/data.parquet`, ...).

    `mergeSchema` is required: column sets drift between years (SINAN-TB lost
    three legacy columns, SIM lost `CRM` in 2019). `_metadata.file_path` is
    used instead of `input_file_name()`, which is not available on serverless.
    """
    return (
        spark.read.option("mergeSchema", "true")
        .parquet(path)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
    )


def add_bronze_metadata(df: DataFrame, partition_cols: tuple[str, ...]) -> DataFrame:
    """Rename path partition columns to `_particao_*` and stamp the load time.

    The partition year is provenance (which yearly file the row came from), not
    a business attribute: a SINAN-TB yearly file also carries notifications
    whose `NU_ANO` is a different year.
    """
    for col in partition_cols:
        df = df.withColumnRenamed(col, f"_particao_{col}")
    return df.withColumn("_particao_ano", F.col("_particao_ano").cast("int")).withColumn(
        "_bronze_loaded_at", F.current_timestamp()
    )
