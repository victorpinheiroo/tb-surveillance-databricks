"""Local Spark session for unit tests.

ANSI mode is on, as on Databricks serverless, so a transformation that would
raise on malformed data there also raises here.
"""

from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Python workers must use the same interpreter as the driver (matters on Windows).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# Spark needs Hadoop's native IO to touch the local filesystem; on Windows that
# means winutils. Without it, tests that read or write files are skipped there
# and run in CI (Linux). Pure DataFrame tests run everywhere.
LOCAL_FS_AVAILABLE = os.name != "nt" or bool(os.environ.get("HADOOP_HOME"))


def _delta_available() -> bool:
    try:
        import delta  # noqa: F401
    except ImportError:
        return False
    return LOCAL_FS_AVAILABLE


DELTA_AVAILABLE = _delta_available()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tb-pipeline-tests")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    if DELTA_AVAILABLE:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def pytest_collection_modifyitems(config, items):
    skip_delta = pytest.mark.skip(reason="Delta not usable here (needs delta-spark; on Windows also winutils)")
    skip_fs = pytest.mark.skip(reason="Spark cannot use the local filesystem on Windows without winutils")
    for item in items:
        if "delta" in item.keywords and not DELTA_AVAILABLE:
            item.add_marker(skip_delta)
        if "localfs" in item.keywords and not LOCAL_FS_AVAILABLE:
            item.add_marker(skip_fs)
