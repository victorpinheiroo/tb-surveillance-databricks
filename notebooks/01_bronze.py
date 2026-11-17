# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Raw Parquet files uploaded by GitHub Actions to the landing Volume are loaded into
# MAGIC bronze Delta tables. The task fails if any file's row count differs from the manifest.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from tb_pipeline import jobs

jobs.run_bronze(spark, cfg)
