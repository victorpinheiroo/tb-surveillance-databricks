# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Typed, decoded and deduplicated tables, loaded incrementally with Delta `MERGE`.
# MAGIC Quality checks run on the staged data; nothing is merged if any of them fails.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from tb_pipeline import jobs

jobs.run_silver(spark, cfg)

# COMMAND ----------

display(spark.table(cfg.table("ops", "merge_log")).orderBy("run_ts", ascending=False).limit(8))
