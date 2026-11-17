# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Parity report
# MAGIC Compares every gold table against the gold produced by the original pandas pipeline,
# MAGIC row by row and column by column. Fails the job on any difference when the whole
# MAGIC 2015–2024 window was processed.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from tb_pipeline import jobs

jobs.run_parity(spark, cfg)

# COMMAND ----------

display(spark.table(cfg.table("ops", "parity_results")).orderBy("run_ts", ascending=False).limit(10))
