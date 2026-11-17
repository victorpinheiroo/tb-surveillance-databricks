# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold
# MAGIC Incidence, treatment abandonment and SINAN × SIM reconciliation, with the same table
# MAGIC and column names as the original pipeline.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from tb_pipeline import jobs

jobs.run_gold(spark, cfg)

# COMMAND ----------

display(spark.table(cfg.table("ops", "quality_results")).orderBy("run_ts", ascending=False).limit(60))
