# Databricks notebook source
# MAGIC %md
# MAGIC Shared setup for every task notebook: puts the bundle's `src/` on the Python
# MAGIC path and builds the pipeline configuration from the job parameters.

# COMMAND ----------

import os
import sys

# The bundle syncs the repository to the workspace; notebooks run with their
# own folder as the working directory, so `src/` is one level up.
_SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(os.path.join(_SRC, "tb_pipeline")):
    raise RuntimeError(f"tb_pipeline package not found at {_SRC}; was the bundle deployed with its src/ folder?")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from tb_pipeline.config import PipelineConfig  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("years", "2015")

cfg = PipelineConfig.from_params(catalog=dbutils.widgets.get("catalog"), years=dbutils.widgets.get("years"))
print(f"catalog={cfg.catalog} years={cfg.years} full_window={cfg.is_full_window}")
