# Databricks notebook source
pip install skimpy

# COMMAND ----------

pip install dataprep

# COMMAND ----------

operator_sdf=spark.sql('select * from NA_PDC_POWERFLEET.Impact')
operator_df = operator_sdf.toPandas()

# COMMAND ----------

import pandas as pd
from dataprep.eda import create_report

create_report(operator_df)
