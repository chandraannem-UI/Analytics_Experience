# Databricks notebook source
# MAGIC %fs
# MAGIC ls 'mnt/edl/raw/na_pdc_powerfleet'

# COMMAND ----------

import pandas as pd
df = pd.read_fwf("/dbfs/mnt/edl/raw/na_pdc_powerfleet/APIImpactDetail_05302022.rpt", skiprows=[1],header=1)

# COMMAND ----------

df.head()

# COMMAND ----------

df.drop(index=df.index[0], 
        axis=0, 
        inplace=True)

# COMMAND ----------

df.head()

# COMMAND ----------

df.tail()

# COMMAND ----------

df.drop(index=df.index[-1], 
        axis=0, 
        inplace=True)

# COMMAND ----------

df.drop(index=df.index[-1], 
        axis=0, 
        inplace=True)

# COMMAND ----------

df.tail()

# COMMAND ----------

sdf = spark.createDataFrame(df)

# COMMAND ----------

sdf.display()

# COMMAND ----------

sdf.write.parquet('/mnt/edl/raw/na_pdc_powerfleet/Impact/', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS NA_PDC_POWERFLEET;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS NA_PDC_POWERFLEET.Impact
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/edl/raw/na_pdc_powerfleet/Impact/'
# MAGIC TBLPROPERTIES ("edl_sources"="com.deere.enterprise.datalake.raw.na_pdc_powerfleet")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE NA_PDC_POWERFLEET.Impact

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from NA_PDC_POWERFLEET.Impact
