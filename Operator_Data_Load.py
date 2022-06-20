# Databricks notebook source
# MAGIC %fs
# MAGIC ls 'mnt/edl/raw/na_pdc_powerfleet'

# COMMAND ----------

Operator_DF = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Data'!A1") \
    .load('/mnt/edl/raw/na_pdc_powerfleet/WORK_FORCE_MAKEUP.xlsx')

# COMMAND ----------

Operator_DF.schema

# COMMAND ----------

Operator_DF.write.parquet('/mnt/edl/raw/na_pdc_powerfleet/Operator/', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS NA_PDC_POWERFLEET;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE NA_PDC_POWERFLEET.Operator
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/edl/raw/na_pdc_powerfleet/Operator/'
# MAGIC TBLPROPERTIES ("edl_sources"="com.deere.enterprise.datalake.raw.na_pdc_powerfleet")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NA_PDC_POWERFLEET.Operator
