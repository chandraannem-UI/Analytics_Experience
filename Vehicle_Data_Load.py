# Databricks notebook source
# MAGIC %fs
# MAGIC ls 'mnt/edl/raw/na_pdc_powerfleet'

# COMMAND ----------

Vehicle_DF = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Report1654004084784'!A1") \
    .load('/mnt/edl/raw/na_pdc_powerfleet/JD_Milan_Vehicle_List.xlsx')

# COMMAND ----------

Vehicle_DF.schema

# COMMAND ----------

Vehicle_DF.write.parquet('/mnt/edl/raw/na_pdc_powerfleet/Vehicle/', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS NA_PDC_POWERFLEET;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE NA_PDC_POWERFLEET.Vehicle
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/edl/raw/na_pdc_powerfleet/Vehicle/'
# MAGIC TBLPROPERTIES ("edl_sources"="com.deere.enterprise.datalake.raw.na_pdc_powerfleet")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NA_PDC_POWERFLEET.Vehicle
