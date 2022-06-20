# Databricks notebook source
# MAGIC %fs
# MAGIC ls 'mnt/edl/raw/na_pdc_powerfleet'

# COMMAND ----------

people_df = spark.read.option("header",True) \
     .csv("/mnt/edl/raw/na_pdc_powerfleet/IDSY_PEOPLE_20220616_164343.csv")


# COMMAND ----------

people_df.display()

# COMMAND ----------

people_df.count()

# COMMAND ----------

people_df.write.parquet('/mnt/edl/raw/na_pdc_powerfleet/IDSY_PEOPLE/', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS NA_PDC_POWERFLEET;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS NA_PDC_POWERFLEET.IDSY_PEOPLE
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/edl/raw/na_pdc_powerfleet/IDSY_PEOPLE/'
# MAGIC TBLPROPERTIES ("edl_sources"="com.deere.enterprise.datalake.raw.na_pdc_powerfleet")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE NA_PDC_POWERFLEET.IDSY_PEOPLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from NA_PDC_POWERFLEET.IDSY_PEOPLE

# COMMAND ----------

people_df.describe().toPandas()
