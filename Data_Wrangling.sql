-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Wrangling to understand the data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Schema of all the tables

-- COMMAND ----------

describe NA_PDC_POWERFLEET.Vehicle

-- COMMAND ----------

describe NA_PDC_POWERFLEET.Operator

-- COMMAND ----------

describe NA_PDC_POWERFLEET.IDSY_PEOPLE

-- COMMAND ----------

describe NA_PDC_POWERFLEET.Impact

-- COMMAND ----------

describe NA_PDC_POWERFLEET.Checklist

-- COMMAND ----------

describe NA_PDC_POWERFLEET.Sleeper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## List and count of different vehicle models

-- COMMAND ----------

select count(*), model from NA_PDC_POWERFLEET.Vehicle group by model order by count(*) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Number of operators by Department

-- COMMAND ----------

select count(*),CHARGE_DEPT_2 from NA_PDC_POWERFLEET.operator group by CHARGE_DEPT_2 order by count(*) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### List of operators having less than 6 months of experience

-- COMMAND ----------

select * from NA_PDC_POWERFLEET.operator where user_start_dt>date_sub(current_timestamp(), 180)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Number of impacts by severity

-- COMMAND ----------

select severity,count(impact_id) as num_of_impacts from NA_PDC_POWERFLEET.Impact group by severity order by count(impact_id) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Number of impacts by Vehicle model type

-- COMMAND ----------

select model,severity,count(impact_id) as num_of_impacts from NA_PDC_POWERFLEET.Impact group by model,severity order by count(impact_id) asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Percentage of failed checklist

-- COMMAND ----------

select * from NA_PDC_POWERFLEET.checklist

-- COMMAND ----------

--select count(*),vehicle,DateCompleted,severity from NA_PDC_POWERFLEET.checklist group by vehicle,DateCompleted,severity order by count(*) asc
select count(*),vehicle,severity from NA_PDC_POWERFLEET.checklist group by vehicle,severity order by count(*) asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1 for distinct status_at_impact
-- MAGIC ### sample sql

-- COMMAND ----------

select distinct status_at_impact from NA_PDC_POWERFLEET.Impact

-- COMMAND ----------

select * from NA_PDC_POWERFLEET.Sleeper

-- COMMAND ----------

describe NA_PDC_POWERFLEET.sleeper


-- COMMAND ----------

select distinct sleep_thresh_mins from na_pdc_powerfleet.sleeper order by sleep_thresh_mins desc

-- COMMAND ----------

select distinct s.Employee_ID,o.EMP_NUM,o.display_name from NA_PDC_POWERFLEET.Sleeper s, NA_PDC_POWERFLEET.Operator o
where s.Employee_ID=o.EMP_NUM

-- COMMAND ----------

select distinct s.Employee_ID from NA_PDC_POWERFLEET.Sleeper s

-- COMMAND ----------

select count(*),Operator from NA_PDC_POWERFLEET.Sleeper group by Operator order by count(*) desc

-- COMMAND ----------

select * from NA_PDC_POWERFLEET.Operator where emp_num like '%60%'

-- COMMAND ----------

select distinct s.vehicle,s.sleep_thresh_mins,s.Veh_Ext_ID_2,v.model from NA_PDC_POWERFLEET.Sleeper s, NA_PDC_POWERFLEET.Vehicle v
where s.vehicle=v.vehicle order by s.sleep_thresh_mins desc

-- COMMAND ----------

select * from na_pdc_powerfleet.checklist where severity='Critical'

-- COMMAND ----------

select distinct severity from na_pdc_powerfleet.checklist

-- COMMAND ----------

describe NA_PDC_POWERFLEET.IDSY_PEOPLE
