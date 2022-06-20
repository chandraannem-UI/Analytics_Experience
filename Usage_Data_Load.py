# Databricks notebook source
# MAGIC %fs
# MAGIC ls 'mnt/edl/raw/na_pdc_powerfleet'

# COMMAND ----------

s='-------------------- -------------- ----------- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ----------------------- ----------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ----------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------- ----------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --------------------------------------- ----------------------- ----------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- ---------------------------------------------------------------------------------------------------- ----------- --------------------------------------- ------------------ ------------- --------------------------------------- --------------------------------------- --------------------------------------- ----------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- ----------- ----------- ----------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- ----------- --------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- ---------------------------------------------------------------------------------------------------- ----------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- --------------------------------------- -------------------- -------------------- ---------------------- --------------- --------------------------------------- ----------------------- ----------------------- ----------------------- ----------- ---------- ------------------------ ----------------------- ---------------------- --------------------- ----------------------- ----------------------- ------------------------- ----------------------- -------- -----'

# COMMAND ----------

s_list= s.split()
print(s_list)

# COMMAND ----------

vl = s.split()

len(vl)

# COMMAND ----------

len_list=[]
tup_list=[]
l=0
for i in s_list:
  len_1=len(i)
  tup_1=(l,l+len(i))
  len_list.append(len_1)
  tup_list.append(tup_1)
  l=l+len(i)+1
print(len_list)
print(tup_list)
  
  

# COMMAND ----------

names = 'Row_ID               Usage_Event_ID Shift_ID    Shift_Name                                                                                                                      Job_Title                                                                                                                                                                                                PowerUp_hrs                             Login                   Logoff                  Login_hrs                               Motion_hrs                              Idling_hrs                              Eng_Off_hrs                             Lift_hrs                                Pct_Motion                              Pct_Motion_2                            Pct_Idling                              Pct_Lift                                Pct_Lift_2                              Pct_Eng_Off                             End_Method                                                                                           End_Code    Batt_Vlt                                Batt_Level_Invalid Total_Impacts Cum_Eng_Starts                          Cumm_Lift                               Max_mph                                 T_ft        Odometer_miles                          Motion_2_hrs                            Lift_2_hrs                              GPU_Out_hrs                             Air_Out_hrs                             Auto_Level_hrs                          Canopy_hrs                              Plane_On_hrs                            Fuel_Level_Start                        Fuel_Level_End                          Output_Temp_Min                         Output_Temp_Max                         Max_lbs                                 Avg_lbs                                 TWL_ft      L_Count     LWL_Count   LWL_hrs                                 Odom_hrs                                Odom_w_Load_hrs                         Max_Lift_wo_Load_hrs                    Tow_Count   Dist_w_Tow_feet Total_Tow_hrs                           Max_Tow_wo_Load_hrs                     T_hrs                                   TWL_hrs                                 Pct_T                                   Pct_TWL                                 Sensor_Type                                                                                          Sensor_Code Key_On_Hrs                              Dead_Man_Hrs                            Load_Hdlr_Hrs                           Activity_Hrs                            Coast_Hrs                               Cum_Key_On                              Cum_Dead_Man                            Cum_Load_Hdlr                           Cum_Activity                            Cum_Coast                               Cum_GPU_Out_Hrs                         Cum_Air_Out_Hrs                         Cum_Auto_Level_Hrs                      Cum_Canopy_Hrs                          Cum_Plane_On_Hrs                        Hard_Bypass_Flag     Soft_Bypass_Flag     Soft_Bypass_Start_Flag Usage_Rec_Count PU_Batt_Vlt                             PU_Date                 Date_Non_Compliant      Next_Completed_OSHA     Overdue     Login_Date Operator_Daily_Use_Count Vehicle_Daily_Use_Count Operator_Daily_Use_Sum Vehicle_Daily_Use_Sum Login_Date_05_Minute    Login_Date_15_Minute    Operator_Simul_Login_Flag Last_Update             Customer Site'

# COMMAND ----------

names_list=names.split()
print(names_list)

# COMMAND ----------

len(names_list)


# COMMAND ----------

len(len_list)

# COMMAND ----------

import pandas as pd
df = pd.read_fwf("/dbfs/mnt/edl/raw/na_pdc_powerfleet/APIUsageDetail_05302022.rpt",skipfooter=5,skiprows=2,colspecs=tup_list,names=names_list)

# COMMAND ----------

import pandas as pd
df = pd.read_fwf("/dbfs/mnt/edl/raw/na_pdc_powerfleet/APIUsageDetail_05302022.rpt",skipfooter=5,header=0)

# COMMAND ----------

df.head()

# COMMAND ----------

df.tail()

# COMMAND ----------

sdf = spark.createDataFrame(df)

# COMMAND ----------

sdf.display()

# COMMAND ----------

sdf.write.parquet('/mnt/edl/raw/na_pdc_powerfleet/Checklist/', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS NA_PDC_POWERFLEET;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS NA_PDC_POWERFLEET.Checklist
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/edl/raw/na_pdc_powerfleet/Checklist/'
# MAGIC TBLPROPERTIES ("edl_sources"="com.deere.enterprise.datalake.raw.na_pdc_powerfleet")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE NA_PDC_POWERFLEET.Checklist

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from NA_PDC_POWERFLEET.Checklist