# Databricks notebook source
# MAGIC %sql
# MAGIC select * from na_pdc_powerfleet.impact

# COMMAND ----------

# MAGIC %sql
# MAGIC describe na_pdc_powerfleet.impact

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(impact_date) as ds,count(*) as y  from na_pdc_powerfleet.impact 
# MAGIC where to_date(impact_date)>'2021-01-01'
# MAGIC group by to_date(impact_date)
# MAGIC order by to_date(impact_date)

# COMMAND ----------

#pip install prophet

# COMMAND ----------

import pandas as pd
from prophet import Prophet

# instantiate the model and set parameters
model = Prophet(
    interval_width=0.95,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=False,
    seasonality_mode='multiplicative'
)


# COMMAND ----------

history_pd_s = spark.sql("""select to_date(impact_date) as ds,count(*) as y  from na_pdc_powerfleet.impact 
where to_date(impact_date)>'2021-01-01'
group by to_date(impact_date)""")
history_pd = history_pd_s.toPandas()

# COMMAND ----------

# fit the model to historical data
model.fit(history_pd)

# COMMAND ----------

future_pd = model.make_future_dataframe(
    periods=90,
    freq='d',
    include_history=True
)

# predict over the dataset
forecast_pd = model.predict(future_pd)

# COMMAND ----------

predict_fig = model.plot(forecast_pd, xlabel='date', ylabel='impacts')
display(predict_fig)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(osha_id),model,to_date(DateCompleted) from na_pdc_powerfleet.checklist where severity ='Critical'
# MAGIC group by model,to_date(DateCompleted)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(impact_id),model,to_date(impact_date) from na_pdc_powerfleet.impact where severity not like '0%'
# MAGIC group by model,to_date(impact_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from na_pdc_powerfleet.checklist where severity <>'Normal'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from na_pdc_powerfleet.impact

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from
# MAGIC (select distinct
# MAGIC   Avg_sleep_time,
# MAGIC   --o.display_name,
# MAGIC   --o.user_id,
# MAGIC   o.seniority,
# MAGIC   o.unit_dept_cd,
# MAGIC   --v.vehicle,
# MAGIC   --v.model,
# MAGIC   --substring(v.model, -4),
# MAGIC   year(current_date) - substring(v.model, -4) as age_of_vehicle,
# MAGIC  u.Avg_activity_time,
# MAGIC  --u.up_case_last_nm,
# MAGIC  --u.up_case_first_nm,
# MAGIC  u.Avg_powerup_hrs,
# MAGIC  u.Avg_login_hrs,
# MAGIC  u.Avg_motion_hrs,
# MAGIC  u.Avg_engoff_hrs,
# MAGIC   --i.shift_id,
# MAGIC     --i.status_pre_impact,
# MAGIC     --i.status_at_impact,
# MAGIC     --i.status_post_impact,
# MAGIC     --i.max_g_impact,
# MAGIC     --i.max_duration,
# MAGIC     --i.impact_ratio,
# MAGIC     case when i.Severity_Code is NULL then 0 else 1 end as Impact_Flag
# MAGIC     --i.impact_level,
# MAGIC     --i.Mins_to_Logoff,
# MAGIC     --i.site,
# MAGIC     --i.MIL_Threshold,
# MAGIC     --i.Max_Speed
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       avg(sleep_time_mins) Avg_sleep_time,
# MAGIC       upper(Last_Name) up_case_last_nm,
# MAGIC       upper(First_Name) up_case_first_nm,
# MAGIC       vehicle
# MAGIC     from
# MAGIC       na_pdc_powerfleet.sleeper
# MAGIC     group by
# MAGIC       upper(Last_Name),
# MAGIC       upper(First_Name),
# MAGIC       vehicle
# MAGIC     order by
# MAGIC       avg(sleep_time_mins) desc
# MAGIC   ) s
# MAGIC   join na_pdc_powerfleet.operator o on o.UP_CASE_LAST_NM = s.up_case_last_nm
# MAGIC   and o.UP_CASE_FRST_NM = s.up_case_first_nm
# MAGIC   join na_pdc_powerfleet.vehicle v on s.Vehicle = v.Vehicle
# MAGIC   join (
# MAGIC         select
# MAGIC           avg(activity_hrs) Avg_activity_time,
# MAGIC           upper(Last_Name) up_case_last_nm,
# MAGIC           upper(First_Name) up_case_first_nm,
# MAGIC           vehicle,
# MAGIC           avg(powerup_hrs) Avg_powerup_hrs,
# MAGIC           avg(login_hrs) Avg_login_hrs,
# MAGIC           avg(motion_hrs) Avg_motion_hrs,
# MAGIC           avg(Eng_Off_hrs) Avg_engoff_hrs
# MAGIC         from
# MAGIC           na_pdc_powerfleet.usage
# MAGIC         group by
# MAGIC           upper(Last_Name),
# MAGIC           upper(First_Name),
# MAGIC           vehicle
# MAGIC         order by
# MAGIC           avg(activity_hrs) desc
# MAGIC       )  u on u.vehicle = v.Vehicle and u.up_case_last_nm = o.UP_CASE_LAST_NM and u.up_case_first_nm = o.UP_CASE_FRST_NM
# MAGIC       left join (
# MAGIC         select
# MAGIC         shift_id,
# MAGIC         vehicle,
# MAGIC         upper(Last_Name) as up_case_last_nm ,
# MAGIC         upper(First_name) as up_case_first_nm,
# MAGIC         status_pre_impact,
# MAGIC         status_at_impact,
# MAGIC         status_post_impact,
# MAGIC         max_g_impact,
# MAGIC         max_duration,
# MAGIC         impact_ratio,
# MAGIC         Severity_Code,
# MAGIC         impact_level,
# MAGIC         Mins_to_Logoff,
# MAGIC         site,
# MAGIC         MIL_Threshold,
# MAGIC         Max_Speed
# MAGIC       from na_pdc_powerfleet.impact 
# MAGIC       order by vehicle
# MAGIC       ) i on i.vehicle = v.Vehicle and i.up_case_last_nm = o.UP_CASE_LAST_NM and i.up_case_first_nm = o.UP_CASE_FRST_NM
# MAGIC       where i.Severity_Code is null
# MAGIC       ) --406

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct
# MAGIC   Avg_sleep_time,
# MAGIC   k.avg_num_of_failed_checklist,
# MAGIC   --o.display_name,
# MAGIC   --o.user_id,
# MAGIC   o.seniority,
# MAGIC   o.unit_dept_cd,
# MAGIC   --v.vehicle,
# MAGIC   --v.model,
# MAGIC   --substring(v.model, -4),
# MAGIC   year(current_date) - substring(v.model, -4) as age_of_vehicle,
# MAGIC  u.Avg_activity_time,
# MAGIC  --u.up_case_last_nm,
# MAGIC  --u.up_case_first_nm,
# MAGIC  u.Avg_powerup_hrs,
# MAGIC  u.Avg_login_hrs,
# MAGIC  u.Avg_motion_hrs,
# MAGIC  u.Avg_engoff_hrs,
# MAGIC   --i.shift_id,
# MAGIC     --i.status_pre_impact,
# MAGIC     --i.status_at_impact,
# MAGIC     --i.status_post_impact,
# MAGIC     --i.max_g_impact,
# MAGIC     --i.max_duration,
# MAGIC     --i.impact_ratio,
# MAGIC     --i.Severity_Code,
# MAGIC     case when i.Severity_Code is NULL or i.Severity_Code=0 then 0 else 1 end as Impact_Flag
# MAGIC     --i.impact_level,
# MAGIC     --i.Mins_to_Logoff,
# MAGIC     --i.site,
# MAGIC     --i.MIL_Threshold,
# MAGIC     --i.Max_Speed
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       avg(sleep_time_mins) Avg_sleep_time,
# MAGIC       upper(Last_Name) up_case_last_nm,
# MAGIC       upper(First_Name) up_case_first_nm,
# MAGIC       vehicle
# MAGIC     from
# MAGIC       na_pdc_powerfleet.sleeper where opr_class <> 'Maintenance'
# MAGIC     group by
# MAGIC       upper(Last_Name),
# MAGIC       upper(First_Name),
# MAGIC       vehicle
# MAGIC     order by
# MAGIC       avg(sleep_time_mins) desc
# MAGIC   ) s
# MAGIC   join na_pdc_powerfleet.operator o on o.UP_CASE_LAST_NM = s.up_case_last_nm
# MAGIC   and o.UP_CASE_FRST_NM = s.up_case_first_nm
# MAGIC   join na_pdc_powerfleet.vehicle v on s.Vehicle = v.Vehicle and v.vehicle not like 'TT%'
# MAGIC   join (
# MAGIC         select
# MAGIC           avg(activity_hrs) Avg_activity_time,
# MAGIC           upper(Last_Name) up_case_last_nm,
# MAGIC           upper(First_Name) up_case_first_nm,
# MAGIC           vehicle,
# MAGIC           avg(powerup_hrs) Avg_powerup_hrs,
# MAGIC           avg(login_hrs) Avg_login_hrs,
# MAGIC           avg(motion_hrs) Avg_motion_hrs,
# MAGIC           avg(Eng_Off_hrs) Avg_engoff_hrs
# MAGIC         from
# MAGIC           na_pdc_powerfleet.usage
# MAGIC         group by
# MAGIC           upper(Last_Name),
# MAGIC           upper(First_Name),
# MAGIC           vehicle
# MAGIC         order by
# MAGIC           avg(activity_hrs) desc
# MAGIC       )  u on u.vehicle = v.Vehicle and u.up_case_last_nm = o.UP_CASE_LAST_NM and u.up_case_first_nm = o.UP_CASE_FRST_NM
# MAGIC       join (select ch.vehicle,sum(ch.sev_code)/(year(current_date) - substring(veh.model, -4)) as avg_num_of_failed_checklist,veh.model from na_pdc_powerfleet.checklist ch,na_pdc_powerfleet.vehicle veh
# MAGIC where ch.vehicle=veh.vehicle and ch.sev_code <>0
# MAGIC group by ch.vehicle,veh.model) k on k.vehicle=v.Vehicle
# MAGIC       left join (
# MAGIC         select
# MAGIC         shift_id,
# MAGIC         vehicle,
# MAGIC         upper(Last_Name) as up_case_last_nm ,
# MAGIC         upper(First_name) as up_case_first_nm,
# MAGIC         status_pre_impact,
# MAGIC         status_at_impact,
# MAGIC         status_post_impact,
# MAGIC         max_g_impact,
# MAGIC         max_duration,
# MAGIC         impact_ratio,
# MAGIC         Severity_Code,
# MAGIC         impact_level,
# MAGIC         Mins_to_Logoff,
# MAGIC         site,
# MAGIC         MIL_Threshold,
# MAGIC         Max_Speed
# MAGIC       from na_pdc_powerfleet.impact 
# MAGIC       order by vehicle
# MAGIC       ) i on i.vehicle = v.Vehicle and i.up_case_last_nm = o.UP_CASE_LAST_NM and i.up_case_first_nm = o.UP_CASE_FRST_NM
# MAGIC    

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn import preprocessing
import matplotlib.pyplot as plt 
plt.rc("font", size=14)
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import seaborn as sns
sns.set(style="white")
sns.set(style="whitegrid", color_codes=True)

# COMMAND ----------

df=spark.sql("""
select
  Avg_sleep_time,
  --o.display_name,
  o.user_id,
  o.seniority,
  o.unit_dept_cd,
  v.vehicle,
  v.model,
  --substring(v.model, -4),
  year(current_date) - substring(v.model, -4) as age_of_vehicle,
 u.Avg_activity_time,
 --u.up_case_last_nm,
 --u.up_case_first_nm,
 u.Avg_powerup_hrs,
 u.Avg_login_hrs,
 u.Avg_motion_hrs,
 u.Avg_engoff_hrs,
  i.shift_id,
    i.status_pre_impact,
    --i.status_at_impact,
    --i.status_post_impact,
    --i.max_g_impact,
    --i.max_duration,
    --i.impact_ratio,
    case when i.Severity_Code =0 then 'N' else 'Y' end as Impact_Flag,
    --i.impact_level,
    --i.Mins_to_Logoff,
    --i.site,
    --i.MIL_Threshold,
    i.Max_Speed
from
  (
    select
      avg(sleep_time_mins) Avg_sleep_time,
      upper(Last_Name) up_case_last_nm,
      upper(First_Name) up_case_first_nm,
      vehicle
    from
      na_pdc_powerfleet.sleeper
    group by
      upper(Last_Name),
      upper(First_Name),
      vehicle
    order by
      avg(sleep_time_mins) desc
  ) s
  join na_pdc_powerfleet.operator o on o.UP_CASE_LAST_NM = s.up_case_last_nm
  and o.UP_CASE_FRST_NM = s.up_case_first_nm
  join na_pdc_powerfleet.vehicle v on s.Vehicle = v.Vehicle
  join (
        select
          avg(activity_hrs) Avg_activity_time,
          upper(Last_Name) up_case_last_nm,
          upper(First_Name) up_case_first_nm,
          vehicle,
          avg(powerup_hrs) Avg_powerup_hrs,
          avg(login_hrs) Avg_login_hrs,
          avg(motion_hrs) Avg_motion_hrs,
          avg(Eng_Off_hrs) Avg_engoff_hrs
        from
          na_pdc_powerfleet.usage
        group by
          upper(Last_Name),
          upper(First_Name),
          vehicle
        order by
          avg(activity_hrs) desc
      )  u on u.vehicle = v.Vehicle and u.up_case_last_nm = o.UP_CASE_LAST_NM and u.up_case_first_nm = o.UP_CASE_FRST_NM
      join (
        select
        shift_id,
        vehicle,
        upper(Last_Name) as up_case_last_nm ,
        upper(First_name) as up_case_first_nm,
        status_pre_impact,
        status_at_impact,
        status_post_impact,
        max_g_impact,
        max_duration,
        impact_ratio,
        Severity_Code,
        impact_level,
        Mins_to_Logoff,
        site,
        MIL_Threshold,
        Max_Speed
      from na_pdc_powerfleet.impact 
      order by vehicle
      ) i on i.vehicle = v.Vehicle and u.up_case_last_nm = o.UP_CASE_LAST_NM and u.up_case_first_nm = o.UP_CASE_FRST_NM""")

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.toPandas()

# COMMAND ----------

df=df.dropna()

# COMMAND ----------

print(df.shape)
print(list(df.columns))

# COMMAND ----------

cat_vars=['seniority','vehicle','model','shift_id','status_pre_impact']
for var in cat_vars:
    cat_list='var'+'_'+var
    cat_list = pd.get_dummies(df[var], prefix=var)
    data1=df.join(cat_list)
    df=data1
cat_vars=['user_id','seniority','unit_dept_cd','vehicle','model','shift_id','status_pre_impact']
data_vars=df.columns.values.tolist()
to_keep=[i for i in data_vars if i not in cat_vars]

# COMMAND ----------

data_final=df[to_keep]
data_final.columns.values

# COMMAND ----------

X = data_final.loc[:, data_final.columns != 'Impact_Flag']
y = data_final.loc[:, data_final.columns == 'Impact_Flag']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
columns = X_train.columns

# COMMAND ----------

data_final_vars=data_final.columns.values.tolist()
y=['Impact_Flag']
X=[i for i in data_final_vars if i not in y]
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
rfe = RFE(logreg, 20)
rfe = rfe.fit(X_train, y_train.values.ravel())
print(rfe.support_)
print(rfe.ranking_)
