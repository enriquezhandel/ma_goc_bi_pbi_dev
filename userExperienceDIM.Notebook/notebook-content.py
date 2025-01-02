# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "845ae521-8d6f-44d5-8a86-4eee09de35ea",
# META       "default_lakehouse_name": "gocDEVBI",
# META       "default_lakehouse_workspace_id": "aedd58a1-3fc3-4d18-b48d-a8d0953254bf"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
!pip install pyspark
!pip install datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.functions import col, explode, from_unixtime, when, regexp_extract, cast,expr
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, ArrayType, DoubleType
import datetime
# from pyspark import SparkContext
from datetime import datetime, timedelta
import json
import os
"""
Initialize the Spark Session 
"""
from pyspark.sql.functions import *
from pyspark.sql.functions import explode
# Initialize Spark Session
spark = SparkSession.builder.appName('DIM_US').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sessions = spark.read.parquet('Files/userSessions/*')
# df_sessions.show(10, False)
df_actions = spark.read.parquet('Files/userActions/*')
# df_actions.show(10, False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sessions.createOrReplaceTempView("df_sessions")
df_actions.createOrReplaceTempView("df_actions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sessionsClean ="""
WITH accountClean AS(
SELECT DISTINCT 
	userSessionId
, accountname
FROM df_actions
WHERE accountname != 'no_accountname'),

account_df AS(
SELECT 
userSessionId
, accountname
, COUNT(userSessionId) AS idControl
FROM accountClean
GROUP BY userSessionId, accountname
HAVING COUNT(userSessionId) = 1),

application_df AS(
SELECT DISTINCT
  application
 ,elastic_id
FROM df_actions
)

SELECT DISTINCT
		sessions.elastic_id
		,sessions.userSessionId
		,CASE WHEN TRIM(sessions.userId) = 'no_user' OR TRIM(sessions.userId) IS NULL THEN 'no_user' ELSE sessions.userId END AS userId
		,CASE WHEN TRIM(sessions.country) = 'no_country' OR TRIM(sessions.country) IS NULL THEN 'no_country' ELSE sessions.country END AS country
		,sessions.userActionCount
		,sessions.numberOfRageClicks
		,sessions.duration as session_duration
		,account.accountname
		,sessions.browserFamily
		,sessions.userExperienceScore as session_userExperienceScore
		,sessions.browserMajorVersion
		,app.application
		,sessions.userType
		,CAST(sessions.startTime_utc AS DATE) AS snapshot_date
FROM df_sessions sessions
LEFT JOIN account_df account
				ON sessions.userSessionId = account.userSessionId
		LEFT JOIN application_df app
				ON sessions.elastic_id = app.elastic_id
WHERE sessions.userType = 'REAL_USER'
AND app.application IN ('Compliance Catalyst v2 (AWS)', 'Orbis (AWS)')
"""
sessionsClean = spark.sql(sessionsClean)
sessionsClean.createOrReplaceTempView("sessionsClean")
# sessionsClean.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sessionsDim="""
SELECT  
    COUNT(userSessionId) AS userSessionIdTotal,
    country,
    SUM(userActionCount) AS userActionCount,
    SUM(numberOfRageClicks) AS numberOfRageClicks,
    accountname,
    session_userExperienceScore,
    application,
    snapshot_date
FROM sessionsClean
GROUP BY 			
    country,
    accountname,
    session_userExperienceScore,
    application,
    snapshot_date
"""
sessionsDim = spark.sql(sessionsDim)
# sessionsDim.show(10, False)
output = 'abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Files/sessionsDim/'
sessionsDim.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actionsClean ="""
WITH accountClean AS(
SELECT DISTINCT 
	userSessionId
, accountname
FROM df_actions
WHERE accountname != 'no_accountname'),

account_df AS(
SELECT 
userSessionId
, accountname
, COUNT(userSessionId) AS idControl
FROM accountClean
GROUP BY userSessionId, accountname
HAVING COUNT(userSessionId) = 1)

SELECT  
    ua.elastic_id,
    ua.userSessionId,
    ua.startTime_utc,
    hour(ua.startTime_utc) AS startTime_utc_hour,
    CASE 
        WHEN ua.apdexCategory = 'TOLERATING' THEN 'TOLERATED' ELSE ua.apdexCategory END AS apdexCategory,
    ua.application,
    ua.duration,
    cast(CAST(ua.duration AS FLOAT)/1000 as float) as duration_seconds,
    ua.name,
    ua.targetUrl,
    ua.type,
    ua.threadname,
    ua.reportbooksection,
    ua.searchby,
    ua.accountid,
    ad.accountname,
    us.userId,
    us.country,
    CAST(us.startTime_utc AS DATE) AS snapshot_date
FROM df_actions ua
  LEFT JOIN df_sessions us
      ON ua.elastic_id = us.elastic_id
  LEFT JOIN account_df ad
      ON ua.userSessionId = ad.userSessionId
WHERE ua.application IN ('Compliance Catalyst v2 (AWS)','Orbis (AWS)')
GROUP BY
			ua.elastic_id,
			ua.userSessionId,
			ua.startTime_utc,
            hour(ua.startTime_utc),
			CASE 
			    WHEN ua.apdexCategory = 'TOLERATING' THEN 'TOLERATED' ELSE ua.apdexCategory END,
			ua.application,
			ua.duration,
			ua.name,
			ua.targetUrl,
			ua.type,
			ua.threadname,
			ua.reportbooksection,
			ua.searchby,
			ua.accountid,
			ad.accountname,
			us.userId,
			us.country,
			CAST(us.startTime_utc AS DATE)
"""
actionsClean = spark.sql(actionsClean)
actionsClean.createOrReplaceTempView("actionsClean")
# actionsClean.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actionsDim="""
SELECT 
	COUNT(userSessionId) AS userSessionIdTotal,
	startTime_utc_hour,
	apdexCategory,
	application,
	duration_seconds,
	threadname,
	accountname,
	country,
	snapshot_date
FROM actionsClean
GROUP BY 
	startTime_utc_hour,
	apdexCategory,
	application,
	duration_seconds,
	threadname,
	accountname,
	country,
	snapshot_date
"""
actionsDim = spark.sql(actionsDim)
actionsDim.createOrReplaceTempView("actionsDim")
# actionsDim.show(10, False)
output = 'abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Files/actionsDim/'
actionsDim.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

comCatOnly ="""
SELECT * FROM actionsClean WHERE application = 'Compliance Catalyst v2 (AWS)'
"""
comCatOnly = spark.sql(comCatOnly)
comCatOnly.createOrReplaceTempView("comCatOnly")
# actionsDim.show(10, False)
output = 'abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Files/comCatOnly/'
comCatOnly.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
