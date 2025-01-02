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
!pip install pandas
!pip install regex

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession, SQLContext
import datetime
from pyspark import SparkContext
import glob
import pyspark
import re
import glob
import pandas as pd 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfAccount = spark.sql("""
WITH accountControl AS(
SELECT  
	 PrimaryAccountId
	,AccountHierarchyId
	,len(AccountHierarchyId) as controlParent
	,MIN(len(AccountHierarchyId)) OVER (PARTITION BY PrimaryAccountId) AS hierachyControl
	,Active
	,AccountName
	,CompanyName
FROM gocDEVBI.ext_FinUserAccount)

SELECT DISTINCT
 	 PrimaryAccountId
	,AccountHierarchyId
	,controlParent
	,hierachyControl
	,Active
	,AccountName
	,CompanyName
FROM accountControl
WHERE controlParent = hierachyControl
""")
dfAccount.createOrReplaceTempView("dfAccount")
dfAccount.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfAlerts = spark.sql("""
SELECT 
    SharingId,
    AlertCriteria,
    Count,
    AlertDate,
    snapshotdate 
FROM gocDEVBI.ext_CatalystMVCAlerts
""")
dfAlerts.createOrReplaceTempView("dfAlerts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfRiskAssessments = spark.sql("""
SELECT 
    SharingId,
    snapshot_date,
    ProductId,
    nr_tasks,
    avg_execution_seconds,
    max_execution_seconds,
    average_wait_seconds,
    max_wait_seconds
FROM gocDEVBI.ext_CatalystMVCRisksAssessment
""")
dfRiskAssessments.createOrReplaceTempView("dfRiskAssessments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfClass = spark.read.format("csv").option("header","true").load("Files/AlertClass/enums_output.csv")
dfClass.createOrReplaceTempView("dfClass")
# dfClass.show(10,False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alertsClean = """
SELECT 
    dfAlerts.SharingId,
    CASE
        WHEN dfAccount.AccountName IS NULL THEN "no_account"
        ELSE dfAccount.AccountName
    END AS AccountName,
    dfAlerts.AlertCriteria,
    CASE
        WHEN dfClass.Enum_Name IS NULL THEN dfAlerts.AlertCriteria
        ELSE  dfClass.Enum_Name
    END AS alertName,
    dfAlerts.Count,
    dfAlerts.AlertDate,
    hour(dfAlerts.AlertDate) AS hourAlert,
    dfAlerts.snapshotdate,
    date_sub(trunc(add_months(dfAlerts.snapshotdate, 1), 'MONTH'), 1) AS eom_snapshotdate 
FROM dfAlerts
    LEFT JOIN dfAccount
        ON dfAlerts.SharingId = dfAccount.PrimaryAccountId
    LEFT JOIN dfClass
        ON dfAlerts.AlertCriteria = dfClass.Value
"""
alertsClean = sqlContext.sql(alertsClean)
alertsClean.createOrReplaceTempView("alertsClean")
# alertsClean.show(10,False)
output = 'Files/comCatV2AlertsDIM/'
alertsClean.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

risksClean = """
SELECT 
    dfRiskAssessments.SharingId,
    CASE
        WHEN dfAccount.AccountName IS NULL THEN "no_account"
        ELSE dfAccount.AccountName
    END AS AccountName,
    dfRiskAssessments.snapshot_date,
    date_sub(trunc(add_months(dfRiskAssessments.snapshot_date, 1), 'MONTH'), 1) AS eom_snapshotdate,
    dfRiskAssessments.ProductId,
    dfRiskAssessments.nr_tasks,
    dfRiskAssessments.avg_execution_seconds,
    dfRiskAssessments.max_execution_seconds,
    dfRiskAssessments.average_wait_seconds,
    dfRiskAssessments.max_wait_seconds,
	(dfRiskAssessments.average_wait_seconds + dfRiskAssessments.avg_execution_seconds) AS totalDuration 
FROM dfRiskAssessments
    LEFT JOIN dfAccount
        ON dfRiskAssessments.SharingId = dfAccount.PrimaryAccountId
GROUP BY 
    dfRiskAssessments.SharingId,
    CASE
        WHEN dfAccount.AccountName IS NULL THEN "no_account"
        ELSE dfAccount.AccountName
    END,
    dfRiskAssessments.snapshot_date,
    date_sub(trunc(add_months(dfRiskAssessments.snapshot_date, 1), 'MONTH'), 1),
    dfRiskAssessments.ProductId,
    dfRiskAssessments.nr_tasks,
    dfRiskAssessments.avg_execution_seconds,
    dfRiskAssessments.max_execution_seconds,
    dfRiskAssessments.average_wait_seconds,
    dfRiskAssessments.max_wait_seconds
"""
risksClean = sqlContext.sql(risksClean)
risksClean.createOrReplaceTempView("risksClean")
# alertsClean.show(10,False)
output = 'Files/comCatV2RisksDIM/'
risksClean.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
