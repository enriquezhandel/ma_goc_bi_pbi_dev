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
from pyspark.sql.functions import split, udf, col, regexp_extract, lit, concat, regexp_replace, substring
import datetime
from pyspark import SparkContext
import glob
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf, col, regexp_extract, lit, concat, regexp_replace,unix_timestamp, from_unixtime
from pyspark.sql.types import ArrayType, StringType
import re
import glob
import pandas as pd 
from pyspark.sql.functions import substring
import datetime
from pyspark.sql.functions import count, col
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.datetime.now()
print(start_time, end="\n")
spark = SparkSession.builder.appName('Snapshot').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet('Files/normalized_aqm_log/*')
df.createOrReplaceTempView("df")
# df.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # SQL Transformation
aqm_sql ="""
SELECT DISTINCT
    provider
    ,source
    ,service
    ,count(Combined_key) OVER (PARTITION BY date,hour,source) as intance_per_hour
    ,hour
    ,date
FROM df
ORDER BY date DESC
"""
aqm_clear = sqlContext.sql(aqm_sql)
# aqm_clear.createOrReplaceTempView("aqm_clear")
# aqm_clear.cache()
# aqm_clear.printSchema()
aqm_finalDF = aqm_clear.select('provider','source','service','intance_per_hour','hour',from_unixtime(unix_timestamp('date', 'MM/dd/yyy')).alias('date'))
aqm_finalDF.createOrReplaceTempView("aqm_finalDF")
output = 'Files/aqmLogDIM/'
aqm_finalDF.write.mode("overwrite").parquet(output)
# aqm_finalDF.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

daily_log ="""
SELECT DISTINCT
    source
    ,TRIM(REPLACE(provider,"_","")) AS provider
    ,service
    ,CAST(SUM(intance_per_hour) OVER (PARTITION BY date, source, provider, service ORDER BY date,provider) AS INT) as instance_per_day
    ,date_format(CAST(date AS DATE), 'EEEE') AS day_name
    ,CAST(date AS DATE) AS date
FROM aqm_finalDF
ORDER BY date DESC
"""
daily_log = sqlContext.sql(daily_log)
daily_log.createOrReplaceTempView("daily_log")
# daily_log.printSchema()
# daily_log.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

previous_daily_log ="""
SELECT DISTINCT
    source
    ,date
    ,provider
    ,service
    ,day_name
    ,instance_per_day
    ,COALESCE(lag(instance_per_day,1) OVER (PARTITION BY day_name, source, provider, service ORDER BY date ASC),0) AS previous_instance_per_day
FROM daily_log
ORDER BY date ASC
"""
previous_daily_log = sqlContext.sql(previous_daily_log)
previous_daily_log.createOrReplaceTempView("previous_daily_log")
# previous_daily_log.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alertReview_log ="""
SELECT DISTINCT
    source
    ,date
    ,provider
    ,service
    ,day_name
    ,instance_per_day
    ,previous_instance_per_day
    ,COALESCE(ROUND((instance_per_day/previous_instance_per_day)*100, 2),0) AS percentage_difference
FROM previous_daily_log
ORDER BY date ASC
"""
alertReview_log = sqlContext.sql(alertReview_log)
alertReview_log.createOrReplaceTempView("alertReview_log")
# alertReview_log.show(10, False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alertClass_log ="""
SELECT DISTINCT
    source
    ,CAST(date AS DATE) AS date
    ,provider
    ,service
    ,day_name
    ,instance_per_day
    ,previous_instance_per_day
    ,percentage_difference
    ,CASE 
        WHEN percentage_difference = 0.0 THEN 'ALERT'
        WHEN percentage_difference < 50.0 THEN 'WARNING'
        ELSE 'GOOD'
    END AS outageMechanism
FROM alertReview_log
ORDER BY date ASC
"""
alertClass_log = sqlContext.sql(alertClass_log)
alertClass_log.createOrReplaceTempView("alertClass_log")
alertClass_log = alertClass_log.coalesce(100)
alertClass_log.cache()
output = 'Files/aqmAlertDIM/'
alertClass_log.write.mode("overwrite").parquet(output)
# alertClass_log.show(10,False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.datetime.now() - start_time
print(end_time, end="\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
