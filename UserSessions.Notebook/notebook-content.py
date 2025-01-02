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
# META       "default_lakehouse_workspace_id": "aedd58a1-3fc3-4d18-b48d-a8d0953254bf",
# META       "known_lakehouses": [
# META         {
# META           "id": "845ae521-8d6f-44d5-8a86-4eee09de35ea"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
!pip install pyspark
!pip install datetime
!pip install elasticsearch
!pip install urllib3

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
from urllib3 import disable_warnings
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import json
import os
"""
Initialize the Spark Session 
"""
from pyspark.sql.functions import *
from pyspark.sql.functions import explode
# Initialize Spark Session
spark = SparkSession.builder.appName('ESK_US').getOrCreate()
# sc = SparkContext.getOrCreate(sparkConf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_yesterday_start_end_unix_timestamps_milliseconds():
    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')

    # Get the start of yesterday with time set to 00:00:00
    start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # Get the end of yesterday by setting time to 23:59:59 for the same day
    end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Convert both to Unix timestamps in milliseconds
    start_of_day_unix_milliseconds = int(start_of_day.timestamp() * 1000)
    end_of_day_unix_milliseconds = int(end_of_day.timestamp() * 1000)

    return date_str, start_of_day_unix_milliseconds, end_of_day_unix_milliseconds


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_str, start_of_today, end_of_today = get_yesterday_start_end_unix_timestamps_milliseconds()
print(date_str)
try:
    os.mkdir("/lakehouse/default/Files/elk_userSessions/{}".format(date_str))
except:
    pass
try:
    os.mkdir("/lakehouse/default/Files/userActions/{}".format(date_str))
except:
    pass
try:
    os.mkdir("/lakehouse/default/Files/userSessions/{}".format(date_str))
except:
    pass


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.now()
print(start_time)

# Disabling TLS warning https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
disable_warnings()
host_url = 'https://512c95556ce84aebbc075ca3406705ae.eu-central-1.aws.cloud.es.io:443'
token = 'eENLWHc0NEJpOHFyc2lPeWk2Rlk6MTNpLUJ5UGpRV3lnc3VaMFBQTm4yQQ=='
client = Elasticsearch(
    host_url,
    api_key=token,
    verify_certs=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare to scroll search
scroll = '1m'  # keep the search context alive for this long
size = 100  # number of results per scroll batch
query = {
    "range": {
        "startTime": {
            "gte": "{}".format(start_of_today), 
            "lte": "{}".format(end_of_today),
        }
    }
}
# query = {"match_all": {}}
response = client.search(
    index="doc",
    scroll=scroll,
    size=size,
    body={"query": query})
# print(response)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the schema
schema = StructType([
    StructField("_scroll_id", StringType(), nullable=True),
    StructField("_shards", StructType([
        StructField("failed", LongType(), nullable=True),
        StructField("skipped", LongType(), nullable=True),
        StructField("successful", LongType(), nullable=True),
        StructField("total", LongType(), nullable=True)
    ]), nullable=True),
    StructField("hits", StructType([
        StructField("hits", ArrayType(StructType([
            StructField("_id", StringType(), nullable=True),
            StructField("_index", StringType(), nullable=True),
            StructField("_score", DoubleType(), nullable=True),
            StructField("_source", StructType([
                StructField("applicationType", StringType(), nullable=True),
                StructField("bounce", BooleanType(), nullable=True),
                StructField("browserFamily", StringType(), nullable=True),
                StructField("browserMajorVersion", StringType(), nullable=True),
                StructField("browserMonitorId", StringType(), nullable=True),
                StructField("browserMonitorName", StringType(), nullable=True),
                StructField("browserType", StringType(), nullable=True),
                StructField("city", StringType(), nullable=True),
                StructField("clientType", StringType(), nullable=True),
                StructField("connectionType", StringType(), nullable=True),
                StructField("continent", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
                StructField("dateProperties", ArrayType(StringType()), nullable=True),
                StructField("displayResolution", StringType(), nullable=True),
                StructField("doubleProperties", ArrayType(StringType()), nullable=True),
                StructField("duration", LongType(), nullable=True),
                StructField("endReason", StringType(), nullable=True),
                StructField("endTime", LongType(), nullable=True),
                StructField("errors", ArrayType(StringType()), nullable=True),
                StructField("events", ArrayType(StructType([
                    StructField("application", StringType(), nullable=True),
                    StructField("internalApplicationId", StringType(), nullable=True),
                    StructField("name", StringType(), nullable=True),
                    StructField("page", StringType(), nullable=True),
                    StructField("pageGroup", StringType(), nullable=True),
                    StructField("pageReferrer", StringType(), nullable=True),
                    StructField("pageReferrerGroup", StringType(), nullable=True),
                    StructField("startTime", LongType(), nullable=True),
                    StructField("type", StringType(), nullable=True)
                ])), nullable=True),
                StructField("hasCrash", BooleanType(), nullable=True),
                StructField("hasError", BooleanType(), nullable=True),
                StructField("hasSessionReplay", BooleanType(), nullable=True),
                StructField("internalUserId", StringType(), nullable=True),
                StructField("ip", StringType(), nullable=True),
                StructField("isp", StringType(), nullable=True),
                StructField("longProperties", ArrayType(StringType()), nullable=True),
                StructField("matchingConversionGoals", ArrayType(StringType()), nullable=True),
                StructField("matchingConversionGoalsCount", LongType(), nullable=True),
                StructField("newUser", BooleanType(), nullable=True),
                StructField("numberOfRageClicks", LongType(), nullable=True),
                StructField("numberOfRageTaps", LongType(), nullable=True),
                StructField("osFamily", StringType(), nullable=True),
                StructField("osVersion", StringType(), nullable=True),
                StructField("partNumber", LongType(), nullable=True),
                StructField("region", StringType(), nullable=True),
                StructField("screenHeight", LongType(), nullable=True),
                StructField("screenOrientation", StringType(), nullable=True),
                StructField("screenWidth", LongType(), nullable=True),
                StructField("startTime", LongType(), nullable=True),
                StructField("stringProperties", ArrayType(StructType([
                    StructField("applicationId", StringType(), nullable=True),
                    StructField("internalApplicationId", StringType(), nullable=True),
                    StructField("key", StringType(), nullable=True),
                    StructField("value", StringType(), nullable=True)
                ])), nullable=True),
                StructField("syntheticEvents", ArrayType(StructType([
                    StructField("errorCode", LongType(), nullable=True),
                    StructField("name", StringType(), nullable=True),
                    StructField("sequenceNumber", LongType(), nullable=True),
                    StructField("syntheticEventId", StringType(), nullable=True),
                    StructField("timestamp", LongType(), nullable=True),
                    StructField("type", StringType(), nullable=True)
                ])), nullable=True),
                StructField("tenantId", StringType(), nullable=True),
                StructField("totalErrorCount", LongType(), nullable=True),
                StructField("totalLicenseCreditCount", LongType(), nullable=True),
                StructField("userActionCount", LongType(), nullable=True),
                StructField("userActions", ArrayType(StructType([
                    StructField("apdexCategory", StringType(), nullable=True),
                    StructField("application", StringType(), nullable=True),
                    StructField("cdnBusyTime", LongType(), nullable=True),
                    StructField("cdnResources", LongType(), nullable=True),
                    StructField("cumulativeLayoutShift", DoubleType(), nullable=True),
                    StructField("customErrorCount", LongType(), nullable=True),
                    StructField("dateProperties", ArrayType(StringType()), nullable=True),
                    StructField("documentInteractiveTime", LongType(), nullable=True),
                    StructField("domCompleteTime", LongType(), nullable=True),
                    StructField("domContentLoadedTime", LongType(), nullable=True),
                    StructField("domain", StringType(), nullable=True),
                    StructField("doubleProperties", ArrayType(StringType()), nullable=True),
                    StructField("duration", LongType(), nullable=True),
                    StructField("endTime", LongType(), nullable=True),
                    StructField("firstInputDelay", LongType(), nullable=True),
                    StructField("firstPartyBusyTime", LongType(), nullable=True),
                    StructField("firstPartyResources", LongType(), nullable=True),
                    StructField("frontendTime", LongType(), nullable=True),
                    StructField("internalApplicationId", StringType(), nullable=True),
                    StructField("internalKeyUserActionId", StringType(), nullable=True),
                    StructField("javascriptErrorCount", LongType(), nullable=True),
                    StructField("keyUserAction", BooleanType(), nullable=True),
                    StructField("largestContentfulPaint", LongType(), nullable=True),
                    StructField("loadEventEnd", LongType(), nullable=True),
                    StructField("loadEventStart", LongType(), nullable=True),
                    StructField("longProperties", ArrayType(StructType([
                        StructField("key", StringType(), nullable=True),
                        StructField("value", LongType(), nullable=True)
                    ])), nullable=True),
                    StructField("matchingConversionGoals", ArrayType(StringType()), nullable=True),
                    StructField("name", StringType(), nullable=True),
                    StructField("navigationStart", LongType(), nullable=True),
                    StructField("networkTime", LongType(), nullable=True),
                    StructField("requestErrorCount", LongType(), nullable=True),
                    StructField("requestStart", LongType(), nullable=True),
                    StructField("responseEnd", LongType(), nullable=True),
                    StructField("responseStart", LongType(), nullable=True),
                    StructField("serverTime", LongType(), nullable=True),
                    StructField("speedIndex", LongType(), nullable=True),
                    StructField("startTime", LongType(), nullable=True),
                    StructField("stringProperties", ArrayType(StructType([
                        StructField("key", StringType(), nullable=True),
                        StructField("value", StringType(), nullable=True)
                    ])), nullable=True),
                    StructField("syntheticEvent", StringType(), nullable=True),
                    StructField("syntheticEventId", StringType(), nullable=True),
                    StructField("targetUrl", StringType(), nullable=True),
                    StructField("thirdPartyBusyTime", LongType(), nullable=True),
                    StructField("thirdPartyResources", LongType(), nullable=True),
                    StructField("totalBlockingTime", StringType(), nullable=True),
                    StructField("type", StringType(), nullable=True),
                    StructField("userActionPropertyCount", LongType(), nullable=True),
                    StructField("visuallyCompleteTime", LongType(), nullable=True)
                ])), nullable=True),
                StructField("userExperienceScore", StringType(), nullable=True),
                StructField("userId", StringType(), nullable=True),
                StructField("userSessionId", StringType(), nullable=True),
                StructField("userType", StringType(), nullable=True)
            ]))
        ])), nullable=True),
        StructField("max_score", DoubleType(), nullable=True),
        StructField("total", StructType([
            StructField("relation", StringType(), nullable=True),
            StructField("value", LongType(), nullable=True)
        ]), nullable=True)
    ]), nullable=True),
    StructField("timed_out", BooleanType(), nullable=True),
    StructField("took", LongType(), nullable=True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_df(nested_df):
    """
    The aim of this function is to flatten the columns for extraction details.
    """
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# While there are more documents to fetch from the scroll
x = 0
while len(response['hits']['hits']):
    # Fetch the next scroll batch
    response = client.scroll(scroll_id=response['_scroll_id'], scroll=scroll)
    # print(response)
    document_json = json.loads(json.dumps(response.body))
    # print(document_json, end= "\n")
    # print(x, end= "\n")
    x = x + 1
    json_name = f'/lakehouse/default/Files/elk_userSessions/{date_str}/data_{x}.json'
    with open(json_name, 'w') as f:
        # print(json_name)
        json.dump(document_json, f)
        # esk_DF = spark.read.json('Files/elk_userSessions/data_{}.json'.format(x), schema)
        # esk_DF.show(10)
        # json_name = f'Files/elk_userSessions/data_{x}.json'
        # flatteningSchema(json_name,schema)
        
end_time = datetime.now() - start_time
print(end_time) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_multiline = spark.read.option("multiline", "true").json(f'Files/elk_userSessions/{date_str}')
# df_multiline.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

esk_flat = df_multiline.select("hits.hits")
# esk_flat.show(10,False)
exploded_df = esk_flat.select(explode(col("hits")).alias("hit"))
# # exploded_df.show(10,False)
userSession_exp = flatten_df(exploded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# USER SESSIONS SCHEMA FLATTEN
userSessionFlat = userSession_exp.select(
                                    col("hit__id").alias("elastic_id")
                                    ,col("hit__source.userSessionId").alias("userSessionId")
                                    ,col("hit__source.userId").alias("userId")
                                    ,col("hit__source.stringProperties").cast("string").alias("stringProperties")
                                    ,col("hit__source.startTime").alias("startTime")
                                    ,from_unixtime(col("hit__source.startTime") / 1000).alias("startTime_utc")
                                    ,col("hit__source.endTime").alias("endTime")
                                    ,from_unixtime(col("hit__source.endTime") / 1000).alias("endTime_utc")
                                    ,col("hit__source.ip").alias("ip")
                                    ,col("hit__source.duration").alias("duration")
                                    ,col("hit__source.browserFamily").alias("browserFamily")
                                    ,col("hit__source.browserMajorVersion").alias("browserMajorVersion")
                                    ,col("hit__source.continent").alias("continent")
                                    ,col("hit__source.country").alias("country")
                                    ,col("hit__source.city").alias("city")
                                    ,col("hit__source.hasCrash").alias("hasCrash")
                                    ,col("hit__source.hasError").alias("hasError")
                                    ,col("hit__source.numberOfRageClicks").alias("numberOfRageClicks")
                                    ,col("hit__source.numberOfRageTaps").alias("numberOfRageTaps")
                                    ,col("hit__source.totalErrorCount").alias("totalErrorCount")
                                    ,col("hit__source.userActionCount").alias("userActionCount")
                                    ,col("hit__source.userExperienceScore").alias("userExperienceScore")
                                    ,col("hit__source.userType").alias("userType")
                                    )
# userSessionFlat.show(10,False)
outputUS = "abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Files/userSessions/{}/".format(date_str)
# userSessionFlat.write.mode('append').saveAsTable("abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Tables/usersessions")
userSessionFlat.write.mode("overwrite").parquet(outputUS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
START'S ACTIONS FLATTENING
"""
# USER ACTIONS Flatten 
userActionFlat = userSession_exp.select("hit__source.userActions",col("hit__source.userSessionId").alias("userSessionId"),col("hit__id").alias("elastic_id"))
# userActionFlat.printSchema()
# userActionFlat.show()
userActionFlat = userActionFlat.select(explode(col("userActions")).alias("userActions"),"userSessionId","elastic_id")
# userActionFlat.printSchema()
# userActionFlat.show()
userActionFlat = userActionFlat.select(
                            col("userSessionId").alias("userSessionId")
                            ,col("elastic_id").alias("elastic_id")
                            ,col("userActions.apdexCategory").alias("apdexCategory")
                            ,col("userActions.application").alias("application")
                            ,col("userActions.cdnBusyTime").alias("cdnBusyTime")
                            ,col("userActions.cdnResources").alias("cdnResources")
                            ,col("userActions.cumulativeLayoutShift").alias("cumulativeLayoutShift")
                            ,col("userActions.documentInteractiveTime").alias("documentInteractiveTime")
                            ,col("userActions.domCompleteTime").alias("domCompleteTime")
                            ,col("userActions.domContentLoadedTime").alias("domContentLoadedTime")
                            ,col("userActions.duration").alias("duration")
                            ,col("userActions.endTime").alias("endTime")
                            ,from_unixtime(col("userActions.endTime") / 1000).alias("endTime_utc")
                            ,col("userActions.firstInputDelay").alias("firstInputDelay")
                            ,col("userActions.firstPartyBusyTime").alias("firstPartyBusyTime")
                            ,col("userActions.firstPartyResources").alias("firstPartyResources")
                            ,col("userActions.frontendTime").alias("frontendTime")
                            ,col("userActions.internalApplicationId").alias("internalApplicationId")
                            ,col("userActions.internalKeyUserActionId").alias("internalKeyUserActionId")
                            ,col("userActions.javascriptErrorCount").alias("javascriptErrorCount")
                            ,col("userActions.keyUserAction").alias("keyUserAction")
                            ,col("userActions.largestContentfulPaint").alias("largestContentfulPaint")
                            ,col("userActions.loadEventEnd").cast("string").alias("loadEventEnd")
                            ,col("userActions.loadEventStart").cast("string").alias("loadEventStart")
                            ,col("userActions.longProperties").cast("string").alias("longProperties")
                            ,col("userActions.name").alias("name")
                            ,col("userActions.navigationStart").alias("navigationStart")
                            ,col("userActions.networkTime").alias("networkTime")
                            ,col("userActions.requestErrorCount").alias("requestErrorCount")
                            ,col("userActions.requestStart").alias("requestStart")
                            ,col("userActions.responseEnd").alias("responseEnd")
                            ,col("userActions.responseStart").alias("responseStart")
                            ,col("userActions.serverTime").alias("serverTime")
                            ,col("userActions.speedIndex").alias("speedIndex")
                            ,col("userActions.startTime").alias("startTime")
                            ,from_unixtime(col("userActions.startTime") / 1000).alias("startTime_utc")
                            ,col("userActions.stringProperties").alias("stringProperties")
                            ,col("userActions.targetUrl").alias("targetUrl")
                            ,col("userActions.thirdPartyBusyTime").alias("thirdPartyBusyTime")
                            ,col("userActions.thirdPartyResources").alias("thirdPartyResources")
                            ,col("userActions.type").alias("type")
                            ,col("userActions.visuallyCompleteTime").alias("visuallyCompleteTime")
                            )
userActionFlatClean = userActionFlat.select(
                                             col("userSessionId").alias("userSessionId")
                                            ,col("elastic_id").alias("elastic_id")
                                            ,col("apdexCategory").alias("apdexCategory")
                                            ,col("application").alias("application")
                                            ,col("cdnBusyTime").alias("cdnBusyTime")
                                            ,col("cdnResources").alias("cdnResources")
                                            ,col("cumulativeLayoutShift").alias("cumulativeLayoutShift")
                                            ,col("documentInteractiveTime").alias("documentInteractiveTime")
                                            ,col("domCompleteTime").alias("domCompleteTime")
                                            ,col("domContentLoadedTime").alias("domContentLoadedTime")
                                            ,col("duration").alias("duration")
                                            ,col("endTime").alias("endTime")
                                            ,from_unixtime(col("endTime") / 1000).alias("endTime_utc")
                                            ,col("firstInputDelay").alias("firstInputDelay")
                                            ,col("firstPartyBusyTime").alias("firstPartyBusyTime")
                                            ,col("firstPartyResources").alias("firstPartyResources")
                                            ,col("frontendTime").alias("frontendTime")
                                            ,col("internalApplicationId").alias("internalApplicationId")
                                            ,col("internalKeyUserActionId").alias("internalKeyUserActionId")
                                            ,col("javascriptErrorCount").alias("javascriptErrorCount")
                                            ,col("keyUserAction").alias("keyUserAction")
                                            ,col("largestContentfulPaint").alias("largestContentfulPaint")
                                            ,col("loadEventEnd").cast("string").alias("loadEventEnd")
                                            ,col("loadEventStart").cast("string").alias("loadEventStart")
                                            ,col("longProperties").cast("string").alias("longProperties")
                                            ,col("name").alias("name")
                                            ,col("navigationStart").alias("navigationStart")
                                            ,col("networkTime").alias("networkTime")
                                            ,col("requestErrorCount").alias("requestErrorCount")
                                            ,col("requestStart").alias("requestStart")
                                            ,col("responseEnd").alias("responseEnd")
                                            ,col("responseStart").alias("responseStart")
                                            ,col("serverTime").alias("serverTime")
                                            ,col("speedIndex").alias("speedIndex")
                                            ,col("startTime").alias("startTime")
                                            ,from_unixtime(col("startTime") / 1000).alias("startTime_utc")
                                            ,col("stringProperties").cast('string').alias("stringProperties")
                                            ,col("targetUrl").alias("targetUrl")
                                            ,col("thirdPartyBusyTime").alias("thirdPartyBusyTime")
                                            ,col("thirdPartyResources").alias("thirdPartyResources")
                                            ,col("type").alias("type")
                                            ,col("visuallyCompleteTime").alias("visuallyCompleteTime")
                                            ,when(userActionFlat["stringProperties"].cast('string').like("%threadname%"), expr("filter(stringProperties, x -> x.key = 'threadname')[0].value")).otherwise("no_threadname").alias("threadname")
                                            ,when(userActionFlat["stringProperties"].cast('string').like("%reportbooksection%"), expr("filter(stringProperties, x -> x.key = 'reportbooksection')[0].value")).otherwise("no_reportbooksection").alias("reportbooksection")
                                            ,when(userActionFlat["stringProperties"].cast('string').like("%searchby%"), expr("filter(stringProperties, x -> x.key = 'searchby')[0].value")).otherwise("no_searchby").alias("searchby")
                                            ,when(userActionFlat["stringProperties"].cast('string').like("%accountid%"), expr("filter(stringProperties, x -> x.key = 'accountid')[0].value")).otherwise("no_accountid").alias("accountid")
                                            ,when(userActionFlat["stringProperties"].cast('string').like("%accountname%"), expr("filter(stringProperties, x -> x.key = 'accountname')[0].value")).otherwise("no_accountname").alias("accountname")
                                            )
outputUA = "abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Files/userActions/{}".format(date_str)
# userActionFlatClean.write.mode('append').saveAsTable("abfss://MA_GOC_DEV@onelake.dfs.fabric.microsoft.com/gocDEVBI.Lakehouse/Tables/useractions")
userActionFlatClean.write.mode("overwrite").parquet(outputUA)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

userSessionFlat.createOrReplaceTempView("userSessionFlat")
insert_sql = """
INSERT INTO gocDEVBI.usersessions
SELECT * FROM userSessionFlat
"""
spark.sql(insert_sql)

userActionFlatClean.createOrReplaceTempView("userActionFlatClean")
insert_sql = """
INSERT INTO gocDEVBI.useractions
SELECT * FROM userActionFlatClean
"""
spark.sql(insert_sql)
print("Sucess Appending of data!!")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
