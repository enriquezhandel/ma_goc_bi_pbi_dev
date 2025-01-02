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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf, col, regexp_extract, lit, concat, regexp_replace
from pyspark.sql.types import ArrayType, StringType
import glob
import pandas as pd 
from pyspark.sql.functions import substring
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.datetime.now()
print(start_time, end="\n")
spark = SparkSession.builder.appName('NormalizeData').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of items bookmark only
filePathSource = '/lakehouse/default/Files/aqm_logs_full/'
list_items_source = glob.glob(f"{filePathSource}"+"*")
list_items_source = list_items_source.__str__().replace(f"{filePathSource}", "").replace("\\", "").replace("'",
                                                                                                             "").replace(
    "[", "").replace("]", "").replace(" ","").split(",")
print(len(list_items_source))

filePathOutput = "/lakehouse/default/Files/normalized_aqm_log/"
list_items_normalized = glob.glob(f"{filePathOutput}"+"*")
list_items_normalized = list_items_normalized.__str__().replace(f"{filePathOutput}", "").replace("\\", "").replace(
    "'", "").replace("[", "").replace("]", "").replace(".parquet", "").replace(" ","").split(",")
print(len(list_items_normalized))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Normalization process
for source in list_items_source:
    # print(i)
    for item in list_items_normalized:
        # print(k)
        if item in list_items_source:
            list_items_source.remove(item)
        else:
            continue
print(len(list_items_source))

fileTXTRead = "Files/aqm_logs_full/"
for file in list_items_source[::-1]:    
    try:
        # print(file)
        file = file.replace('\\', '/').replace(f"{fileTXTRead}","")
        print(file)
        # Load the data
        df = spark.read.text(f"{fileTXTRead}"+f"{file}")
        # df.show(10,False)

        # Extracting dates 
        date = df.collect()[0]
        date = date.value
        # print(date, end="\n")
        df = df.withColumn("date", lit(date))

        # Filter logs with 'Published'
        df = df.filter("value like '%Published%'")

        # # Split the logs
        df = df.withColumn('Time', split(df['value'], ' ').getItem(0))
        pattern = "\\[(.*?)\\]"
        df = df.withColumn("Source", regexp_extract(df['value'], pattern, 1))
        df = df.withColumn('Provider', split(df['value'], 'Published').getItem(1))
        df = df.withColumn('Hour', split(df['Time'], ':').getItem(0))

        # Concatenating columns
        df = df.select("value","Source","Provider","Time","Date","Hour",concat(df.Hour,df.Source).alias("Combined_key"))
        # df.show(10,False)

        # Cleaning Source, Provider & Service
        df = df.withColumn('Source',regexp_replace('Source','PS/.',''))
        df = df.withColumn("Provider", regexp_replace(col("Provider"), '[0-9]', ''))
        df = df.withColumn("Provider", substring(df["Provider"], 0, 16))
        df = df.withColumn("Service", substring(df["Provider"], 9, 16))
        df = df.withColumn("Provider", substring(df["Provider"], 0, 9))
        # df.show(10,False)
        
        # Save the dataframe to a parquet file
        output = "Files/normalized_aqm_log/{}".format(file)
        # print(output)
        df.write.mode("overwrite").parquet(output)
    except:
        print("Unable to process {}".format(file))
        pass
end_time = datetime.datetime.now() - start_time
print(end_time, end="\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
