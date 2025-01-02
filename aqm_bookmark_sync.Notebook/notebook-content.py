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
!pip install boto3
!pip install datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import boto3
import os
# from dotenv import find_dotenv, load_dotenv
import glob
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bucket_aqm = 'ma-am-dev-tracelog-storage'
region_aqm = 'us-east-1'
access_key_aqm = 'AKIAQJRZBVTIAWWVR5OA'
secret_access_key_aqm = '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'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.datetime.now()
print(start_time, end="\n")
# S3 Resource
s3_resource = boto3.resource("s3",
                             region_name=region_aqm,
                             aws_access_key_id=access_key_aqm,
                             aws_secret_access_key=secret_access_key_aqm)

s3_bucket = s3_resource.Bucket(bucket_aqm)
files = s3_bucket.objects.all()
# print(files)
element_s3 = list()
x = 0
for i in files:
    # print(i)
    # print(i.key)
    host_log = i.key
    element_s3.append(host_log)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# """
# Bookmark review start's here"
# """
filePath = '/lakehouse/default/Files/aqm_logs_full/'
# print(element_s3)
list_items = glob.glob(f"{filePath}"+"*")
# print(list_items)
list_items = list_items.__str__().replace(f"{filePath}", "").replace("\\", "").replace("'", "").replace("[",
                                                                                                                "").replace(
    "]", "").replace(".txt","").replace(" ", "").split(",")
print(len(list_items))
print(len(element_s3))
# print(list_items)
# print(element_s3)
for s3 in element_s3:
    # print(s3)
    for item in list_items:
        # print(item)
        if item in element_s3:
            element_s3.remove(item)
        else:
            continue
print(len(element_s3))

# """
# END
# """


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Downloading only new files
out_path = filePath
for element in element_s3:
    print(element)
    s3_bucket.download_file(element, out_path+element)
end_time = datetime.datetime.now() - start_time
print(end_time, end="\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
