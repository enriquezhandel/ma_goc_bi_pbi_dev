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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
SELECT DISTINCT
			accountname, 
			COUNT(elastic_id) AS total_actions,
			AVG(duration_seconds) as avg_duration_seconds,
			threadname,
			apdexCategory,
			snapshot_date
FROM gocDEVBI.comCatOnly
WHERE threadname <>  'no_threadname'
AND lower(accountname) IN ('amex compliance gms'
                        ,'manuchar'
                        ,'nets merchant services (original)'
                        ,'permodalan nasional berhad'
                        ,'stadtwerke muenchen gmbh'
                        ,'ebara corporation'
                        ,'chanel new'
                        ,'st engineering management services pte ltd'
                        ,'asian development bank ph'
                        ,'xe.com'
                        ,'ssab ab'
                        ,'eight advisory'
                        ,'forvis mazars'
                        ,'santos ltd'
                        ,'f. hoffmann - la roche ltd.'
                        ,'hitachi, ltd.'
                        ,'euronext london limited'
                        ,'airbus cc2 grid')
group by 	accountname,
			apdexCategory,
			threadname,
			snapshot_date
ORDER BY snapshot_date, accountname
""")
# df.show()
# Define the path where you want to save the CSV file
output_path = "Files/ad-hocOnly/PerfomanceComparison"

# Write the DataFrame to a CSV file
df.write.csv(output_path, header=True, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
