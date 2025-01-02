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
from pyspark.sql.functions import col, count, collect_list, concat_ws, when, lit, explode, split, trim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean_df = spark.sql("""
SELECT DISTINCT
    eMail AS email,
    FirstName,
    LastName,
    fDashBoard as Notification,
    Products,
    ProductCount
    ,CAST(CURRENT_DATE() AS DATE) AS snapshot_date
    ,CASE
        WHEN FirstName IS NULL OR FirstName = " "  THEN 'missing_name' 
        WHEN LastName IS NULL OR LastName = " " THEN 'missing_LastName'
        WHEN FirstName IS NULL AND LastName IS NULL THEN 'to_remove'
        ELSE 'clean'
    END AS nameControl
FROM gocDEVBI.ext_ProductStrategy_v_UsersProducts
WHERE Products IS NOT NULL
AND LEFT(eMail,7) <> 'noreply'
AND fDashBoard = 0
""")
clean_df.createOrReplaceTempView("clean_df")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

df_classified = clean_df.withColumn("format",when(col("Email").rlike(email_regex), "Correct_Format").otherwise("Incorrect_Format"))
df_classified.createOrReplaceTempView("df_classified")
# df_classified.show(10,False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_selected = df_classified.select("Products")
df_split = df_selected.withColumn("Product", explode(split(df_selected["Products"], ","))).withColumn("Product", trim("Product")).drop("Products")
# Show the result
df_unique = df_split.dropDuplicates(["Product"])

# Show the result
# df_unique.show(truncate=False)
# Define the path where you want to save the CSV file
output_path = "Files/ad-hocOnly/products.csv"

# Write the DataFrame to a CSV file
df_unique.write.csv(output_path, header=True, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

finalDF ="""
SELECT DISTINCT
     Email
    ,FirstName
    ,LastName
    ,Notification
    ,Products
    ,snapshot_date
FROM df_classified
WHERE
    format <> 'Incorrect_Format'
AND NOT nameControl IN ('to_remove','missing_name','missing_LastName')
AND FirstName <> ' '
AND LastName <> ' '
AND Notification = 0
"""
finalDF = sqlContext.sql(finalDF)
finalDF.createOrReplaceTempView("finalDF")
finalDF.show(10, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = 'Files/StatusPageUAT/'
finalDF.write.mode("overwrite").parquet(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
