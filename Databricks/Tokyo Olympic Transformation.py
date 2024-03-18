# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "db5f4a5d-d78d-407f-ba9f-011e3812e1e7",
"fs.azure.account.oauth2.client.secret": 'XcH8Q~5BuDhbNnZ~Gx3Q3z.jaxXIAqWEx5d5bcC~',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8fd4e2ce-55a7-4936-98f6-9a3719848f6e/oauth2/token"}


dbutils.fs.mount(
source = "abfss://ctr-tokyo-olympic-data@satokyoolympic.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolymic"

# COMMAND ----------

Athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/Medals.csv")
Teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/Teams.csv")

# COMMAND ----------

Athletes.show()

# COMMAND ----------

Athletes.printSchema()

# COMMAND ----------

Coaches.show()

# COMMAND ----------

Coaches.printSchema()

# COMMAND ----------

EntriesGender.show()

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

Medals.show()

# COMMAND ----------

Medals.printSchema()

# COMMAND ----------

top_gold_medals_countries = Medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------


Athletes.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed data/Athletes")
Coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed data/Coaches")
EntriesGender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed data/EntriesGender")
Medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed data/Medals")
Teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed data/Teams")

# COMMAND ----------


