# Databricks notebook source
# MAGIC %md # hello
# MAGIC
# MAGIC %python
# MAGIC from pyspark.sql import functions
# MAGIC

# COMMAND ----------

# MAGIC %run ./Utils

# COMMAND ----------

print(add(1,2))

# COMMAND ----------

print(f"{name} a")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()

df_data = df
df_schema = ["idx"]
# df_final = spark.createDataFrame(data = df_data,schema=df_schema)
# print(df_final.select(sum(df_final.idx)))
df_data.select(sum(df_data))

# COMMAND ----------


