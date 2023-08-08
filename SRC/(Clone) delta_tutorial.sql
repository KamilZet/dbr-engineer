-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Load the data from its source.
-- MAGIC df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
-- MAGIC
-- MAGIC # Write the data to a table.
-- MAGIC table_name = "people_10m"
-- MAGIC df.write.saveAsTable(table_name)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
-- MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
-- MAGIC ) AS VALUES
-- MAGIC   (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
-- MAGIC   (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
-- MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
-- MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
-- MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
-- MAGIC   (90000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
-- MAGIC
-- MAGIC   select * from people_updates

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC MERGE INTO people_10m
-- MAGIC USING people_updates
-- MAGIC ON people_10m.id = people_updates.id
-- MAGIC WHEN MATCHED THEN UPDATE SET *
-- MAGIC WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

UPDATE default.people_10m SET gender = 'Female' WHERE gender = 'F';
UPDATE default.people_10m SET gender = 'Male' WHERE gender = 'M';

-- COMMAND ----------

SELECT * FROM people_10m WHERE id >= 9999998

-- COMMAND ----------

DESCRIBE HISTORY people_10m

-- COMMAND ----------

SELECT mod(id,1000) FROM people_10m VERSION AS OF 0
group by mod(id,1000)
order by 1 desc

-- COMMAND ----------

SELECT max(id) FROM people_10m VERSION --AS OF 0
-- group by mod(id,1000)
order by 1 desc
