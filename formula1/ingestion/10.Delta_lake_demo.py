# Databricks notebook source
# MAGIC %md
# MAGIC ####Delta Lake Documents website
# MAGIC https://docs.delta.io/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ####Manage Table and External Table
# MAGIC <ul><li>Write Delta to delta lake (manage table)
# MAGIC <li>Wite data to delta lake (external table)
# MAGIC <li>Read data from delta lake (Table)
# MAGIC <li>Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog udemy;
# MAGIC /*CREATE DATABASE IF NOT EXISTS udemy.f1_demo
# MAGIC LOCATION '/udemy/formual1/demodb'
# MAGIC */

# COMMAND ----------


results_df = spark.read \
  .option("inferSchema", True) \
  .json("s3://databricks02ar/f1/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create manage table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")


                                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create external table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/Volumes/udemy/formula1/demo/results_managed")

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/Volumes/udemy/formula1/demo/results_managed")

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC Show partitions f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update and Delete Records
# MAGIC <ul><li>Update Delta Table
# MAGIC <li>Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table udemy.f1_demo.events_external
# MAGIC using delta
# MAGIC location '/Volumes/udemy/formula1/demo/events_external'
# MAGIC     
# MAGIC

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/Volumes/udemy/formula1/demo/results_managed")

display(results_external_df)

#results_external_df.saveAsTable("udemy.f1_demo.events_external")


# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where position <= 10
# MAGIC  and points not in ('N', '\N')
# MAGIC     

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = deltaTable.forPath(spark, "/Volumes/udemy/formula1/demo/events_managed") 
deltaTable.update("points <= 10", {"points": "21 - posititon"})

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr

# Load the Delta table
deltaTable = DeltaTable.forPath(spark, "/Volumes/udemy/formula1/demo/events_managed")

# Update rows where points <= 10
deltaTable.update(
    condition = expr("points <= 10"),
    set = { "points": expr("21 - position") }
)


# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where points > 10;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr

# Load the Delta table
deltaTable = DeltaTable.forPath(spark, "/Volumes/udemy/formula1/demo/events_managed")

# Update rows where points <= 10
deltaTable.delete(
    condition = expr("points <= 0")
)

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = deltaTable.forPath(spark, "/Volumes/udemy/formula1/demo/events_managed") 
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upsert using Merge

# COMMAND ----------

drivers_day1_df = spark.read \
  .option("inferSchema", True) \
  .json("/Volumes/udemy/formula1/raw/2021-03-21/drivers.json") \
  .filter("driverId <= 10") \
  .select("driverId", "dob", "name.forename", "name.surname")


display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
  .option("inferSchema", True) \
  .json("/Volumes/udemy/formula1/raw/2021-03-28/drivers.json") \
  .filter("driverId BETWEEN 6 and 15") \
  .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read \
  .option("inferSchema", True) \
  .json("/Volumes/udemy/formula1/raw/2021-03-28/drivers.json") \
  .filter("driverId BETWEEN 6 and 15 OR driverId BETWEEN 16 AND 20") \
  .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks02ar/f1/demo/drivers_merge'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) 
# MAGIC        VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) 
# MAGIC        VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day3 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) 
# MAGIC        VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC INSERT OVERWRITE DIRECTORY '/Volumes/udemy/formula1/demo/drivers_merge'
# MAGIC USING DELTA
# MAGIC SELECT * FROM f1_demo.drivers_merge;
# MAGIC */
# MAGIC

# COMMAND ----------

"""drivers_merge_df = spark.table("f1_demo.drivers_merge")

drivers_merge_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/udemy/formula1/demo/drivers_merge")
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "s3://databricks02ar/f1/demo/drivers_merge/")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
.whenMatchedUpdate(set = { 
    "dob" : "upd.dob", 
    "forename" : "upd.forename", 
    "surname" : "upd.surname", 
    "updatedDate": "current_timestamp()" 
}) \
.whenNotMatchedInsert(values = {
    "driverId": "upd.driverId",
    "dob": "upd.dob",
    "forename": "upd.forename",
    "surname": "upd.surname",
    "createdDate": "current_timestamp()"
}) \
.execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge limit 10

# COMMAND ----------

spark.read.format("delta").load("s3://databricks02ar/f1/demo/drivers_merge").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####History-versioning and Time Travel
# MAGIC <ul><li>History & Versioning
# MAGIC <li>Time Travel
# MAGIC <li>Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog udemy

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2 order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2026-03-12T15:56:23.000+00:00' order by 1

# COMMAND ----------

spark.read.format("delta").option("versionAsOf ", 2).load( "s3://databricks02ar/f1/demo/drivers_merge/").show()

# COMMAND ----------

spark.read.format("delta").option("timestampAsOf", "2026-03-12T15:56:23.000+00:00").load( "s3://databricks02ar/f1/demo/drivers_merge/").orderBy("driverId").show()



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col

spark.read.format("delta").option("timestampAsOf", "2026-03-12T15:56:23.000+00:00").load( "s3://databricks02ar/f1/demo/drivers_merge/").orderBy(col("driverId").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Vacuum Does:
# MAGIC <ul><li><b>Deletes orphaned files:</b> Removes data files that are not part of the current Delta table snapshot.
# MAGIC <li><b>Retention threshold:</b> By default, Databricks keeps files for 7 days to allow for time travel queries. Files older than this threshold can be safely deleted.
# MAGIC <li><b>Skips system directories:</b> It ignores directories starting with ` (like `delta_log) to avoid corrupting metadata.
# MAGIC <li><b>Reclaims storage:</b> Frees up space in cloud storage (S3, ADLS, etc.) by cleaning unused files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS;
# MAGIC
# MAGIC --ANALYZE TABLE f1_demo.drivers_merge COMPUTE STATISTICS;
# MAGIC --ANALYZE TABLE f1_demo.drivers_merge COMPUTE STATISTICS FOR COLUMNS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Delete driver 1 however data is available in previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Data is available in previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 7

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data Recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 7 src
# MAGIC ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks02ar/f1/demo/drivers_txn'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn
# MAGIC     
# MAGIC --SELECT * FROM f1_demo.drivers_txn TIMESTAMP AS OF

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC <b>One parquet file has been created in folder location as mentioned while creating table</b>
# MAGIC s3://databricks02ar/f1/demo/drivers_txn
# MAGIC <br><ul>New <b>praquet</b> and <b>json</b> file will create for each transaction.</br>
# MAGIC that way TIME TRAVEL maintain history.</br>
# MAGIC <br>{"commitInfo":{"timestamp":1773382349102,"userId":"73541139559476",</br>
# MAGIC {"<b>add</b>":{"path":"<b>part-00000-676a2f75-7499-4fc3-b223-92a09a89dadb.c000.snappy.parquet</b>"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn where driverId = 1
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC <br>{"commitInfo":{"timestamp":1773382349102,"userId":"73541139559476",</br>
# MAGIC {"<b>remove</b>":{"path":"<b>part-00000-b6ad1670-85ec-4a91-89c8-6bca0087eb6f.c000.snappy.parquet</b>"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create Checkpoint for tiny files
# MAGIC <ul><li>Delta Lake keeps track of all changes in JSON log files inside the _delta_log folder.
# MAGIC <li>To avoid replaying thousands of JSON files during queries, Delta periodically writes a checkpoint (a Parquet file summarizing the table state).
# MAGIC <li>The property delta.checkpointInterval specifies the number of commits after which a checkpoint is created.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE f1_demo.drivers_txn
# MAGIC SET TBLPROPERTIES ("delta.checkpointInterval" = "10");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn where driverId > 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create tiny files

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                  SELECT * FROM f1_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC ####Convert Parquet to Delta 
# MAGIC <ol><li> Parquet Table to Delta 
# MAGIC <li>Parquet file to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Parquet Table to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION 's3://databricks02ar/f1/demo/drivers_convert_to_delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC <b>There is no delta logfile in folder "s3://databricks02ar/f1/demo/drivers_convert_to_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Once the table is converted <b>Parquet</b> to <b>Delta</b> format, the <b>_delta_log</b> directory will be created, and relevant log and checkpoint files will be available inside it.
# MAGIC <ul><li>_last_checkpoint
# MAGIC <li>_staged_commits/
# MAGIC <li>00000000000000000000.checkpoint.parquet
# MAGIC <li>00000000000000000000.crc
# MAGIC <li>00000000000000000000.json
# MAGIC <br><br> Which maintain History, Time Travel.</br>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Parquet File to Delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("s3://databricks02ar/f1/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`s3://databricks02ar/f1/demo/drivers_convert_to_delta_new`

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create table from external file.
# MAGIC <b>Need table structure to create table from external file</b>
# MAGIC <br>Multiple reliable ways to find column of Dalta files. <b>Delta transaction log (_delta_log)</b>, which is the source of truth for schema in Delta Lake.
# MAGIC <ol><li>DESCRIBE DETAIL (best starting point)
# MAGIC <li>DESCRIBE TABLE (column‑level view)
# MAGIC <li>Read schema via Spark (PySpark)
# MAGIC <li>Quick data inspection (limited but practical)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>DESCRIBE DETAIL and DESCRIBE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE DETAIL delta.`s3://databricks02ar/f1/demo/drivers_convert_to_delta_new`;
# MAGIC DESCRIBE TABLE delta.`s3://databricks02ar/f1/demo/drivers_convert_to_delta_new`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED delta.`s3://databricks02ar/f1/demo/drivers_convert_to_delta_new`;

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Read schema via Spark (PySpark)

# COMMAND ----------

df = spark.read.format("delta") \
    .load("s3://databricks02ar/f1/demo/drivers_convert_to_delta_new")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Quick data inspection (limited but practical)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.`s3://databricks02ar/f1/demo/drivers_convert_to_delta_new`
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta_new (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 's3://databricks02ar/f1/demo/drivers_convert_to_delta_new'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_convert_to_delta_new;

# COMMAND ----------


spark.read.format("delta") \
  .load("s3://databricks02ar/f1/demo/drivers_convert_to_delta_new") \
  .show()


# COMMAND ----------

