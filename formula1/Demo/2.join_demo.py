# Databricks notebook source
# MAGIC %md
# MAGIC ###👉 Type of Joins

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Inner Join
# MAGIC between "circuits" and "races" table

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC <b> The columns "circuit_id" and "name" are duplicated, so we’ll use a SELECT statement to resolve this.

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, races_df.race_name, circuits_df.location, circuits_df.country, races_df.race_year)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Left Outer Join
# MAGIC between "circuits" and "races" table

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, races_df.race_name, circuits_df.location, circuits_df.country, races_df.race_year)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Right Outer
# MAGIC Join between "circuits" and "races" table

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, races_df.race_name, circuits_df.location, circuits_df.country, races_df.race_year)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Full Outer Join 
# MAGIC between "circuits" and "races" table

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, races_df.race_name, circuits_df.location, circuits_df.country, races_df.race_year)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Semi Join
# MAGIC </br>Select columns only left side of the join (between "circuits" and "races" table)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country)

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Anti Join
# MAGIC Anti join is exclude data or right side join and can select only left side columns

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country)

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Only Interchange the table here

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7. Cross Join
# MAGIC Cross Join between "circuits" and "races" table

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)
display(race_circuits_df)

# COMMAND ----------

# Cross Join work like this
int(circuits_df.count()) * int(races_df.count())

# COMMAND ----------

#https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results

# COMMAND ----------

# MAGIC %md
# MAGIC ####8. Natural Join
# MAGIC Automatically joins tables on all columns with the same name (`ustomer_id in this case).

# COMMAND ----------

# MAGIC %md
# MAGIC <b>🔹 Natural Join Equivalent in PySpark</b>
# MAGIC <br>PySpark doesn’t have a direct NATURAL JOIN keyword, but you can simulate it by joining on all common columns. For your case:

# COMMAND ----------

# Find common columns between both DataFrames
common_cols = list(set(circuits_df.columns) & set(races_df.columns))

# Perform a natural INNER JOIN (on all common columns)
race_circuits_df = circuits_df.join(races_df, on=common_cols, how="inner") \
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country)

display(race_circuits_df)


# COMMAND ----------

# MAGIC %sql
# MAGIC /* Four columns are same both of the tables hence we can't use natural join
# MAGIC 1. circuit_ref
# MAGIC 2. name
# MAGIC 3. data_source
# MAGIC 5. file_date
# MAGIC 6. ingestion_date
# MAGIC */
# MAGIC /*
# MAGIC SELECT circuit_id, name, location, country
# MAGIC FROM f1_processed.circuits c
# MAGIC NATURAL JOIN f1_processed.races r 
# MAGIC */
# MAGIC
# MAGIC SELECT * FROM 
# MAGIC     (SELECT circuit_id, name, location, country
# MAGIC     FROM f1_processed.circuits) c
# MAGIC NATURAL JOIN 
# MAGIC     (SELECT race_id, race_year, round, circuit_id
# MAGIC     from f1_processed.races) r 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.circuit_id,
# MAGIC        c.name,
# MAGIC        c.location,
# MAGIC        c.country
# MAGIC FROM f1_processed.circuits c
# MAGIC LEFT JOIN f1_processed.races r on (r.circuit_id = c.circuit_id and r.name = c.name);

# COMMAND ----------

# MAGIC %sql
# MAGIC desc f1_processed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc f1_processed.races