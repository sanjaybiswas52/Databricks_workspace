# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM demo_catalog.demo_schema.circuits LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG demo_catalog;  
# MAGIC USE SCHEMA demo_schema;
# MAGIC
# MAGIC select * from circuits LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

display(spark.sql('SHOW TABLES'))

# COMMAND ----------

df = spark.table('demo_catalog.demo_schema.circuits')
display(df)