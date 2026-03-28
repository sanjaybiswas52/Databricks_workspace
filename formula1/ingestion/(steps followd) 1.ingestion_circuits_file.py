# Databricks notebook source
# MAGIC %md
# MAGIC ####Spark Documentation
# MAGIC http://spark.apache.org/docs/latest/api/python/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ![AAD](/Volumes/devdb/raw/image/Image 04-03-26 at 15.36.jpeg)

# COMMAND ----------

spark.read.csv("")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls  dbfs:/Volumes/formula1dl/raw/files/

# COMMAND ----------

circuits_df = spark.read.csv("/Volumes/formula1dl/raw/files/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

#circuits_df.show()
display(circuits_df)

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("/Volumes/formula1dl/raw/files/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/formula1dl/raw/files/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), #False means that the column is not nullable
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv("/Volumes/formula1dl/raw/files/circuits.csv")

circuits_df.printSchema()

circuits_df.show()


# COMMAND ----------

from pyspark.sql.functions import lit

""""
circuits_select_df= circuits_df.select(col("name"), col("location")).withColumnRenamed("name", "circuit_name").withColumn("location", col("location").substr(1, 3)).withColumn("corrected_date", to_date(col("date"), "yyyy-MM-dd")).show()
from pyspark.sql.functions import lit

circuits_df = circuits_df.withColumn("data_source", lit("Ergast API"))
display(circuits_df)


circuits_select_df= circuits_df.select(circuits_df.name, circuits_df.location).withColumnRenamed("name", "circuit_name").withColumn("location", circuits_df.location.substr(1, 3)).withColumn("corrected_date", to_date(circuits_df.date, "yyyy-MM-dd")).withColumn("data_source", lit("Ergast API"))
display(circuits_select_df)
"""
# Remove column curl
circuits_select_df= circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

circuits_select_df.show()

# COMMAND ----------

circuits_select_df= circuits_df.select(circuits_df["circuitId"], 
                                       circuits_df["circuitRef"], 
                                       circuits_df["name"], 
                                       circuits_df["location"], 
                                       circuits_df["country"],
                                       circuits_df["lat"], 
                                       circuits_df["lng"], 
                                       circuits_df["alt"])

circuits_select_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

circuits_select_df= circuits_df.select(col("circuitId"), 
                                       col("circuitRef"), 
                                       col("name"), 
                                       col("location"), 
                                       col("country"),
                                       col("lat"), 
                                       col("lng"), 
                                       col("alt"))

circuits_select_df.show()


