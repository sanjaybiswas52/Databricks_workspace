# Databricks notebook source
# MAGIC %md
# MAGIC ##🌟 Set of optimization techniques in Pyspark
# MAGIC  Practically, these are grouped into 6–7 major types (categories), each containing multiple methods.

# COMMAND ----------

# MAGIC %md
# MAGIC ###1️⃣ DataFrame & Query Optimization
# MAGIC <b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) Use DataFrame API instead of RDD
# MAGIC <br>&nbsp;&nbsp;(b) Predicate pushdown
# MAGIC <br>&nbsp;&nbsp;(c) Column pruning
# MAGIC <br>&nbsp;&nbsp;(d) Avoid UDFs (prefer built‑in functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Predicate pushdown
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Predicate pushdown in PySpark is an optimization technique where filter conditions (WHERE clauses) are pushed down to the data source level (like Parquet, ORC, Delta, or JDBC) so that Spark only reads the relevant subset of data instead of scanning the entire dataset.
# MAGIC
# MAGIC <b>• Without pushdown:</b> Spark loads all data into memory, then applies filters.
# MAGIC <br><b>• With pushdown:</b> Spark instructs the data source to apply the filter while reading, so only matching rows are loaded.
# MAGIC <br>This reduces I/O, speeds up queries, and saves cluster resources.

# COMMAND ----------

# Enable predicate pushdown (usually enabled by default for Parquet/ORC)
spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# Read Parquet file
df = spark.read.csv("/Volumes/demo_catalog/demo_schema/demo_raw/sales_data.csv", header=True, inferSchema=True)

# Apply filter (predicate)
north_sales = df.filter(df.region == "North")

# Trigger action
north_sales.show()

# Debugging: check query plan
north_sales.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ⚡ Example with JDBC (SQL Database)

# COMMAND ----------

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/dev_db") \
    .option("dbtable", "dept") \
    .option("user", "root") \
    .option("password", "root@100") \
    .load()

# Filter with predicate pushdown
#north_sales = df.filter("region = 'North'")
north_sales = df.filter("dept = 10")
north_sales.explain(True)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dept WHERE dept = 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####(c) Column pruning:
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Column pruning in PySpark is an optimization where Spark reads only the required columns from the data source instead of loading all columns. This reduces I/O, memory usage, and speeds up queries — especially useful when working with wide tables (many columns).
# MAGIC
# MAGIC <b>🔑 How Column Pruning Works</b>
# MAGIC <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;• Without pruning: Spark loads all columns, even if you only need a few.
# MAGIC <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;• With pruning: Spark instructs the data source (Parquet, ORC, Delta, JDBC, etc.) to return only the requested columns.

# COMMAND ----------

# Read Parquet file
df = spark.read.parquet("/Volumes/demo_catalog/demo_schema/demo_raw/sales_parquet/")

# Apply column pruning: only select required columns
pruned_df = df.select("region", "amount")

# Trigger action
pruned_df.show(5)

# Debugging: check query plan
pruned_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ####(d) Avoid UDFs (prefer built‑in functions)
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A <b>UDF (User Defined Function) </b>in PySpark is a way to extend Spark’s functionality by writing custom functions in Python (or Scala/Java) and applying them to DataFrame columns. They’re useful when built‑in Spark functions don’t cover your specific logic.
# MAGIC
# MAGIC • A wrapper around a Python function so it can be used in Spark SQL/DataFrame operations.
# MAGIC <br>• Registered with udf() or spark.udf.register().
# MAGIC <br>• Executes row by row, applying your custom logic.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Sample DataFrame
data = [("North", 250), ("South", 120), ("East", 75)]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

# Define Python function
def label_amount(amount):
    return "High" if amount > 200 else "Low"

# Register UDF
label_udf = udf(label_amount, StringType())

# Use UDF in DataFrame
df_with_label = df.withColumn("amount_label", label_udf(df["amount"]))
df_with_label.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Best Practice
# MAGIC <br>• Use Spark SQL built‑in functions (when, col, concat, regexp_replace, etc.) whenever possible.
# MAGIC <br>• Built‑in functions are optimized, vectorized, and benefit from predicate pushdown and column pruning.
# MAGIC
# MAGIC <b>Equivalent Built‑in Example (Better than UDF)

# COMMAND ----------

from pyspark.sql.functions import when

df_with_label = df.withColumn(
    "amount_label",
    when(df.amount > 200, "High").otherwise("Low")
)
df_with_label.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###2️⃣ Partitioning Optimization
# MAGIC <b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) repartition()
# MAGIC <br>&nbsp;&nbsp;(b) coalesce()
# MAGIC <br>&nbsp;&nbsp;(c) File partition tuning (spark.sql.files.maxPartitionBytes)
# MAGIC <br>&nbsp;&nbsp;(d) Shuffle partition tuning (spark.sql.shuffle.partitions)
# MAGIC
# MAGIC 📌 Why: Bad partitioning = slow jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ####(a) Repartition: Data skew scenario
# MAGIC Some user_id has apprx. 10 records but one of the user_id has millions of records.
# MAGIC <ul><li>Most user_id → ~20 records
# MAGIC <li>One user_id → millions of records

# COMMAND ----------


df = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_trans.csv', header=True)
df.show(5)


# COMMAND ----------

df.repartition(10, "user_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ❌ This will NOT help (actually worse)
# MAGIC <br><b>df.repartition(10, "user_id")</b>
# MAGIC <ul><li>Spark uses hash(user_id)
# MAGIC <li>All records of same user_id go to one partition

# COMMAND ----------

# MAGIC %md
# MAGIC #####✅ Option 1: Use random repartition

# COMMAND ----------

df.repartition(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 👍 Yes, this will help
# MAGIC <ul><li>Data is distributed randomly
# MAGIC <li>That heavy user_id gets spread across partitions

# COMMAND ----------

# MAGIC %md
# MAGIC #####✅ Option 2: Salting technique (best practice)

# COMMAND ----------

from pyspark.sql.functions import col, rand

df = df.withColumn("salt", (rand() * 10).cast("int"))
df.show(10)
df = df.repartition("user_id", "salt")


# COMMAND ----------

# MAGIC %md
# MAGIC ###3️⃣Join Optimization
# MAGIC
# MAGIC <b>Key methods:</b><br>
# MAGIC &nbsp;&nbsp;(a) Broadcast joins
# MAGIC <br>&nbsp;&nbsp;(b) Join hints
# MAGIC <br>&nbsp;&nbsp;(c) Sort‑merge vs shuffle hash join selection
# MAGIC
# MAGIC 📌 Why: Joins cause most performance issues

# COMMAND ----------

# MAGIC %md
# MAGIC ####(a) Broadcast joins – Small dataset (to broadcast)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import broadcast

#Large Data set
df_trans = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_trans.csv', header=True)

#Small Data set
dim_df = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_info.csv', header=True)

# Broadcast join
result_df = df_trans.join(
    broadcast(dim_df),
    on="user_id",
    how="inner"
)

# join hint
joined_df = (df_trans
             .join(dim_df.hint("broadcast"), on="user_id", how="inner")
            )

joined_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Joins hints
# MAGIC <b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(i) BROADCAST
# MAGIC <br>&nbsp;&nbsp;(ii) MERGE
# MAGIC <br>&nbsp;&nbsp;(iii)SHUFFLE_HASH
# MAGIC <br>&nbsp;&nbsp;(iv)SHUFFLE_REPLICATE_NL

# COMMAND ----------

# MAGIC %md
# MAGIC #####1️⃣ Join Hints

# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹BROADCAST hint

# COMMAND ----------

from pyspark.sql.functions import broadcast

#Large Data set
df_trans = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_trans.csv', header=True)

#Small Data set
dim_df = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_info.csv', header=True)

# join hint
joined_df = (df_trans
             .join(dim_df.hint("broadcast"), on="user_id", how="inner")
            )

joined_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹MERGE hint
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;In PySpark, the MERGE hint is a way to tell Spark’s optimizer to use a sort‑merge join</b> strategy when executing a join. <b>This is useful when both sides of the join are large</b> and already sorted or when you want to avoid broadcast joins. By applying the hint, you influence the Catalyst optimizer’s choice of join algorithm.
# MAGIC <ul><li>Purpose:
# MAGIC <ul><li>Both datasets are large (broadcast join is not feasible).
# MAGIC <br><li>Data is already sorted on the join key.
# MAGIC <li>You want predictable join behavior.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MergeHintExample").getOrCreate()

# Sample DataFrames
employees = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Charlie", 10)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (10, "HR"),
    (20, "Finance"),
    (30, "IT")
], ["dept_id", "dept_name"])

# Apply MERGE hint on departments DataFrame
joined_df = employees.join(departments.hint("merge"), "dept_id")

# Show results
joined_df.show()

# Debugging: check query plan
joined_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹SHUFFLE_HASH hint
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The <b>SHUFFLE_HASH hint</b> in PySpark is a directive you can give to Spark’s Catalyst optimizer to use a shuffle hash join strategy when performing a join. Normally, Spark decides the join strategy automatically (broadcast, sort‑merge, shuffle hash) based on dataset sizes and configs. By applying the hint, you override the optimizer’s choice.
# MAGIC <br><b>Note:</b>
# MAGIC <ul><li>Spark builds a hash table on the join keys after shuffling both datasets by the join key.
# MAGIC <li>Best suited when both datasets are of moderate size and hash join is more efficient than sort‑merge.
# MAGIC <li>Avoids sorting overhead required in sort‑merge joins.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShuffleHashHintExample").getOrCreate()

# Sample DataFrames
employees = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Charlie", 10)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (10, "HR"),
    (20, "Finance"),
    (30, "IT")
], ["dept_id", "dept_name"])

# Apply SHUFFLE_HASH hint on departments DataFrame
joined_df = employees.join(departments.hint("shuffle_hash"), "dept_id")

# Show results
joined_df.show()

# Debugging: check query plan
joined_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹SHUFFLE_REPLICATE_NL hint
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The <b>SHFFLE_REPLICATE_NL hint</b> in PySpark tells Spark’s optimizer to use a shuffle-and-replicate nested loop join strategy. This is one of the least common join strategies, but it can be useful in specific scenarios.
# MAGIC
# MAGIC <b>🔑 What is a Shuffle Replicate Nested Loop Join</b>
# MAGIC <ul><li>Spark shuffles one side of the dataset and replicates the other side across all partitions.
# MAGIC <li>Then it performs a nested loop join (comparing every row from one dataset with every row from the other).
# MAGIC <li>Use case: When join conditions are complex (non‑equality joins, e.g., <, >, !=) and other join strategies (like sort‑merge or hash join) are not applicable.
# MAGIC <li>Cost: Very expensive for large datasets, because it can lead to a Cartesian‑like explosion.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShuffleReplicateNLHintExample").getOrCreate()

# Sample DataFrames
employees = spark.createDataFrame([
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
], ["emp_id", "name", "age"])

departments = spark.createDataFrame([
    (10, "HR"),
    (20, "Finance"),
    (30, "IT")
], ["dept_id", "dept_name"])

# Apply SHUFFLE_REPLICATE_NL hint
# Example: join with a non-equality condition
joined_df = employees.join(departments.hint("shuffle_replicate_nl"), employees.age > departments.dept_id)

# Show results
joined_df.show()

# Debugging: check query plan
joined_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC #####2️⃣ Partitioning Hints
# MAGIC Used to control data distribution & output files.
# MAGIC <br><b>Supported partition hints:</b>
# MAGIC <ul><li>COALESCE
# MAGIC <li>REPARTITION
# MAGIC <li>REPARTITION_BY_RANGE
# MAGIC <li>REBALANCE

# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹COALESCE
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`cooalesce()`</b> in PySparks a transformation used to reduce the number of partitions in a DataFrame or RDD. Unlike epartition(), which can both increase or decrease partitions and involves a full shuffle, coalesce() only decreases partitions and avoids a full shuffle, making it more efficient when downsizing.
# MAGIC <b>🔑 Key Points</b>
# MAGIC <ul><li>Purpose: Reduce partitions without a full shuffle.
# MAGIC <li>Efficient: Faster than repartition() when decreasing partitions.
# MAGIC <li>Use case: Optimize small datasets before writing to disk (e.g., fewer output files).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, countDistinct

spark = SparkSession.builder.appName("CoalesceExample").getOrCreate()

# Sample DataFrame
data = [("North", 250), ("South", 120), ("East", 75), ("West", 300)]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

df.write.mode("overwrite").parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/sales_file")
# Check initial number of partitions
#print("Initial partitions:", df.rdd.getNumPartitions())
df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

# Reduce partitions using coalesce
df_coalesced = df.coalesce(1)

# Check new number of partitions
#print("After coalesce:", df_coalesced.rdd.getNumPartitions())
df_coalesced.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

# Save as single file (since only 1 partition)
df_coalesced.write.mode("overwrite").parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/sales_single_file")


# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹REPARTITION
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`repartition()`</b> in PySpark is a transformation used to increase or decrease the number of partitions in a DataFrame or RDD. Unlike coalesce(), which only reduces partitions without a full shuffle, repartition() always triggers a full shuffle to evenly distribute data across the specified number of partitions.
# MAGIC <br><b>🔑 Key Points</b>
# MAGIC <ul><li><b>Purpose:</b> Change the number of partitions (increase or decrease).
# MAGIC <li><b>Shuffle:</b> Always involves a full shuffle → more expensive than coalesce().
# MAGIC <li><b>Use case:</b>
# MAGIC <ul><li>Increase partitions for parallelism (large datasets).
# MAGIC <li>Balance data distribution across partitions.
# MAGIC <li>Prepare for joins or aggregations to avoid skew.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, countDistinct

spark = SparkSession.builder.appName("RepartitionExample").getOrCreate()

# Sample DataFrame
data = [("North", 250), ("South", 120), ("East", 75), ("West", 300)]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

# Check initial number of partitions
#print("Initial partitions:", df.rdd.getNumPartitions())
df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

# Increase partitions using repartition
df_repartitioned = df.repartition(6)

# Check new number of partitions
#print("After repartition:", df_repartitioned.rdd.getNumPartitions())
df_repartitioned.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()
        
# Save with balanced partitions
df_repartitioned.write.mode("overwrite").parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/sales_balanced")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ REPARTITION(4, user_id) */ * FROM demo_catalog.demo_schema.user_trans WHERE user_id = 999 limit 10;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹REPARTITION_BY_RANGE
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`repartitionByRange()`</b> inySpark is a transformation that redistributes rows into partitions based on the range of values in one or more columns. Unlike repartition(), which randomly shuffles data across partitions, repartitionByRange() ensures that rows with similar values end up in the same partition. This is especially useful for range queries, sorting, and bucketing.
# MAGIC <br><b>🔑 Key Points</b>
# MAGIC <ul><li><b>Purpose:</b> Partition data by column ranges.
# MAGIC <li><b>Deterministic:</b> Rows are grouped into partitions based on value ranges.
# MAGIC <li><b>Use case:</b> Optimizing queries that filter or sort by a specific column (e.g., date ranges, numeric ranges).
# MAGIC <li>Shuffle: Yes, it involves a shuffle to redistribute rows.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, countDistinct

df = spark.read.format('delta').load('/Volumes/demo_catalog/demo_schema/demo_raw/orders/')

df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

df_range = df.repartitionByRange(3, "order_date")

#print(df_range.rdd.getNumPartitions())

df_range.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######🔹REBALANCE
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`rebalancee()`</b> in PySparks a transformation introduced in Spark 3.2 that redistributes data across partitions to achieve a more <b>even load balance</b>. Unlike `repartition), which randomly shuffles data, `ebalance() tries to spread rows more evenly across partitions, reducing skew and improving parallelism.
# MAGIC <br><b>🔑 Key Points</b>
# MAGIC <ul><li>Purpose: Redistribute rows evenly across partitions.
# MAGIC <li>Difference from repartition():
# MAGIC <ul><li><b>repartition()</b> → random shuffle, may still cause uneven distribution.
# MAGIC <li><b>rebalance()</b> → explicitly balances rows across partitions.</ul>
# MAGIC <li>Use case:
# MAGIC <ul><li>When partitions are skewed (some very large, some very small).
# MAGIC <li>Before expensive operations like joins or aggregations.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, countDistinct

spark = SparkSession.builder.appName("RebalanceExample").getOrCreate()

# Sample DataFrame
data = [("North", 250), ("South", 120), ("East", 75), ("West", 300), ("North", 450), ("South", 220)]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

# Check initial partition distribution
df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

# Rebalance into 3 partitions
df_balanced = df.rebalance(numPartitions=3)

# Check new partition distribution
df_balanced.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()



# COMMAND ----------

# MAGIC %md
# MAGIC #####3️⃣ Skew Hints
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Skew hints in PySpark are optimizer hints that tell Spark how to handle data skew during joins. Data skew happens when one or more keys have disproportionately large amounts of data compared to others, causing uneven partition sizes and slowing down queries.
# MAGIC <br><b>🔑 What are Skew Hints?</b>
# MAGIC <ul><li>Spark normally tries to detect skew automatically (via Adaptive Query Execution).
# MAGIC <li>With skew hints, you explicitly tell Spark to apply skew handling strategies.
# MAGIC <li>Spark will split skewed partitions into smaller chunks and replicate the other side of the join to balance the workload.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SkewHintExample").getOrCreate()

# Sample DataFrames
sales = spark.createDataFrame([
    ("North", 250),
    ("North", 450),
    ("North", 300),   # Skewed key 'North'
    ("South", 120),
    ("East", 75),
    ("West", 300)
], ["region", "amount"])

regions = spark.createDataFrame([
    ("North", "Zone1"),
    ("South", "Zone2"),
    ("East", "Zone3"),
    ("West", "Zone4")
], ["region", "zone"])

# Apply skew hint on the smaller DataFrame
joined_df = sales.join(regions.hint("skew"), "region")

joined_df.show()

# Debugging: check query plan
joined_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ###4️⃣Caching & Persistence Optimization
# MAGIC Reuse data efficiently
# MAGIC <br><b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) cache()
# MAGIC <br>&nbsp;&nbsp;(b)persist(MEMORY_AND_DISK)
# MAGIC <br>&nbsp;&nbsp;(c)unpersist()
# MAGIC
# MAGIC 📌 Why: Avoid recomputation in iterative workloads

# COMMAND ----------

# MAGIC %md
# MAGIC #####(b) Persist and Cache is not supported on Serverless compute.
# MAGIC
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Large dataset (your skewed data)
# MAGIC <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Use the earlier dataset:<br>
# MAGIC &nbsp;&nbsp;📌 user_trans.csv (with skewed user_id = 999)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

#Large Data set
df_trans = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_trans.csv', header=True)

#Small Data set
dim_df = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/user_info.csv', header=True)

# cache use for small data set
#dim_df.cache()

#Persist use for large data set
#dim_trans.persist(StorageLevel.MEMORY_AND_DISK)

# Broadcast join
result_df = df_trans.join(
    broadcast(dim_df),
    on="user_id",
    how="inner"
)

result_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####(c) Unpersist
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Using unpersist() in PySpark to clear cached data from memory/disk when you no longer need it:

# COMMAND ----------

# Step 1: Read CSV into DataFrame
df = spark.read.csv("/Volumes/demo_catalog/demo_schema/demo_raw/sales_data.csv", header=True, inferSchema=True)

# Step 2: Cache the DataFrame (store in memory for faster reuse)
#df.cache() - PERSIST TABLE is not supported on serverless compute

# Step 3: Trigger an action to actually load it into cache
df.count()   # forces Spark to evaluate and cache the data

# Step 4: Use the cached DataFrame in analysis
df.filter(df.region == "North").show(5)

# Step 5: Release memory/disk resources when caching is no longer needed
#df.unpersist() - PERSIST TABLE is not supported on serverless compute


# COMMAND ----------

# MAGIC %md
# MAGIC ###5️⃣ File Format & Storage Optimization
# MAGIC Reduces scan and I/O cost
# MAGIC <br><b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) Use Parquet / ORC / Delta
# MAGIC <br>&nbsp;&nbsp;(b) Compression (Snappy, ZSTD)
# MAGIC <br>&nbsp;&nbsp;(c) Partition pruning
# MAGIC
# MAGIC 📌 Why: Columnar formats are faster and cheaper

# COMMAND ----------

# MAGIC %md
# MAGIC ####(a) Use Parquet / ORC / Delta
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ORC (Optimized Row Columnar) file format</b> is a highly efficient columnar storage format used in big data systems like Apache Spark and Hive. It is designed to store large datasets in a compressed, optimized way, making queries faster and reducing storage costs.
# MAGIC <br><b>🔑 Why Use ORC Files</b>
# MAGIC <ul><li><b>Columnar storage</b> → reads only required columns.
# MAGIC <li><b>Compression</b> → reduces disk space usage.
# MAGIC <li><b>Predicate pushdown</b> → filters applied at the storage level.
# MAGIC <li><b>Efficient for analytics</b> → faster scans and aggregations.
# MAGIC <li><b>Integration</b> → works seamlessly with Spark, Hive, and other Hadoop ecosystem tools.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ORCExample").getOrCreate()

# Sample DataFrame
data = [
    ("North", 250),
    ("South", 120),
    ("East", 75),
    ("West", 300)
]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

# Write DataFrame to ORC format
df.write.mode("overwrite").orc("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/sales_orc")

# Read DataFrame to ORC format
df_orc = spark.read.orc("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/sales_orc")
df_orc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Compression (Snappy, ZSTD)
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Compression in Spark (Snappy, ZSTD)</b> refers to reducing the size of data stored on disk or transmitted across the network by applying compression algorithms. Spark supports multiple codecs (Snappy, ZSTD, Gzip, LZO, etc.), and the choice affects speed vs. compression ratio.
# MAGIC <br><b>🔑 Why Compression Matters</b>
# MAGIC <ul><li>Reduces storage size → saves disk space.
# MAGIC <li>Improves I/O performance → less data read/written.
# MAGIC <li>Trade‑off: Some codecs are faster (Snappy), others compress better (ZSTD).

# COMMAND ----------

# MAGIC %md
# MAGIC <b>⚡ Common Codecs</b>
# MAGIC <table border="1" cellpadding="6" cellspacing="0">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Codec</th>
# MAGIC       <th>Speed</th>
# MAGIC       <th>Compression Ratio</th>
# MAGIC       <th>Best Use Case</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td><b>Snappy</b></td>
# MAGIC       <td>Very fast</td>
# MAGIC       <td>Moderate</td>
# MAGIC       <td>General purpose, real‑time analytics</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td><b>ZSTD (Zstandard)</b></td>
# MAGIC       <td>Fast + tunable</td>
# MAGIC       <td>High</td>
# MAGIC       <td>Large datasets, better compression with good speed</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td><b>Gzip</b></td>
# MAGIC       <td>Slower</td>
# MAGIC       <td>High</td>
# MAGIC       <td>Archival storage, when size matters more than speed</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td><b>LZO</b></td>
# MAGIC       <td>Fast</td>
# MAGIC       <td>Low</td>
# MAGIC       <td>Legacy Hadoop workloads</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####(i) Writing Parquet with Snappy

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ORCExample").getOrCreate()

# Sample DataFrame
data = [
    ("North", 250),
    ("South", 120),
    ("East", 75),
    ("West", 300)
]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)

df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/parquet_sales_snappy")

df.write.mode("overwrite") \
    .option("compression", "zstd") \
    .parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/parquet_sales_zstd")

df_snappy = spark.read.parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/parquet_sales_snappy")
df_zstd   = spark.read.parquet("/Volumes/demo_catalog/demo_schema/demo_raw/tmp/parquet_sales_zstd")

print("Snappy count:", df_snappy.count())
print("ZSTD count:", df_zstd.count())


# COMMAND ----------

# MAGIC %md
# MAGIC #####(ii) Writing Parquet with zstd
# MAGIC See above 5b(i)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6️⃣ Shuffle & Skew Optimization
# MAGIC Handles uneven data distribution
# MAGIC <br><b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) Reduce wide transformations
# MAGIC <br>&nbsp;&nbsp;(b) Skew join handling
# MAGIC <br>&nbsp;&nbsp;(c) Salting keys
# MAGIC <br>&nbsp;&nbsp;(d) AQE skew optimization
# MAGIC
# MAGIC 📌 Why: Skewed data can kill performance

# COMMAND ----------

# MAGIC %md
# MAGIC #####(a1) Wide transformations
# MAGIC Reduce wide transformations :
# MAGIC <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Wide transformations in PySpark are operations that require data to be shuffled across the cluster, because each output partition depends on multiple input partitions.
# MAGIC <br><b>Examples:</b> groupBy, join, `distinct orderBy.</br>
# MAGIC <b>Impact:</b> More expensive than narrow transformations because they involve network I/O.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, countDistinct

df = spark.read.format('delta').load('/Volumes/demo_catalog/demo_schema/demo_raw/sales/')
df.show(5)

df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

df_files = dbutils.fs.ls("/Volumes/demo_catalog/demo_schema/demo_raw/sales")
print(f"Number of files: {len(df_files)}")

# Wide transformation: groupBy (requires shuffle)
grouped_df = df.groupBy("region").sum("amount")

df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

# Show results
grouped_df.show()

# Debugging: check query plan
grouped_df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC <b>🔍 Why This is Wide</b>
# MAGIC <br>• `groupBy(“region) requires Spark to shuffle all rows with the same region into the same partition.
# MAGIC <br>• The shuffle makes this a wide transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC #####(a2) Narrow transformations:
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;In PySpark are operations where each output partition depends only on a single input partition.
# MAGIC <br><b>Examples:</b> map, filter, union, select, flatMap.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, countDistinct

df = spark.read.format('delta').load('/Volumes/demo_catalog/demo_schema/demo_raw/sales/')
df.show(5)

df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

df_files = dbutils.fs.ls("/Volumes/demo_catalog/demo_schema/demo_raw/sales")
print(f"Number of files: {len(df_files)}")

# Narrow transformation: filter
north_sales = df.filter(df.region == "North")

# Narrow transformation: select
selected = north_sales.select("region", "amount")

# Action to trigger execution
selected.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Skew join handling
# MAGIC See point 2(a) for data skewness

# COMMAND ----------

# MAGIC %md
# MAGIC ####(c) Salting keys
# MAGIC See point 2a(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(d) AQE skew optimization
# MAGIC <b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Adaptive Query Execution</b> (AQE) skew optimization in PySpark automatically detects skewed partitions during runtime and splits them into smaller, balanced partitions to avoid bottlenecks. 
# MAGIC <br><br><b>Dataset</b></br>
# MAGIC Imagine you have a sales dataset where most transactions come from one region (North), while other regions have very few records.

# COMMAND ----------

data = [
    ("North", 250), ("North", 450), ("North", 300),  # skewed key
    ("South", 120), ("East", 75), ("West", 300)
]
columns = ["region", "amount"]
df = spark.createDataFrame(data, columns)
display(df)

# Another small dimension table
region_info = [("North", "Zone1"), ("South", "Zone2"), ("East", "Zone3"), ("West", "Zone4")]
dim_df = spark.createDataFrame(region_info, ["region", "zone"])
display(dim_df)

# Wide transformation: join (risk of skew on 'North')
joined_df = df.join(dim_df, "region")



# COMMAND ----------

# MAGIC %md
# MAGIC ⚡ Enabling AQE Skew Optimization

# COMMAND ----------

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Optional: set threshold for skew partition size
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 64 * 1024 * 1024)  # 64 MB


# COMMAND ----------

# MAGIC %md
# MAGIC ###7️⃣ Execution Engine Optimization
# MAGIC Spark runtime level
# MAGIC <br><b>Key methods:</b>
# MAGIC <br>&nbsp;&nbsp;(a) Adaptive Query Execution (AQE)
# MAGIC <br>&nbsp;&nbsp;(b) Kryo serialization
# MAGIC <br>&nbsp;&nbsp;(c) Apache Arrow for Pandas UDFs
# MAGIC <br>&nbsp;&nbsp;(d) Executor memory & cores tuning
# MAGIC
# MAGIC 📌 Why: Runtime tuning improves overall efficiency

# COMMAND ----------

# MAGIC %md
# MAGIC ####(a) Adaptive Query Execution (AQE)
# MAGIC See point 6d

# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Kryo serialization
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Kryo serialization is a high‑performance serialization framework used in Apache Spark to efficiently convert objects into byte streams for storage or transmission. Spark uses Java serialization by default, but Kryo is faster and produces smaller serialized sizes, which improves performance in shuffle operations, caching, and network communication.

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session with Kryo serialization enabled
spark = SparkSession.builder \
    .appName("KryoExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrationRequired", "false") \
    .getOrCreate()

# Sample data
data = [("North", 250), ("South", 120), ("East", 75), ("West", 300)]
columns = ["region", "amount"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Perform a transformation (narrow)
filtered = df.filter(df.amount > 100)

# Trigger action
filtered.show()

##print(spark.conf.get("spark.serializer"))



# COMMAND ----------

# MAGIC %md
# MAGIC ###8️⃣ Applying Z-Ordering
# MAGIC <b>Z-Ordering in PySpark with Delta Lake</b>. Z-Ordering is especially useful when you frequently filter on multiple columns, because it co-locates related data blocks on disk, reducing scan overhead.

# COMMAND ----------

#%run ./include/'01.create delta file and table using CSV file'

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_catalog;
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM demo_schema.sales
# MAGIC WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'
# MAGIC   AND product_id = 'P123';

# COMMAND ----------

# MAGIC %md
# MAGIC ####(a) Run query with explain(True)

# COMMAND ----------

# Count number of files scanned before optimization
df_before = spark.sql("""
SELECT * FROM demo_schema.sales WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31' AND product_id = 'P110'
""")
print("Files scanned before optimization:")
df_before.explain(True)   # shows physical plan with file scan details

# COMMAND ----------

# MAGIC %md
# MAGIC ####(b) Describe table and Note files scan count.

# COMMAND ----------

df_det = spark.sql("DESCRIBE DETAIL demo_schema.sales")
display(df_det)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(c) Apply Z-odering (executeZOrderBy)

# COMMAND ----------

from delta.tables import DeltaTable

# Path to Delta table
sales_table_path = "s3://sanjaydatabricks01/dlt/sales"

# Load Delta table
sales_table = DeltaTable.forPath(spark, sales_table_path)

# Optimize with Z-Ordering
sales_table.optimize().executeZOrderBy("order_date", "product_id")

# COMMAND ----------

df_det = spark.sql("DESCRIBE DETAIL demo_schema.sales")
display(df_det)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(d) Run query again with explain(True) → confirm fewer files scanned.

# COMMAND ----------

df_after = spark.sql("""
SELECT * FROM demo_schema.sales WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31' AND product_id = 'P110'
""")
print("Files scanned after optimization:")
df_after.explain(True)


# COMMAND ----------

df_det = spark.sql("DESCRIBE DETAIL demo_schema.sales")
display(df_det)

# COMMAND ----------

# MAGIC %md
# MAGIC ####(e) Show HISTORY or Version of data.

# COMMAND ----------

df_hist = spark.sql("DESCRIBE HISTORY demo_schema.sales")
display(df_hist)
