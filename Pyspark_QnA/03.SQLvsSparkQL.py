# Databricks notebook source
# MAGIC %md
# MAGIC ###👉 DDL SQL Command

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹 RENAME Table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_catalog.demo_schema.employees
# MAGIC RENAME TO demo_catalog.demo_schema.emp;

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹 ADD Column in Table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_catalog.demo_schema.employees
# MAGIC ADD COLUMNS (employee_name STRING);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE demo_catalog.demo_schema.employees
# MAGIC SET employee_name = CONCAT(first_name, ' ', last_name);
# MAGIC

# COMMAND ----------

# Department Dataframe
dept_df = spark.read.csv('s3://databricks02ar/demo/departments.csv', header=True, inferSchema=True)
dept_df.show(n=5, truncate=False)

#Employee Dataframe
emp_df = spark.read.csv('s3://databricks02ar/demo/employees.csv', header=True, inferSchema=True)
emp_df.show(n=5, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_catalog;
# MAGIC use schema demo_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.employee_id, e.employee_name, e.job_title, e.department_id, e.manager_id
# MAGIC FROM demo_catalog.demo_schema.employees e limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###👉 Basic Data Files Activity

# COMMAND ----------

# MAGIC %md
# MAGIC ###Export data from table to Databricks Volume /Volumes/devdb/hr/emp/ in CSV format

# COMMAND ----------

# Replace with your actual table name
table_name = "demo_catalog.demo_schema.employees"   # example: catalog.schema.table

# Load table as DataFrame
emp_df = spark.table(table_name)
emp_df.coalesce(1)

# Write to a Unity Catalog Volume path as CSV
emp_df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", ",") \
  .csv("/Volumes/demo_catalog/demo_schema/demo_raw/employees1.csv")

table_name = "demo_catalog.demo_schema.departments"   # example: catalog.schema.table

# Load table as DataFrame
dept_df = spark.table(table_name)
dept_df.coalesce(1)

# Write to a Unity Catalog Volume path as CSV
dept_df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", ",") \
  .csv("/Volumes/demo_catalog/demo_schema/demo_raw/departments1.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- External CSV table (reads data from S3 in-place)
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS demo_catalog.demo_schema.DEPARTMENT
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://databricks02ar/demo/departments.csv',  -- S3 path to the CSV file
# MAGIC   header 'true',       -- First line of CSV has column names
# MAGIC   inferSchema 'true'   
# MAGIC );
# MAGIC
# MAGIC /*
# MAGIC CREATE TABLE redbird.exdb.students_marks (
# MAGIC   student_id STRING,
# MAGIC   name STRING,
# MAGIC   maths INT,
# MAGIC   science INT,
# MAGIC   english INT
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header true
# MAGIC )
# MAGIC LOCATION '/Volumes/devdb/hr/school/students_marks.csv';
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ###1) Join: List employees with department names
# MAGIC Question: Return emp_name, dept_name, and manager_name (self-join on employees).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.employee_id,
# MAGIC        e.employee_name,
# MAGIC        e.job_title,
# MAGIC        e.manager_id,
# MAGIC        m.employee_name AS manager_name
# MAGIC FROM demo_catalog.demo_schema.employees e
# MAGIC LEFT JOIN demo_catalog.demo_schema.departments d
# MAGIC   ON (e.department_id = d.department_id)
# MAGIC LEFT JOIN demo_catalog.demo_schema.employees m
# MAGIC   ON (e.manager_id = m.employee_id)
# MAGIC   limit 5;

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

emp_df = spark.read.csv('s3://databricks02ar/demo/employees.csv', header=True, inferSchema=True)
emp_df.show(n=5, truncate=False)


# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

emp_df.describe().show()


# COMMAND ----------

emp_df = emp_df.select("employee_id","employee_name", "job_title", "department_id", "manager_id")

emp_df.show(n=5, truncate=False)


# COMMAND ----------

# DBTITLE 1,Cell 8
from pyspark.sql.functions import coalesce, lit, when, col, lower, trim

a = emp_df.alias("a")
b = emp_df.alias("b")

manager_df = a.join(b, a.manager_id == b.employee_id, how="left")
manager_df = manager_df.select(
    a.employee_id,
    a.employee_name,
    a.job_title,
    a.department_id,
    coalesce(a.manager_id.cast("string"), lit("")).alias("manager_id"),
    coalesce(b.employee_name, lit("")).alias("manager_name")
)
filtered_df = manager_df.filter((trim(col("manager_name")) != ""))
filtered_df.show(n=50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2) Aggregation + conditional: Department headcount & average salary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.department_name,
# MAGIC        COUNT(*) AS headcount,
# MAGIC        ROUND(AVG(e.salary), 2) AS avg_salary,
# MAGIC        SUM(CASE WHEN e.salary >= 100000 THEN 1 ELSE 0 END) AS high_earners
# MAGIC FROM demo_catalog.demo_schema.departments d
# MAGIC JOIN demo_catalog.demo_schema.employees e ON e.department_id = d.department_id
# MAGIC GROUP BY d.department_name
# MAGIC ORDER BY headcount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Export table data into CSV file

# COMMAND ----------

# DBTITLE 1,Cell 14

from pyspark.sql.functions import col, avg, sum, when, count

emp_df = emp_df.select("department_id", "salary")
dept_df = dept_df.select("department_id", "department_name")

agg_df = emp_df.groupBy("department_id").agg(
    count("*").alias("count"),
    avg(col("salary")).alias("avg_salary"),
    sum(when(col("salary") > 100000, 1).otherwise(0)).alias("high_earners")
).orderBy(col("count").desc())

join_df = dept_df.join(agg_df, dept_df.department_id == agg_df.department_id, how="inner")
join_df.show(n=5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###👉 Basic SQL Queries and Solution 

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹 Rank() - Top 2 earners in each department

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE_EMP AS (
# MAGIC SELECT employee_name,
# MAGIC        department_id,
# MAGIC        salary,
# MAGIC        RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS sal_rank
# MAGIC FROM demo_catalog.demo_schema.employees
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM CTE_EMP
# MAGIC WHERE sal_rank <= 2;

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

#Employee Dataframe
#emp_df = spark.read.csv('/Volumes/demo_catalog/demo_schema/demo_raw/employees.csv', header=True, inferSchema=True)
emp_df = spark.table('demo_catalog.demo_schema.employees')

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

emp_df = emp_df.select("employee_name", "department_id", "salary")

w = Window.partitionBy("department_id").orderBy(F.col("salary").desc())
ranked_df = emp_df.withColumn("dept_rank", F.dense_rank().over(w))
final_df = ranked_df.filter(F.col("dept_rank") <= 2)
final_df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹 Remove Deduplicate: keep the latest by hire_date
# MAGIC Question: If an employee appears multiple times (dirty data), keep the most recent record.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assume duplicates in employees_dupe (same emp_id, different hire_date)
# MAGIC CREATE OR REPLACE TABLE demo_catalog.demo_schema.employees_dupe AS 
# MAGIC select 1 as emp_id, 'Ravi' as emp_name, 10000 as salary, '2022-01-01' as hire_date
# MAGIC union ALL 
# MAGIC select 2 as emp_id, 'Soni' as emp_name, 20000 as salary, '2021-02-04' as hire_date
# MAGIC union ALL
# MAGIC select 3 as emp_id, 'Moni' as emp_name, 30000 as salary, '2023-04-06' as hire_date
# MAGIC union ALL
# MAGIC select 1 as emp_id, 'Ravi' as emp_name, 10000 as salary, '2022-01-01' as hire_date
# MAGIC union ALL
# MAGIC select 2 as emp_id, 'Soni' as emp_name, 20000 as salary, '2021-02-04' as hire_date
# MAGIC union ALL
# MAGIC select 1 as emp_id, 'Ravi' as emp_name, 10000 as salary, '2022-01-01' as hire_date;
# MAGIC
# MAGIC SELECT * FROM demo_catalog.demo_schema.employees_dupe;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE demo_catalog.demo_schema.employees_dupe AS
# MAGIC SELECT emp_id, emp_name, salary, hire_date
# MAGIC FROM (
# MAGIC   SELECT emp_id, emp_name, salary, hire_date,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY emp_id, emp_name, salary, hire_date
# MAGIC            ORDER BY hire_date DESC
# MAGIC          ) AS rn
# MAGIC   FROM demo_catalog.demo_schema.employees_dupe
# MAGIC ) t
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC SELECT * FROM demo_catalog.demo_schema.employees_dupe;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeesDF").getOrCreate()

# Sample data
data = [
    (2, "Soni", 20000, "2021-02-04"),
    (3, "Moni", 30000, "2023-04-06"),
    (2, "Soni", 20000, "2021-02-04"),
    (1, "Ravi", 10000, "2022-01-01"),
    (1, "Ravi", 10000, "2022-01-01"),
    (1, "Ravi", 10000, "2022-01-01")
]

# Define schema
columns = ["emp_id", "emp_name", "salary", "hire_date"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# COMMAND ----------

df = df.distinct().orderBy("emp_id")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) ) Gaps & Islands: contiguous order sessions by 30‑minute gap
# MAGIC Platform notes:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_catalog.demo_schema.orders
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://databricks02ar/demo/orders/',
# MAGIC   inferSchema 'true',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC /*CREATE TABLE IF NOT EXISTS demo_catalog.demo_schema.orders
# MAGIC (order_id integer,
# MAGIC  customer_id integer,
# MAGIC  order_dt timestamp,
# MAGIC  amount double 
# MAGIC )
# MAGIC USING CSV
# MAGIC location '/Volumes/demo_catalog/demo_schema/demo_raw/orders/';
# MAGIC
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ordered AS (
# MAGIC   SELECT o.*,
# MAGIC          LAG(order_dt) OVER (PARTITION BY customer_id ORDER BY order_dt) AS prev_dt
# MAGIC   FROM demo_catalog.demo_schema.orders o
# MAGIC ),
# MAGIC flagged AS (
# MAGIC   SELECT *,
# MAGIC          CASE WHEN prev_dt IS NULL OR TIMESTAMPDIFF(minute, prev_dt, order_dt) > 30
# MAGIC               THEN 1 ELSE 0 END AS new_session_flag
# MAGIC   FROM ordered
# MAGIC ),
# MAGIC sessioned AS (
# MAGIC   SELECT *,
# MAGIC          SUM(new_session_flag) OVER (PARTITION BY customer_id ORDER BY order_dt
# MAGIC                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
# MAGIC   FROM flagged
# MAGIC )
# MAGIC SELECT customer_id, session_id,
# MAGIC        MIN(order_dt) AS session_start,
# MAGIC        MAX(order_dt) AS session_end,
# MAGIC        COUNT(*)      AS orders_in_session,
# MAGIC        SUM(amount)   AS session_amount
# MAGIC FROM sessioned
# MAGIC GROUP BY customer_id, session_id
# MAGIC ORDER BY customer_id, session_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ordered AS (
# MAGIC   SELECT o.*,
# MAGIC          LAG(order_dt) OVER (PARTITION BY customer_id ORDER BY order_dt) AS prev_dt
# MAGIC   FROM demo_catalog.demo_schema.orders o
# MAGIC ),
# MAGIC flagged AS (
# MAGIC   SELECT *,
# MAGIC          CASE WHEN prev_dt IS NULL OR TIMESTAMPDIFF(minute, prev_dt, order_dt) > 30
# MAGIC               THEN 1 ELSE 0 END AS new_session_flag
# MAGIC   FROM ordered
# MAGIC ),
# MAGIC sessioned AS (
# MAGIC   SELECT *,
# MAGIC          SUM(new_session_flag) OVER (PARTITION BY customer_id ORDER BY order_dt
# MAGIC                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
# MAGIC   FROM flagged
# MAGIC )
# MAGIC SELECT customer_id, session_id,
# MAGIC        MIN(order_dt) AS session_start,
# MAGIC        MAX(order_dt) AS session_end,
# MAGIC        COUNT(*)      AS orders_in_session,
# MAGIC        SUM(amount)   AS session_amount
# MAGIC FROM sessioned
# MAGIC GROUP BY customer_id, session_id
# MAGIC ORDER BY customer_id, session_start;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 34
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window
from pyspark.sql import functions as F

order_df = order_df_org.select("order_id", "customer_id", "order_dt", "amount")

w = Window.partitionBy("customer_id").orderBy(col("order_dt").desc())
order_df = order_df.withColumn("prev_dt", lag(col("order_dt"), 1).over(w))

# Compute time difference in minutes
order_df = order_df.withColumn(
    "diff_minutes",
    (F.unix_timestamp("prev_dt") - F.unix_timestamp("order_dt")) / 60
)
# Flag new session if gap > 30 minutes
order_df = order_df.withColumn(
    "session_flag",
    F.when(F.col("diff_minutes") > 30, 1).otherwise(0)
)

# Compute sum of new session
w1 = Window.partitionBy("customer_id").orderBy("order_dt") \
           .rowsBetween(Window.unboundedPreceding, Window.currentRow)

order_df = order_df.withColumn(
    "session_id",
    F.sum("session_flag").over(w1)
)

order_df = order_df.groupBy("customer_id", "session_id").agg(
    F.count("*").alias("orders_in_session"),
    F.sum("amount").alias("session_amount"),
    F.max("order_dt").alias("session_end"),
    F.min("order_dt").alias("session_start")
).orderBy("customer_id", "session_start")
         
order_df.show(n=50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ###7) Running totals & 7‑day moving average (time series)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily revenue per customer
# MAGIC WITH daily AS (
# MAGIC   SELECT DATE(order_dt) AS day,
# MAGIC          customer_id,
# MAGIC          SUM(amount) AS revenue
# MAGIC   FROM demo_catalog.demo_schema.orders
# MAGIC   GROUP BY day, customer_id
# MAGIC ),
# MAGIC calc AS (
# MAGIC   SELECT day,
# MAGIC          customer_id,
# MAGIC          revenue,
# MAGIC          SUM(revenue) OVER (PARTITION BY customer_id ORDER BY day) AS running_total,
# MAGIC          AVG(revenue) OVER (PARTITION BY customer_id ORDER BY day
# MAGIC                             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7day
# MAGIC   FROM daily
# MAGIC )
# MAGIC SELECT * FROM calc ORDER BY customer_id, day;

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, date_format, col, sum, avg

order_df = spark.read.csv("/Volumes/demo_catalog/demo_schema/demo_raw/orders", header=True, inferSchema=True)

order_df = order_df.select(date_format("order_dt", "yyyy-MM-dd").alias("day"), "customer_id", col("amount").alias("revenue"))

order_df = order_df.groupBy("day", "customer_id").agg(F.sum("revenue").alias("revenue")).orderBy("customer_id",)

w = Window.partitionBy("customer_id").orderBy(col("day").desc())
order_df = order_df.withColumn("running_total", sum(col("revenue")).over(w))
order_df = order_df.withColumn("ma_7day", avg(col("revenue")).over(Window.partitionBy("customer_id").orderBy(col("day")).rowsBetween(-6, 0)))


# Compute time difference in minutes

order_df.show(n=5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ###8) Find Missing Dates (GENERATOR)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Key Difference
# MAGIC • Snowflake → uses SEQ4() and GENERATOR.
# MAGIC • Databricks → uses `sequence) + explode() to generate ranges.
# MAGIC */
# MAGIC WITH date_series AS (
# MAGIC   SELECT explode(
# MAGIC            sequence(
# MAGIC              (SELECT MIN(order_dt) FROM demo_catalog.demo_schema.orders),
# MAGIC              (SELECT MAX(order_dt) FROM demo_catalog.demo_schema.orders),
# MAGIC              interval 1 day
# MAGIC            )
# MAGIC          ) AS dt
# MAGIC   
# MAGIC   /*SELECT DATEADD(
# MAGIC            day,
# MAGIC            SEQ4(),
# MAGIC            (SELECT MIN(order_dt) FROM redbird.exdb.orders)
# MAGIC          ) AS dt
# MAGIC   FROM TABLE(GENERATOR(ROWCOUNT => 1000))   -- constant value*/
# MAGIC )
# MAGIC SELECT o.ORDER_ID, o.CUSTOMER_ID, o.AMOUNT, TO_VARCHAR(dt, 'YYYY-MM-DD') AS missing_order_dt
# MAGIC FROM date_series ds
# MAGIC LEFT JOIN demo_catalog.demo_schema.orders o
# MAGIC   ON TO_VARCHAR(ds.dt, 'YYYY-MM-DD') = TO_VARCHAR(o.order_dt, 'YYYY-MM-DD')
# MAGIC WHERE ds.dt <= (SELECT MAX(order_dt) FROM demo_catalog.demo_schema.orders)
# MAGIC   AND o.order_dt IS NULL
# MAGIC ORDER BY dt;