# Databricks notebook source
# MAGIC %md
# MAGIC ###👉 Type of Joins

# COMMAND ----------

# Employee Table
emp_data = [
    (1, "John", 10,"2010-10-01", 4000),
    (2, "Jane", 10, "2009-01-31", 6000),
    (3, "Mike", 20, "2010-03-01",7000),
    (4, "Sam", 20, "2011-07-31",3000),
    (5, "Ravi", 30, "2006-12-31",8000),
    (6, "Vishal", 40, "2010-04-30",8000)
]

emp_cols = ["emp_id", "emp_name", "dept_id", "hire_date", "salary"]

emp_df = spark.createDataFrame(emp_data, emp_cols)


# Department Table
dept_data = [
    (10, "HR", "India"),
    (20, "IT", "India"),
    (30, "Finance", "India")
]

dept_cols = ["dept_id", "dept_name", "loc_id"]

dept_df = spark.createDataFrame(dept_data, dept_cols)

emp_df.createOrReplaceTempView("employee")
dept_df.createOrReplaceTempView("department")


# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Inner Join
# MAGIC <br>between "employee" and "department" table</br>
# MAGIC <b>distributed by
# MAGIC <ul><li>Send all HR rows → same partition
# MAGIC <li>All IT rows → another partition
# MAGIC <li>All Finance rows → another partition

# COMMAND ----------

# MAGIC %sql
# MAGIC select e.emp_id, e.emp_name, e.salary, e.dept_id, d.dept_name
# MAGIC from employee e
# MAGIC   inner join department d on (e.dept_id = d.dept_id and d.dept_id = 20)
# MAGIC distribute by e.dept_id

# COMMAND ----------

join_df = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner") \
  .select(emp_df.emp_id, emp_df.emp_name, emp_df.salary, emp_df.dept_id, dept_df.dept_name) \
    .filter(emp_df.dept_id == 20)

display(join_df)



# COMMAND ----------

# MAGIC %md
# MAGIC <b> Select is used for remove duplicate column name which has been used in join condition "dept_id".

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Left Outer Join
# MAGIC All records from the left table and the matching records from the right table.

# COMMAND ----------

# MAGIC %sql
# MAGIC select e.emp_id, e.emp_name, e.salary, e.dept_id, d.dept_name
# MAGIC from employee e
# MAGIC   left join department d on (e.dept_id = d.dept_id and d.dept_id = 20)
# MAGIC distribute by e.dept_id

# COMMAND ----------


from pyspark.sql.functions import col

join_df = emp_df.alias("e").join( dept_df.alias("d"),
    (col("e.dept_id") == col("d.dept_id")) & (col("d.dept_id") == 20), "left") \
    .select(
    col("e.emp_id"),
    col("e.emp_name"),
    col("e.salary"),
    col("e.dept_id"),
    col("d.dept_name")
)

join_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Right Outer
# MAGIC All records from the right table and the matching records from the left table.

# COMMAND ----------

# MAGIC %sql
# MAGIC select e.emp_id, e.emp_name, e.salary, e.dept_id, d.dept_name
# MAGIC from employee e
# MAGIC   right join department d on (e.dept_id = d.dept_id and d.dept_id = 20)
# MAGIC distribute by e.dept_id

# COMMAND ----------

from pyspark.sql.functions import col

filter_dept = dept_df.filter(dept_df.dept_id == 20)

join_df = emp_df.alias("e").join(filter_dept.alias("d"), col("e.dept_id") == col("d.dept_id"), "right") \
  .select(emp_df.emp_id, emp_df.emp_name, emp_df.salary, emp_df.dept_id, filter_dept.dept_name)

display(join_df)

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

# MAGIC %md
# MAGIC #####(a) left enti join
# MAGIC PySpark (EXISTS equivalent to LEFT_SEMI JOIN)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM department d
# MAGIC     WHERE e.dept_id = d.dept_id
# MAGIC     AND d.loc_id LIKE 'Ind%'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ PySpark (EXISTS equivalent to LEFT_SEMI JOIN)

# COMMAND ----------

from pyspark.sql.functions import col

result_df = emp_df.join(
    dept_df.filter(col("loc_id").like("Ind%")),
    emp_df.dept_id == dept_df.dept_id,
    "left_semi"
)

result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####(b) right enti join
# MAGIC NOT EXISTS equivalent to LEFT_ANTI JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM department d
# MAGIC     WHERE e.dept_id = d.dept_id
# MAGIC     AND d.loc_id LIKE 'Ind%'
# MAGIC )

# COMMAND ----------

result_df = emp_df.join(
    dept_df.filter(col("loc_id").like("Ind%")),
    emp_df.dept_id == dept_df.dept_id,
    "left_anti"
)

result_df.show()

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

# COMMAND ----------

# MAGIC %md
# MAGIC ###👉 Decode in Pyspark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, quarter
from pyspark.sql.window import Window

#df = emp_df.withColumn("DISTRIBUTION_YR", F.quarter("hire_date"))
fund_window_spec = Window.partitionBy(F.year("hire_date")).orderBy("hire_date")

#df = df.withColumn("DISTRIBUTION_YR", F.sum("salary").over(fund_window_spec))
df = emp_df.withColumn("QTR_DISTRIBUTION", F.quarter("hire_date")) \
        .withColumn("YEAR", F.year("hire_date")) \
        .withColumn(
        "DITRIBUTION_YR",
        F.when (
            F.quarter("hire_date").isin([1,2,3]), F.col("salary")) \
        .otherwise(
            F.sum("salary").over(fund_window_spec)
                    )
                )
        



display(df)
