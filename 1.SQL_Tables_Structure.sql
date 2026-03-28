-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Create ICEBERG table with partition

-- COMMAND ----------


CREATE TABLE workspace.default.orders_iceberg (
    order_id        BIGINT,
    order_date      DATE,
    country         STRING,
    customer_id     BIGINT,
    amount          DECIMAL(18,2)
)
USING iceberg
PARTITIONED BY (order_date);
-- CLUSTERED BY (customer_id) INTO 32 BUCKETS;
--  `Bucketing in Databricks Runtime` is not supported for Delta tables

-- COMMAND ----------


CREATE TABLE workspace.default.orders_iceberg2 (
    order_id        BIGINT,
    order_ts        TIMESTAMP,
    order_date      DATE,
    country         STRING,
    customer_id     BIGINT,
    amount          DECIMAL(18,2)
)
USING iceberg
PARTITIONED BY (months(order_date), country)
TBLPROPERTIES (
    'write.order-by' = 'country, order_date, customer_id',
    'write.target-file-size-bytes' = '536870912'  -- 512 MB, optional
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Alter PARTITION from month to year
-- MAGIC <p>Delta tables do not support altering partitioning fields after table creation. You cannot drop or change partition fields using this syntax for an existing Delta table</p>
-- MAGIC <br>To change the partitioning of the table, you must create a new table with the desired partition structure and migrate your data if necessary.

-- COMMAND ----------


ALTER TABLE workspace.default.orders_iceberg2
DROP PARTITION FIELD months(order_date);


-- COMMAND ----------


ALTER TABLE workspace.default.orders_iceberg2
ADD PARTITION FIELD (year(order_date));



-- COMMAND ----------


CREATE TABLE workspace.default.orders_iceberg2 (
    order_id        BIGINT,
    order_ts        TIMESTAMP,
    order_date      DATE,
    country         STRING,
    customer_id     BIGINT,
    amount          DECIMAL(18,2)
)
USING iceberg
PARTITIONED BY (order_date, country)
TBLPROPERTIES (
    'write.order-by' = 'country, order_date, customer_id',
    'write.target-file-size-bytes' = '536870912'  -- 512 MB, optional
);


-- COMMAND ----------

use catalog udemy;
use schema f1_processed;
show tables;

-- COMMAND ----------

SELECT * FROM f1_presentation.race_results limit 10

-- COMMAND ----------

-- DBTITLE 1,Read Delta table correctly
-- MAGIC %python
-- MAGIC #df = spark.read.format('csv').load('/Volumes/demo_catalog/demo_schema/demo_raw/orders/',header=True,inferSchema=True)
-- MAGIC
-- MAGIC df = spark.read.format('json').load('s3://sanjaydatabricks01/json/orders/')
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

select * from demo_catalog.demo_schema.orders

/Volumes/demo_catalog/demo_schema/demo_raw/orders/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##👉 Create Table using flat file

-- COMMAND ----------

/*
CREATE TABLE IF NOT EXISTS redbird.bronze_db.employees_bronze_raw
USING CSV
OPTIONS (
  path 's3://sanjaydatabricks01/csv/demo/emp/',
  header 'true',
  inferSchema 'true'
);
*/

-- COMMAND ----------


select * from  retail_catalog.bronze_db.customers_cdc limit 10;
--SELECT * FROM retail_catalog.bronze_db.customers_cdc_clean;
--SELECT * FROM retail_catalog.gold_db.scd2_customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json('s3://sanjaydatabricks01/json/orders')
-- MAGIC display(df)