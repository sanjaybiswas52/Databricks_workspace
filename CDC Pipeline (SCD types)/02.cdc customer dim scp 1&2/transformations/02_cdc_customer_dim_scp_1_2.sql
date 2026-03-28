-- ---------------------------------------
-- 1/ Ingesting data with Autoloader
-- ---------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_db.customers_cdc
COMMENT "New customer data incrementally ingested from cloud object storage landing zone"
AS SELECT *, 'DELETE' AS operation
FROM STREAM read_files(
  "s3://sanjaydatabricks01/json/customers/",
  format => "json",
  inferColumnTypes => true
);

-- --------------------------------------------------
-- 2/ Cleanup & expectations to track data quality
-- --------------------------------------------------
-- this could also be a VIEW
CREATE OR REFRESH STREAMING TABLE bronze_db.customers_cdc_clean
(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (operation IN ('APPEND', 'DELETE', 'UPDATE')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_json_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type"
AS SELECT * 
FROM STREAM(bronze_db.customers_cdc);

-- ---------------------------------------------------------
-- 3/ Materializing the silver table with APPLY CHANGES
-- ---------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE gold_db.customers
  COMMENT "Clean, materialized customers";

CREATE FLOW customers_cdc_flow AS
AUTO CDC INTO gold_db.customers
FROM stream(bronze_db.customers_cdc_clean)
KEYS (id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY id --primary key, auto-incrementing ID of any kind that can be used to identity order of events, or timestamp
COLUMNS * EXCEPT (operation, _rescued_data)
STORED AS SCD TYPE 1;

-- -----------------------------------------------------
  -- 4/ Slowly Changing Dimension of type 2 (SCD2)
  -- -----------------------------------------------------
  -- create the table
CREATE OR REFRESH STREAMING TABLE gold_db.SCD2_customers
  COMMENT "Slowly Changing Dimension Type 2 for customers";

-- store all changes as SCD2
CREATE FLOW SCD2_customers_cdc_flow AS
AUTO CDC INTO gold_db.SCD2_customers
FROM stream(bronze_db.customers_cdc_clean)
KEYS (id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY email
COLUMNS * EXCEPT (operation, _rescued_data)
STORED AS SCD TYPE 2;