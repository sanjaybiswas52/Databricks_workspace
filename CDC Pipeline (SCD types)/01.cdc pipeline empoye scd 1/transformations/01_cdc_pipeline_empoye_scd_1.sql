-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

----------------------------------------------------
-- STEP 1: CDC FILES → BRONZE RAW
----------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_db.employees_bronze_raw
COMMENT "Raw employee CDC feed"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed" = false
)
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS raw_file_name
FROM STREAM read_files(
  's3://sanjaydatabricks01/csv/demo/emp/',
  format => 'CSV',
  header => 'true'
);
----------------------------------------------------
-- STEP 2: BRONZE CLEAN (CDC AWARE)
----------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_db.employees_bronze_clean
(
  CONSTRAINT valid_employee_id
    EXPECT (employee_id IS NOT NULL)
    ON VIOLATION FAIL UPDATE,

  CONSTRAINT valid_operation
    EXPECT (operation IN ('INSERT','UPDATE','DELETE'))
    ON VIOLATION DROP ROW
)
COMMENT "Validated employee CDC data"
AS
SELECT
  employee_id,
  employee_name,
  job_title,
  department_id,
  CAST(hire_date AS DATE) AS hire_date,
  salary,
  operation,
  to_timestamp(hire_date, 'yyyy-MM-dd HH:mm:ss') AS event_timestamp,
  CAST(updated_at AS TIMESTAMP) AS updated_at ,
  ingestion_time,
  raw_file_name
FROM STREAM bronze_db.employees_bronze_raw;

----------------------------------------------------
-- STEP 3: SILVER (SCD TYPE 1 WITH CDC)
----------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_db.employees_scd_1
COMMENT "Employee SCD Type 1 (latest state)";

CREATE FLOW employees_scd1_flow AS
AUTO CDC INTO silver_db.employees_scd_1
FROM STREAM bronze_db.employees_bronze_clean
KEYS (employee_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY updated_at
COLUMNS * EXCEPT ( operation, raw_file_name) -- exlude columns 
STORED AS SCD TYPE 1;
