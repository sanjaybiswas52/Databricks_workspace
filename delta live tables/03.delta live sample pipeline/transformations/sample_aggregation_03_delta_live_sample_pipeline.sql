
CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
COMMENT "Raw orders ingested from JSON files"
AS
SELECT *
FROM cloud_files(
  "s3://sanjaydatabricks01/json/orders/orders.json",
  "json"
);

CREATE OR REFRESH LIVE TABLE silver_orders
COMMENT "Cleaned orders with quality rules"
--CONSTRAINT amount_positive EXPECT (amount > 0) ON VIOLATION DROP
--CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)
AS
SELECT
  order_id,
  customer_id,
  amount,
  CAST(order_date AS DATE) AS order_date
FROM LIVE.bronze_orders;