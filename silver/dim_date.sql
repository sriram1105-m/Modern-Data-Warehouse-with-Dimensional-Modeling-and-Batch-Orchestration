-- Step 1: Get date range from source data
WITH min_max_date AS (
  SELECT
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
  FROM tp_bronze.bronze_raw_sales_feed
),

-- Step 2: Generate date dimension from min to max date
date_range AS (
  SELECT explode(sequence(min_date, max_date, interval 1 day)) AS order_date
  FROM min_max_date
)

-- Step 3: Build dimension table
SELECT
  CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_key,
  order_date,
  day(order_date) AS day,
  month(order_date) AS month,
  year(order_date) AS year,
  weekofyear(order_date) AS week,
  quarter(order_date) AS quarter,
  CASE WHEN dayofweek(order_date) IN (1, 7) THEN true ELSE false END AS is_weekend,
  current_timestamp() AS created_at,
  'batch_20250711' AS batch_id
FROM date_range;
