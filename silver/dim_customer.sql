-- Step 1: Create dimension table
CREATE TABLE IF NOT EXISTS tp_silver.dim_customer (
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    customer_region STRING,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    batch_id STRING
)
USING DELTA;

-- Step 2: Load initial batch
INSERT INTO tp_silver.dim_customer
WITH staged_customers AS (
    SELECT
        md5(CONCAT(customer_email, '-', customer_region)) AS customer_id,
        customer_name,
        customer_email,
        customer_region,
        DATE('2025-07-11') AS valid_from,
        DATE('9999-12-31') AS valid_to,
        true AS is_current,
        current_timestamp() AS created_at,
        'batch_20250711' AS batch_id
    FROM tp_bronze.bronze_raw_sales_feed
    WHERE customer_email IS NOT NULL
)
SELECT * FROM staged_customers;
