-- Step 1: Prepare the staging layer for the fact table
WITH fact_staging AS (
  SELECT
    r.order_id,
    r.order_date,
    c.customer_id,
    p.product_id,
    d.date_key,
    r.channel,
    r.payment_method,
    r.unit_price,
    r.quantity,
    r.total_price,
    current_timestamp() AS created_at,
    'batch_20250711' AS batch_id
  FROM tp_bronze.bronze_raw_sales_feed r
  JOIN tp_silver.dim_customer c
    ON md5(concat(r.customer_email, '-', r.customer_region)) = c.customer_id
  JOIN tp_silver.dim_product p
    ON md5(concat(r.product_name, '-', r.brand)) = p.product_id
  JOIN tp_silver.dim_date d
    ON r.order_date = d.order_date
)

-- Step 2: Insert into fact_sales table
SELECT * FROM fact_staging;
