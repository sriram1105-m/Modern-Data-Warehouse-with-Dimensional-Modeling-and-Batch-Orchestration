-- Step 1: Extract unique products and stage for dim_product
WITH staged_products AS (
  SELECT
    md5(CONCAT(product_name, '-', brand)) AS product_id,
    product_name,
    brand,
    current_timestamp() AS created_at,
    'batch_20250711' AS batch_id
  FROM (
    SELECT DISTINCT product_name, brand
    FROM tp_bronze.bronze_raw_sales_feed
    WHERE product_name IS NOT NULL AND brand IS NOT NULL
  )
)

-- Step 2: Create table if not exists and load into dim_product
MERGE INTO tp_silver.dim_product AS target
USING staged_products AS source
ON target.product_id = source.product_id
WHEN NOT MATCHED THEN
  INSERT (
    product_id,
    product_name,
    brand,
    created_at,
    batch_id
  )
  VALUES (
    source.product_id,
    source.product_name,
    source.brand,
    source.created_at,
    source.batch_id
  );
