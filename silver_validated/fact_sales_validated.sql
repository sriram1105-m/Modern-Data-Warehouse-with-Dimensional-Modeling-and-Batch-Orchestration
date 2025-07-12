-- Step 1: Validate records for nulls or inconsistencies
CREATE OR REPLACE TEMP VIEW fact_sales_validated_vw AS
SELECT *,
       md5(concat_ws('|', order_id, order_date, customer_id, product_id, date_key)) AS row_hash
FROM tp_silver.fact_sales
WHERE customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND date_key IS NOT NULL;

-- Step 2: Load into validated table
INSERT INTO tp_silver_validated.fact_sales_validated
SELECT * FROM fact_sales_validated_vw;
