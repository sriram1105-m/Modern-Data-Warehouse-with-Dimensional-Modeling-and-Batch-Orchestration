-- Step 1: Summarize brand performance
CREATE OR REPLACE TEMP VIEW brand_performance_summary_vw AS
SELECT
  p.brand,
  SUM(f.quantity) AS total_units_sold,
  ROUND(SUM(f.unit_price * f.quantity), 2) AS total_revenue,
  ROUND(AVG(f.unit_price), 2) AS avg_unit_price
FROM tp_silver_validated.fact_sales_validated f
JOIN tp_silver_validated.dim_product p
  ON f.product_id = p.product_id
GROUP BY p.brand;

-- Step 2: Load into gold table
INSERT INTO tp_gold.brand_performance_summary
SELECT * FROM brand_performance_summary_vw;
