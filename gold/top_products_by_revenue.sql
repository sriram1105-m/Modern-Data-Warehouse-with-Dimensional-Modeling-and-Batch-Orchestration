-- Step 1: Get top products by revenue
CREATE OR REPLACE TEMP VIEW top_products_by_revenue_vw AS
SELECT
  p.product_name,
  p.brand,
  SUM(f.quantity) AS total_units_sold,
  ROUND(SUM(f.unit_price * f.quantity), 2) AS total_revenue,
  RANK() OVER (ORDER BY SUM(f.unit_price * f.quantity) DESC) AS revenue_rank
FROM tp_silver_validated.fact_sales_validated f
JOIN tp_silver_validated.dim_product p
  ON f.product_id = p.product_id
GROUP BY p.product_name, p.brand;

-- Step 2: Load into gold table
INSERT INTO tp_gold.top_products_by_revenue
SELECT * FROM top_products_by_revenue_vw;
