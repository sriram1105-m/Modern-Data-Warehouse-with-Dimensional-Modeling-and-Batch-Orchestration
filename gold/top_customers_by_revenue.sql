-- Step 1: Calculate total revenue by customer
CREATE OR REPLACE TEMP VIEW top_customers_by_revenue_vw AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  ROUND(SUM(f.unit_price * f.quantity), 2) AS total_spent,
  RANK() OVER (ORDER BY SUM(f.unit_price * f.quantity) DESC) AS revenue_rank
FROM tp_silver_validated.fact_sales_validated f
JOIN tp_silver_validated.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_email;

-- Step 2: Load into gold table
INSERT INTO tp_gold.top_customers_by_revenue
SELECT * FROM top_customers_by_revenue_vw;
