-- Step 1: Calculate monthly revenue by channel
CREATE OR REPLACE TEMP VIEW monthly_revenue_by_channel_vw AS
SELECT
  year(order_date) AS year,
  month(order_date) AS month,
  channel,
  ROUND(SUM(unit_price * quantity), 2) AS total_revenue
FROM tp_silver_validated.fact_sales_validated
GROUP BY year(order_date), month(order_date), channel;

-- Step 2: Load into gold table
INSERT INTO tp_gold.monthly_revenue_by_channel
SELECT * FROM monthly_revenue_by_channel_vw;
