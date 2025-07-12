-- SQL to load raw sales feed into bronze layer
 
CREATE SCHEMA IF NOT EXISTS tp_bronze;
CREATE SCHEMA IF NOT EXISTS tp_silver;
CREATE SCHEMA IF NOT EXISTS tp_silver_validated;
CREATE SCHEMA IF NOT EXISTS tp_gold;

ALTER TABLE default.bronze_raw_sales_feed
  RENAME TO tp_bronze.bronze_raw_sales_feed;
