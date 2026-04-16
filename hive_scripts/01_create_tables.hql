-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Hive
-- File   : 01_create_tables.hql
-- Purpose: Create Hive database, external tables, and partitioned
--          tables for analytics
-- Run    : hive -f hive_scripts/01_create_tables.hql
-- ═══════════════════════════════════════════════════════════════

-- ─── Create Database ──────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS retail_analytics
COMMENT 'Smart Retail Analytics Platform Database'
LOCATION '/retail_platform/hive_data'
WITH DBPROPERTIES ('creator'='team', 'date'='2024-01-01');

USE retail_analytics;

-- ════════════════════════════════════════════════════════════════
-- TABLE 1: Main Retail Transactions (External, points to HDFS)
-- ════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS retail_transactions;

CREATE EXTERNAL TABLE retail_transactions (
    user_id       STRING     COMMENT 'Customer ID (U + numeric)',
    product_id    STRING     COMMENT 'Product ID (P + hex code)',
    product_name  STRING     COMMENT 'Product name',
    category      STRING     COMMENT 'Product category',
    price         DOUBLE     COMMENT 'Unit price in GBP',
    quantity      INT        COMMENT 'Quantity purchased',
    total_revenue DOUBLE     COMMENT 'price × quantity',
    ts            STRING     COMMENT 'Transaction timestamp (YYYY-MM-DD HH:MM:SS)',
    year_month    STRING     COMMENT 'YYYY-MM for partitioning',
    year          STRING     COMMENT 'YYYY',
    city          STRING     COMMENT 'Customer city',
    gender        STRING     COMMENT 'Customer gender (M/F)',
    rating        DOUBLE     COMMENT 'Product rating (1.0–5.0)',
    price_tier    STRING     COMMENT 'Budget / Mid / Premium'
)
COMMENT 'Cleaned retail transactions from UCI Online Retail II dataset'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/retail_platform/processed/cleaned_retail'
TBLPROPERTIES ('skip.header.line.count'='0');

-- ════════════════════════════════════════════════════════════════
-- TABLE 2: Partitioned Table (by year_month) for faster queries
-- ════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS retail_partitioned;

CREATE TABLE retail_partitioned (
    user_id       STRING,
    product_id    STRING,
    product_name  STRING,
    category      STRING,
    price         DOUBLE,
    quantity      INT,
    total_revenue DOUBLE,
    ts            STRING,
    year          STRING,
    city          STRING,
    gender        STRING,
    rating        DOUBLE,
    price_tier    STRING
)
PARTITIONED BY (year_month STRING)
CLUSTERED BY (category) INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;

-- Populate partitioned table from external table
INSERT OVERWRITE TABLE retail_partitioned
PARTITION (year_month)
SELECT
    user_id, product_id, product_name, category,
    price, quantity, total_revenue, ts, year,
    city, gender, rating, price_tier, year_month
FROM retail_transactions
WHERE user_id IS NOT NULL AND price > 0;

-- ════════════════════════════════════════════════════════════════
-- TABLE 3: Product Summary (aggregated)
-- ════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS product_summary;

CREATE TABLE product_summary AS
SELECT
    product_id,
    product_name,
    category,
    COUNT(*)                        AS num_orders,
    SUM(quantity)                   AS total_qty,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(price), 2)            AS avg_price,
    ROUND(AVG(rating), 2)           AS avg_rating
FROM retail_transactions
GROUP BY product_id, product_name, category;

-- ════════════════════════════════════════════════════════════════
-- TABLE 4: Customer Summary
-- ════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS customer_summary;

CREATE TABLE customer_summary AS
SELECT
    user_id,
    COUNT(DISTINCT product_id)      AS unique_products,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(total_revenue), 2)    AS total_spend,
    ROUND(AVG(total_revenue), 2)    AS avg_order_value,
    MAX(ts)                         AS last_purchase,
    city,
    gender,
    CASE
        WHEN SUM(total_revenue) >= 5000 THEN 'VIP'
        WHEN SUM(total_revenue) >= 1000 THEN 'High_Value'
        WHEN SUM(total_revenue) >= 200  THEN 'Regular'
        ELSE 'Occasional'
    END                             AS segment
FROM retail_transactions
GROUP BY user_id, city, gender;

-- Verify tables
SHOW TABLES;
DESCRIBE retail_transactions;
SELECT COUNT(*) AS total_rows FROM retail_transactions;
