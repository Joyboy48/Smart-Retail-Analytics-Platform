-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Hive Analytics Queries
-- File   : 02_analytics_queries.hql
-- Purpose: All business intelligence queries for the project
-- Run    : hive -f hive_scripts/02_analytics_queries.hql
-- ═══════════════════════════════════════════════════════════════

USE retail_analytics;

-- Enable optimizations
SET hive.vectorized.execution.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;

-- ════════════════════════════════════════════════════════════════
-- QUERY 1: Top 10 Best-Selling Products by Revenue
-- ════════════════════════════════════════════════════════════════
SELECT '=== TOP 10 PRODUCTS BY REVENUE ===' AS header;

SELECT
    product_name,
    category,
    SUM(quantity)                   AS total_units_sold,
    ROUND(SUM(total_revenue), 2)    AS total_revenue_gbp,
    ROUND(AVG(price), 2)            AS avg_unit_price,
    ROUND(AVG(rating), 2)           AS avg_rating,
    COUNT(DISTINCT user_id)         AS unique_buyers
FROM retail_transactions
GROUP BY product_name, category
ORDER BY total_revenue_gbp DESC
LIMIT 10;

-- ════════════════════════════════════════════════════════════════
-- QUERY 2: Revenue by Category
-- ════════════════════════════════════════════════════════════════
SELECT '=== REVENUE BY CATEGORY ===' AS header;

SELECT
    category,
    COUNT(DISTINCT product_id)      AS unique_products,
    COUNT(DISTINCT user_id)         AS unique_customers,
    SUM(quantity)                   AS total_units_sold,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(price), 2)            AS avg_price,
    ROUND(SUM(total_revenue) * 100.0 /
        SUM(SUM(total_revenue)) OVER(), 2) AS revenue_pct
FROM retail_transactions
GROUP BY category
ORDER BY total_revenue DESC;

-- ════════════════════════════════════════════════════════════════
-- QUERY 3: Monthly Revenue Trends (Growth Analysis)
-- ════════════════════════════════════════════════════════════════
SELECT '=== MONTHLY REVENUE TRENDS ===' AS header;

SELECT
    year_month,
    COUNT(DISTINCT user_id)         AS unique_customers,
    COUNT(*)                        AS num_transactions,
    ROUND(SUM(total_revenue), 2)    AS monthly_revenue,
    ROUND(AVG(total_revenue), 2)    AS avg_order_value,
    LAG(ROUND(SUM(total_revenue),2))
        OVER (ORDER BY year_month)  AS prev_month_revenue,
    ROUND(
        (SUM(total_revenue) - LAG(SUM(total_revenue))
            OVER (ORDER BY year_month)) * 100.0 /
        NULLIF(LAG(SUM(total_revenue)) OVER (ORDER BY year_month), 0),
    2)                              AS mom_growth_pct
FROM retail_transactions
GROUP BY year_month
ORDER BY year_month;

-- ════════════════════════════════════════════════════════════════
-- QUERY 4: High-Value Customers (VIP Segment)
-- ════════════════════════════════════════════════════════════════
SELECT '=== TOP 20 HIGH-VALUE CUSTOMERS ===' AS header;

SELECT
    user_id,
    city,
    gender,
    COUNT(DISTINCT product_id)      AS products_bought,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(total_revenue), 2)    AS total_spend,
    ROUND(AVG(total_revenue), 2)    AS avg_order_value,
    MAX(ts)                         AS last_purchase_date,
    CASE
        WHEN SUM(total_revenue) >= 5000 THEN 'VIP'
        WHEN SUM(total_revenue) >= 1000 THEN 'High_Value'
        WHEN SUM(total_revenue) >= 200  THEN 'Regular'
        ELSE 'Occasional'
    END                             AS segment
FROM retail_transactions
GROUP BY user_id, city, gender
ORDER BY total_spend DESC
LIMIT 20;

-- ════════════════════════════════════════════════════════════════
-- QUERY 5: Customer Segment Distribution
-- ════════════════════════════════════════════════════════════════
SELECT '=== CUSTOMER SEGMENT DISTRIBUTION ===' AS header;

WITH customer_spend AS (
    SELECT
        user_id,
        SUM(total_revenue) AS total_spend
    FROM retail_transactions
    GROUP BY user_id
),
segmented AS (
    SELECT
        CASE
            WHEN total_spend >= 5000 THEN 'VIP'
            WHEN total_spend >= 1000 THEN 'High_Value'
            WHEN total_spend >= 200  THEN 'Regular'
            ELSE 'Occasional'
        END AS segment,
        total_spend
    FROM customer_spend
)
SELECT
    segment,
    COUNT(*)                        AS num_customers,
    ROUND(AVG(total_spend), 2)      AS avg_spend,
    ROUND(SUM(total_spend), 2)      AS total_revenue,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS customer_pct
FROM segmented
GROUP BY segment
ORDER BY total_revenue DESC;

-- ════════════════════════════════════════════════════════════════
-- QUERY 6: Gender-based Purchase Analysis
-- ════════════════════════════════════════════════════════════════
SELECT '=== GENDER-BASED PURCHASE ANALYSIS ===' AS header;

SELECT
    gender,
    category,
    COUNT(*)                        AS num_orders,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(price), 2)            AS avg_price
FROM retail_transactions
GROUP BY gender, category
ORDER BY gender, total_revenue DESC;

-- ════════════════════════════════════════════════════════════════
-- QUERY 7: City-Wise Revenue Ranking
-- ════════════════════════════════════════════════════════════════
SELECT '=== CITY-WISE REVENUE RANKING ===' AS header;

SELECT
    city,
    COUNT(DISTINCT user_id)         AS customers,
    COUNT(*)                        AS orders,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
FROM retail_transactions
GROUP BY city
ORDER BY revenue_rank;

-- ════════════════════════════════════════════════════════════════
-- QUERY 8: Price Tier Analysis
-- ════════════════════════════════════════════════════════════════
SELECT '=== PRICE TIER ANALYSIS ===' AS header;

SELECT
    price_tier,
    category,
    COUNT(*)                        AS num_orders,
    ROUND(SUM(total_revenue), 2)    AS revenue,
    ROUND(AVG(rating), 2)           AS avg_rating
FROM retail_transactions
GROUP BY price_tier, category
ORDER BY price_tier, revenue DESC;

-- ════════════════════════════════════════════════════════════════
-- QUERY 9: Yearly Comparison
-- ════════════════════════════════════════════════════════════════
SELECT '=== YEARLY COMPARISON ===' AS header;

SELECT
    year,
    COUNT(DISTINCT user_id)         AS unique_customers,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(total_revenue), 2)    AS annual_revenue,
    ROUND(AVG(total_revenue), 2)    AS avg_order_value
FROM retail_transactions
GROUP BY year
ORDER BY year;

-- ════════════════════════════════════════════════════════════════
-- QUERY 10: Top Category per City
-- ════════════════════════════════════════════════════════════════
SELECT '=== TOP CATEGORY PER CITY ===' AS header;

SELECT city, category, total_revenue
FROM (
    SELECT
        city,
        category,
        ROUND(SUM(total_revenue), 2)  AS total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY SUM(total_revenue) DESC
        ) AS rn
    FROM retail_transactions
    GROUP BY city, category
) ranked
WHERE rn = 1
ORDER BY total_revenue DESC;
