-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Pig ETL Script
-- File   : 03_revenue_by_category.pig
-- Purpose: Revenue aggregation by category + monthly trends
-- Run    : pig -f pig_scripts/03_revenue_by_category.pig
-- ═══════════════════════════════════════════════════════════════

cleaned = LOAD '/retail_platform/processed/cleaned_retail'
    USING PigStorage(',')
    AS (
        user_id:      chararray,
        product_id:   chararray,
        product_name: chararray,
        category:     chararray,
        price:        double,
        quantity:     int,
        total_revenue:double,
        timestamp:    chararray,
        year_month:   chararray,
        year:         chararray,
        city:         chararray,
        gender:       chararray,
        rating:       double,
        price_tier:   chararray
    );

-- ════════════════════════════════════════════════════════════════
-- PART A: Revenue by Category (overall)
-- ════════════════════════════════════════════════════════════════
by_category = GROUP cleaned BY category;

cat_revenue = FOREACH by_category GENERATE
    group                                   AS category,
    COUNT(cleaned)                          AS num_transactions: long,
    SUM(cleaned.quantity)                   AS total_qty: long,
    ROUND_TO(SUM(cleaned.total_revenue), 2) AS total_revenue: double,
    ROUND_TO(AVG(cleaned.price), 2)         AS avg_price: double,
    ROUND_TO(AVG(cleaned.rating), 2)        AS avg_rating: double;

cat_sorted = ORDER cat_revenue BY total_revenue DESC;

STORE cat_sorted INTO '/retail_platform/pig_output/revenue_by_category'
    USING PigStorage(',');

DUMP cat_sorted;

-- ════════════════════════════════════════════════════════════════
-- PART B: Monthly revenue trends
-- ════════════════════════════════════════════════════════════════
by_month = GROUP cleaned BY year_month;

monthly = FOREACH by_month GENERATE
    group                                   AS year_month,
    COUNT(cleaned)                          AS num_orders: long,
    SIZE(cleaned.user_id)                   AS unique_customers: long,
    ROUND_TO(SUM(cleaned.total_revenue), 2) AS monthly_revenue: double,
    ROUND_TO(AVG(cleaned.total_revenue), 2) AS avg_order_value: double;

monthly_sorted = ORDER monthly BY year_month ASC;

STORE monthly_sorted INTO '/retail_platform/pig_output/monthly_trends'
    USING PigStorage(',');

DUMP monthly_sorted;

-- ════════════════════════════════════════════════════════════════
-- PART C: Revenue by City
-- ════════════════════════════════════════════════════════════════
by_city = GROUP cleaned BY city;

city_revenue = FOREACH by_city GENERATE
    group                                   AS city,
    COUNT(cleaned)                          AS num_orders: long,
    ROUND_TO(SUM(cleaned.total_revenue), 2) AS total_revenue: double;

city_sorted = ORDER city_revenue BY total_revenue DESC;

STORE city_sorted INTO '/retail_platform/pig_output/revenue_by_city'
    USING PigStorage(',');
