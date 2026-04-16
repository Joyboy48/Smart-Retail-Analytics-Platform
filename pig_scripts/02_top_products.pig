-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Pig ETL Script
-- File   : 02_top_products.pig
-- Purpose: Find Top 20 best-selling products by total revenue
--          and quantity sold
-- Run    : pig -f pig_scripts/02_top_products.pig
-- ═══════════════════════════════════════════════════════════════

-- ─── Load Cleaned Data ────────────────────────────────────────────
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
        rating:        double,
        price_tier:   chararray
    );

-- ─── Group by product ────────────────────────────────────────────
by_product = GROUP cleaned BY (product_id, product_name, category);

-- ─── Aggregate per product ───────────────────────────────────────
product_stats = FOREACH by_product GENERATE
    FLATTEN(group)                          AS (product_id, product_name, category),
    COUNT(cleaned)                          AS num_orders: long,
    SUM(cleaned.quantity)                   AS total_qty: long,
    SUM(cleaned.total_revenue)              AS total_revenue: double,
    AVG(cleaned.price)                      AS avg_price: double,
    AVG(cleaned.rating)                     AS avg_rating: double;

-- ─── Round revenue to 2 decimal places ───────────────────────────
product_stats_rounded = FOREACH product_stats GENERATE
    product_id,
    product_name,
    category,
    num_orders,
    total_qty,
    ROUND_TO(total_revenue, 2)  AS total_revenue: double,
    ROUND_TO(avg_price, 2)      AS avg_price: double,
    ROUND_TO(avg_rating, 2)     AS avg_rating: double;

-- ─── Sort descending by total revenue ────────────────────────────
top_by_revenue = ORDER product_stats_rounded BY total_revenue DESC;

-- ─── Take Top 20 ─────────────────────────────────────────────────
top_20 = LIMIT top_by_revenue 20;

-- ─── Store results ───────────────────────────────────────────────
STORE top_20 INTO '/retail_platform/pig_output/top_products'
    USING PigStorage(',');

-- ─── Print to console ────────────────────────────────────────────
DUMP top_20;
