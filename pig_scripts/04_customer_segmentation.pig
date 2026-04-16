-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Pig ETL Script
-- File   : 04_customer_segmentation.pig
-- Purpose: RFM-based customer segmentation
--          R = Recency (days since last purchase)
--          F = Frequency (number of orders)
--          M = Monetary (total spend)
-- Run    : pig -f pig_scripts/04_customer_segmentation.pig
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

-- ─── Group by customer ────────────────────────────────────────────
by_customer = GROUP cleaned BY user_id;

-- ─── Compute RFM metrics per customer ────────────────────────────
customer_rfm = FOREACH by_customer GENERATE
    group                                        AS user_id,
    COUNT(cleaned)                               AS frequency: long,       -- F
    ROUND_TO(SUM(cleaned.total_revenue), 2)      AS monetary: double,      -- M
    ROUND_TO(AVG(cleaned.total_revenue), 2)      AS avg_order_value: double,
    MAX(cleaned.timestamp)                       AS last_purchase: chararray,
    MIN(cleaned.timestamp)                       AS first_purchase: chararray,
    ROUND_TO(AVG(cleaned.rating), 2)             AS avg_rating: double,
    cleaned.city                                 AS cities;

-- ─── Classify customers by monetary value ────────────────────────
-- Segment:
--   VIP        = spend >= 5000
--   High Value = spend 1000-4999
--   Regular    = spend 200-999
--   Occasional = spend < 200
segmented = FOREACH customer_rfm GENERATE
    user_id,
    frequency,
    monetary,
    avg_order_value,
    last_purchase,
    first_purchase,
    avg_rating,
    (monetary >= 5000.0 ? 'VIP'        :
     monetary >= 1000.0 ? 'High_Value' :
     monetary >= 200.0  ? 'Regular'    : 'Occasional') AS segment: chararray;

-- ─── Sort by monetary descending ─────────────────────────────────
sorted_customers = ORDER segmented BY monetary DESC;

-- ─── Top 50 highest-value customers ──────────────────────────────
top_customers = LIMIT sorted_customers 50;

-- ─── Store ───────────────────────────────────────────────────────
STORE sorted_customers INTO '/retail_platform/pig_output/customer_segments'
    USING PigStorage(',');

-- ─── Print top customers ─────────────────────────────────────────
DUMP top_customers;

-- ─── Segment counts ──────────────────────────────────────────────
by_segment    = GROUP segmented BY segment;
segment_count = FOREACH by_segment GENERATE
    group         AS segment,
    COUNT(segmented) AS num_customers: long,
    ROUND_TO(SUM(segmented.monetary), 2)  AS total_revenue: double,
    ROUND_TO(AVG(segmented.monetary), 2)  AS avg_spend: double;

segment_sorted = ORDER segment_count BY total_revenue DESC;
DUMP segment_sorted;

STORE segment_sorted INTO '/retail_platform/pig_output/segment_summary'
    USING PigStorage(',');
