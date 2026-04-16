-- ═══════════════════════════════════════════════════════════════
-- Smart Retail Analytics Platform — Apache Pig ETL Script
-- File   : 01_clean_transform.pig
-- Purpose: Load raw retail data from HDFS, clean nulls,
--          filter bad records, and transform data types
-- Run    : pig -f pig_scripts/01_clean_transform.pig
-- ═══════════════════════════════════════════════════════════════

-- ─── STEP 1: Register any UDF jars (none needed for basic Pig) ───
-- (Uncomment if you add custom UDFs later)
-- REGISTER '/path/to/my-udf.jar';

-- ─── STEP 2: Load raw data from HDFS ─────────────────────────────
-- Schema: user_id, product_id, product_name, category,
--         price, quantity, timestamp, city, gender, rating
raw_data = LOAD '/retail_platform/raw_data/retail_data.csv'
    USING PigStorage(',')
    AS (
        user_id:     chararray,
        product_id:  chararray,
        product_name:chararray,
        category:    chararray,
        price:       double,
        quantity:    int,
        timestamp:   chararray,
        city:        chararray,
        gender:      chararray,
        rating:      double
    );

-- ─── STEP 3: Display first 5 records (for debugging) ─────────────
-- DUMP raw_data;   -- Uncomment to see data in console
-- DESCRIBE raw_data;  -- Show schema

-- ─── STEP 4: Remove records with NULL critical fields ─────────────
-- Critical fields: user_id, product_id, category, price, quantity, timestamp
no_nulls = FILTER raw_data BY
    user_id     IS NOT NULL AND user_id     != '' AND
    product_id  IS NOT NULL AND product_id  != '' AND
    category    IS NOT NULL AND category    != '' AND
    price       IS NOT NULL AND
    quantity    IS NOT NULL AND
    timestamp   IS NOT NULL AND timestamp   != '';

-- ─── STEP 5: Remove business-invalid records ──────────────────────
-- Price must be > 0, Quantity must be >= 1, Rating must be 1.0-5.0
valid_records = FILTER no_nulls BY
    price    > 0.0   AND
    quantity >= 1    AND
    rating   >= 1.0  AND
    rating   <= 5.0;

-- ─── STEP 6: Add computed columns ────────────────────────────────
-- total_revenue = price × quantity
-- price_tier    = derived category (Budget / Mid / Premium)
transformed = FOREACH valid_records GENERATE
    user_id,
    product_id,
    product_name,
    UPPER(category)                     AS category,
    price,
    quantity,
    (double)(price * quantity)          AS total_revenue: double,
    timestamp,
    SUBSTRING(timestamp, 0, 7)          AS year_month: chararray,  -- 'YYYY-MM'
    SUBSTRING(timestamp, 0, 4)          AS year: chararray,
    city,
    gender,
    rating,
    (price < 2.0  ? 'Budget'  :
     price < 10.0 ? 'Mid'     : 'Premium') AS price_tier: chararray;

-- ─── STEP 7: Remove duplicates on (user_id, product_id, timestamp) 
distinct_data = DISTINCT transformed;

-- ─── STEP 8: Order by timestamp ───────────────────────────────────
ordered = ORDER distinct_data BY timestamp ASC;

-- ─── STEP 9: Store cleaned data back to HDFS ──────────────────────
STORE ordered INTO '/retail_platform/processed/cleaned_retail'
    USING PigStorage(',');

-- ─── STEP 10: Quick stats using ILLUSTRATE ────────────────────────
-- Count total clean records
grouped_all = GROUP ordered ALL;
total_count = FOREACH grouped_all GENERATE COUNT(ordered) AS total_rows;
DUMP total_count;
