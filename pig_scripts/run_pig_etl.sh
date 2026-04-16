#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  Smart Retail Analytics Platform — Pig ETL Runner
#  Runs all Pig scripts in order
# ═══════════════════════════════════════════════════════════════

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()      { echo -e "${GREEN}[✅ DONE]${NC}  $*"; }
info()    { echo -e "${GREEN}[INFO]${NC}    $*"; }
err()     { echo -e "${RED}[ERROR]${NC}   $*"; }
section() { echo -e "\n${GREEN}══════════════════════════════════════${NC}"; \
            echo -e "${GREEN}  $*${NC}"; \
            echo -e "${GREEN}══════════════════════════════════════${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/../logs"
mkdir -p "$LOG_DIR"

run_pig() {
    local script="$1"
    local label="$2"
    local logfile="$LOG_DIR/$(basename $script .pig).log"

    section "$label"
    info "Running: pig -x mapreduce -f $script"
    info "Log: $logfile"

    if pig -x mapreduce -f "$script" > "$logfile" 2>&1; then
        ok "$label completed"
        tail -10 "$logfile"
    else
        err "$label FAILED! Check log: $logfile"
        tail -20 "$logfile"
        exit 1
    fi
}

echo "╔══════════════════════════════════════════════════════════╗"
echo "║     Smart Retail Analytics — Pig ETL Pipeline            ║"
echo "╚══════════════════════════════════════════════════════════╝"

# Check HDFS data exists
if ! hdfs dfs -test -f /retail_platform/raw_data/retail_data.csv 2>/dev/null; then
    err "Dataset not found in HDFS! Run: bash hdfs_ops/hdfs_upload.sh"
    exit 1
fi

# Remove old Pig output (so scripts can write fresh)
info "Clearing old Pig output directories…"
hdfs dfs -rm -r -f /retail_platform/processed/cleaned_retail   2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/top_products    2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/revenue_by_category 2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/monthly_trends  2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/revenue_by_city 2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/customer_segments 2>/dev/null
hdfs dfs -rm -r -f /retail_platform/pig_output/segment_summary 2>/dev/null

# Run scripts in order
run_pig "$SCRIPT_DIR/01_clean_transform.pig"    "Script 1: Data Cleaning & Transformation"
run_pig "$SCRIPT_DIR/02_top_products.pig"        "Script 2: Top Products Analysis"
run_pig "$SCRIPT_DIR/03_revenue_by_category.pig" "Script 3: Revenue by Category & Monthly Trends"
run_pig "$SCRIPT_DIR/04_customer_segmentation.pig" "Script 4: Customer Segmentation (RFM)"

# Collect output to local for Python visualization
section "Collecting Pig Output to Local"
LOCAL_RESULTS="$SCRIPT_DIR/../python_viz/pig_results"
mkdir -p "$LOCAL_RESULTS"

collect() {
    local hdfs_path="$1"
    local local_file="$2"
    hdfs dfs -getmerge "$hdfs_path" "$LOCAL_RESULTS/$local_file" 2>/dev/null \
        && ok "Collected: $local_file" \
        || echo -e "${YELLOW}[SKIP]${NC}  Could not collect $hdfs_path"
}

collect "/retail_platform/pig_output/top_products"       "top_products.csv"
collect "/retail_platform/pig_output/revenue_by_category" "revenue_by_category.csv"
collect "/retail_platform/pig_output/monthly_trends"     "monthly_trends.csv"
collect "/retail_platform/pig_output/revenue_by_city"    "revenue_by_city.csv"
collect "/retail_platform/pig_output/customer_segments"  "customer_segments.csv"
collect "/retail_platform/pig_output/segment_summary"    "segment_summary.csv"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         All Pig ETL Scripts Completed ✅                 ║"
echo "║  Next: bash hive_scripts/run_hive.sh                     ║"
echo "╚══════════════════════════════════════════════════════════╝"
