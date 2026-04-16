#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
#  Smart Retail Analytics Platform — HDFS Operations
#  Creates project directory structure in HDFS and uploads dataset
# ═══════════════════════════════════════════════════════════════════

set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()      { echo -e "${GREEN}[✅ OK]${NC}    $*"; }
info()    { echo -e "${GREEN}[INFO]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}   $*"; }
section() { echo -e "\n${GREEN}─── $* ───${NC}"; }

# ─── Paths ────────────────────────────────────────────────────────
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATASET_DIR="$PROJECT_DIR/dataset"
HDFS_CSV="$DATASET_DIR/retail_hdfs_ready.csv"

# HDFS directory tree
HDFS_ROOT="/retail_platform"
HDFS_RAW="$HDFS_ROOT/raw_data"
HDFS_PROCESSED="$HDFS_ROOT/processed"
HDFS_PIG_OUT="$HDFS_ROOT/pig_output"
HDFS_HIVE_WAREHOUSE="$HDFS_ROOT/hive_data"
HDFS_LOGS="$HDFS_ROOT/logs"

# ─── Step 1: Verify HDFS is Running ───────────────────────────────
section "Step 1: HDFS Health Check"
if ! hdfs dfsadmin -report &>/dev/null; then
    echo -e "${RED}[ERROR] HDFS is not running!${NC}"
    echo "  Start it with: \$HADOOP_HOME/sbin/start-dfs.sh"
    exit 1
fi
ok "HDFS is running"
hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Configured Capacity|DFS Used"

# ─── Step 2: Create HDFS Directory Structure ──────────────────────
section "Step 2: Creating HDFS Directory Structure"

DIRS=(
    "$HDFS_ROOT"
    "$HDFS_RAW"
    "$HDFS_PROCESSED"
    "$HDFS_PIG_OUT"
    "$HDFS_PIG_OUT/top_products"
    "$HDFS_PIG_OUT/revenue_by_category"
    "$HDFS_PIG_OUT/monthly_trends"
    "$HDFS_PIG_OUT/customer_segments"
    "$HDFS_HIVE_WAREHOUSE"
    "$HDFS_LOGS"
)

for dir in "${DIRS[@]}"; do
    if hdfs dfs -test -d "$dir" 2>/dev/null; then
        warn "$dir already exists — skipping"
    else
        hdfs dfs -mkdir -p "$dir"
        ok "Created: $dir"
    fi
done

# ─── Step 3: Upload Dataset ───────────────────────────────────────
section "Step 3: Uploading Dataset to HDFS"

if [ ! -f "$HDFS_CSV" ]; then
    echo -e "${RED}[ERROR] Dataset not found: $HDFS_CSV${NC}"
    echo "  Run first: python3 dataset/preprocess_real_data.py"
    exit 1
fi

FILE_SIZE=$(du -sh "$HDFS_CSV" | cut -f1)
info "Local file: $HDFS_CSV  ($FILE_SIZE)"

HDFS_DEST="$HDFS_RAW/retail_data.csv"
if hdfs dfs -test -f "$HDFS_DEST" 2>/dev/null; then
    warn "File already in HDFS: $HDFS_DEST"
    echo "  Use -put -f to force overwrite."
else
    info "Uploading to HDFS (this may take a few minutes for large files)…"
    hdfs dfs -put "$HDFS_CSV" "$HDFS_DEST"
    ok "Uploaded: $HDFS_DEST"
fi

# ─── Step 4: Verify Upload ────────────────────────────────────────
section "Step 4: Verifying HDFS Storage"

echo ""
echo "📂 HDFS Directory Tree:"
hdfs dfs -ls -R "$HDFS_ROOT" 2>/dev/null

echo ""
echo "📊 File sizes in HDFS:"
hdfs dfs -du -h "$HDFS_ROOT" 2>/dev/null

echo ""
echo "📋 Row count verification (first 5 lines):"
hdfs dfs -cat "$HDFS_DEST" 2>/dev/null | head -5

echo ""
echo "📋 Total lines in HDFS file:"
hdfs dfs -cat "$HDFS_DEST" 2>/dev/null | wc -l

# ─── Step 5: Set Permissions ──────────────────────────────────────
section "Step 5: Setting HDFS Permissions"
hdfs dfs -chmod -R 777 "$HDFS_ROOT" 2>/dev/null
ok "Permissions set to 777 on $HDFS_ROOT"

# ─── Summary ──────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              HDFS Upload Complete ✅                         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Dataset:  $HDFS_DEST"
echo "║  Run Pig:  bash pig_scripts/run_pig_etl.sh                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
