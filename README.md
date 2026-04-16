# Smart Retail Analytics Platform
## End-to-End Big Data Pipeline — UCI Online Retail II

---

## Software Status (Audit: 2026-04-08)

| Component       | Status       | Version / Location                   |
|----------------|:------------:|--------------------------------------|
| Java (JDK)      | ✅ Installed | OpenJDK 21.0.10                      |
| Hadoop (HDFS, YARN, MapReduce) | ✅ Installed | 3.3.6 @ `~/hadoop-3.3.6` |
| Apache Pig      | ❌ Missing   | Needs download → `setup/install_missing.sh` |
| Apache Hive     | ❌ Missing   | Needs download → `setup/install_missing.sh` |
| HBase           | ❌ Missing   | Needs download → `setup/install_missing.sh` |
| ZooKeeper       | ❌ Missing   | Bundled inside HBase (standalone mode) |
| Python 3        | ✅ Installed | 3.12.3                               |
| pip3            | ✅ Installed | 24.0                                 |
| SSH             | ✅ Installed | OpenSSH 9.6p1                        |
| pandas          | ✅ Installed | (just installed)                     |
| matplotlib      | ✅ Installed | (just installed)                     |
| seaborn         | ✅ Installed | (just installed)                     |
| wget / curl     | ✅ Installed | Available                            |

---

## Dataset

| File | Rows | Size | Description |
|------|-----:|-----:|-------------|
| `dataset/retail_raw.csv` | 1,067,371 | ~150 MB | Original UCI download |
| `dataset/retail_cleaned.csv` | **805,549** | ~110 MB | After cleaning (removed returns/nulls) |
| `dataset/retail_scaled.csv` | **8,055,490** | **780 MB** | 10× scaled for Big Data simulation |
| `dataset/retail_hdfs_ready.csv` | 8,055,490 | 780 MB | No header — ready for HDFS upload |

**Source**: [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) — free, public domain

---

## Project Folder Structure

```
SMART RETAIL ANALYTICS PLATFORM/
├── dataset/
│   ├── generate_dataset.py       # (old synthetic generator — replaced)
│   ├── preprocess_real_data.py   # ← Real UCI data preprocessor
│   ├── retail_cleaned.csv        # ← Clean data (805K rows)
│   └── retail_hdfs_ready.csv     # ← HDFS upload file (8M rows)
│
├── setup/
│   ├── install_missing.sh        # ← Install Pig/Hive/HBase (smart, skips existing)
│   └── start_all_services.sh     # ← Start/stop all services
│
├── hdfs_ops/
│   └── hdfs_upload.sh            # ← Upload dataset to HDFS
│
├── pig_scripts/
│   ├── 01_clean_transform.pig    # ETL: clean, transform, type-cast
│   ├── 02_top_products.pig       # Aggregation: top 20 products by revenue
│   ├── 03_revenue_by_category.pig # Revenue by category, month, city
│   ├── 04_customer_segmentation.pig # RFM customer segments
│   └── run_pig_etl.sh            # ← Run all Pig scripts in order
│
├── hive_scripts/
│   ├── 01_create_tables.hql      # Database + 4 Hive tables (ORC, partitioned)
│   ├── 02_analytics_queries.hql  # 10 HiveQL analytics queries
│   └── run_hive.sh               # ← Execute all Hive scripts
│
├── hbase_scripts/
│   └── hbase_operations.py       # Create tables, bulk load, real-time demos
│
├── python_viz/
│   ├── generate_charts.py        # ← Generates 5 dark charts + data.js
│   ├── pig_results/              # Pig output (CSV) collected here
│   └── charts/                   # Generated PNG charts
│       ├── 01_top_products.png
│       ├── 02_revenue_by_category.png
│       ├── 03_monthly_trends.png
│       ├── 04_customer_segments.png
│       └── 05_city_category_heatmap.png
│
├── dashboard/
│   ├── index.html                # ← Interactive web dashboard
│   └── data.js                   # Auto-generated chart data
│
└── docs/
    ├── README.md                 # This file
    └── viva_questions.py         # 11 Q&A + 2-min presentation script
```

---

## Step-by-Step Execution Guide

### Phase 1: Install Missing Components

```bash
# Install Pig, Hive, HBase (skips already-installed Hadoop)
bash "setup/install_missing.sh"

# Reload environment variables
source ~/.bashrc
```

### Phase 2: Start Services

```bash
# Start HDFS + YARN
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Start HBase (includes embedded ZooKeeper)
$HBASE_HOME/bin/start-hbase.sh

# Verify — should show: NameNode, DataNode, HMaster
jps
```

### Phase 3: HDFS Operations

```bash
# Create HDFS directory tree + upload 8M row dataset
bash hdfs_ops/hdfs_upload.sh

# Verify
hdfs dfs -ls /retail_platform/
hdfs dfs -du -h /retail_platform/raw_data/
```

### Phase 4: Apache Pig ETL

```bash
# Run all 4 Pig scripts via MapReduce
bash pig_scripts/run_pig_etl.sh

# Or run individually:
pig -x mapreduce -f pig_scripts/01_clean_transform.pig
pig -x mapreduce -f pig_scripts/02_top_products.pig
pig -x mapreduce -f pig_scripts/03_revenue_by_category.pig
pig -x mapreduce -f pig_scripts/04_customer_segmentation.pig
```

### Phase 5: Apache Hive Analytics

```bash
# Initialize metastore (first time only)
$HIVE_HOME/bin/schematool -initSchema -dbType derby

# Create tables and run all queries
hive -f hive_scripts/01_create_tables.hql
hive -f hive_scripts/02_analytics_queries.hql

# Or run a single query interactively:
hive
> USE retail_analytics;
> SELECT category, ROUND(SUM(price*quantity),2) as revenue
>   FROM retail_transactions
>   GROUP BY category ORDER BY revenue DESC;
```

### Phase 6: HBase Real-Time Layer

```bash
# Start HBase Thrift server (for Python client)
$HBASE_HOME/bin/hbase thrift start &
sleep 10

# Create tables, load data, run lookup demos
python3 hbase_scripts/hbase_operations.py

# Interactive HBase shell:
$HBASE_HOME/bin/hbase shell
> list
> scan 'retail_products', {LIMIT => 5}
> get 'retail_customers', 'U13085'
```

### Phase 7: Visualization

```bash
# Already done! Charts are in python_viz/charts/
# To regenerate:
python3 python_viz/generate_charts.py

# Open dashboard:
xdg-open dashboard/index.html
# Or: firefox dashboard/index.html
```

---

## Key Analytical Insights

| Insight | Value |
|---------|-------|
| Total Revenue | £17.7M (Dec 2009 – Dec 2011) |
| Best-Selling Product | Regency Cakestand 3 Tier |
| Top Category | General Merchandise (~39%) |
| Peak Month | November 2011 (holiday seasonality) |
| Avg Order Value | £22.03 |
| Unique Customers | 5,877 |
| VIP Customers | ~3% generate >20% of revenue |

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `JAVA_HOME not set` | Env var missing | `source ~/.bashrc` |
| `Connection refused 9000` | HDFS not running | `$HADOOP_HOME/sbin/start-dfs.sh` |
| `Safe mode is ON` | NameNode in safe mode | `hdfs dfsadmin -safemode leave` |
| `pig: command not found` | Pig not in PATH | `export PATH=$PATH:$PIG_HOME/bin` |
| `hive: command not found` | Hive not in PATH | `export PATH=$PATH:$HIVE_HOME/bin` |
| `HBase Thrift refused` | Thrift not started | `hbase thrift start &` |
| `ModuleNotFoundError: happybase` | Python lib missing | `pip3 install happybase --break-system-packages` |
| `NameNode not formatted` | First-time setup | `hdfs namenode -format` |
| `Pig compilation error` | Wrong HDFS path | Verify with `hdfs dfs -ls /retail_platform/` |

---

## Viva Preparation

```bash
python3 docs/viva_questions.py
```

Topics covered:
1. Dataset selection rationale
2. HDFS internals (blocks, replication, NameNode vs DataNode)
3. Pig Latin ETL walkthrough
4. Pig vs Hive — when to use which
5. Hive partitioning & bucketing design
6. HBase vs RDBMS comparison
7. HBase row-key strategy
8. ZooKeeper's role in the cluster
9. Big Data scaling methodology
10. Top insights from analysis
11. Production-readiness improvements

---

*Smart Retail Analytics Platform — Built with Hadoop 3.3.6 ecosystem*
