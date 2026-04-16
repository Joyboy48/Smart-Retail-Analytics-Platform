# 🛒 Smart Retail Analytics Platform

[![Hadoop Ecosystem](https://img.shields.io/badge/Hadoop-3.3.6-blue.svg)](https://hadoop.apache.org/)
[![Apache Pig](https://img.shields.io/badge/Apache_Pig-0.17.0-orange.svg)](https://pig.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache_Hive-3.1.3-yellow.svg)](https://hive.apache.org/)
[![Apache HBase](https://img.shields.io/badge/Apache_HBase-2.5.8-green.svg)](https://hbase.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-blueviolet.svg)](https://www.python.org/)

An end-to-end scalable Big Data pipeline capable of ingesting, cleaning, processing, and serving real-world retail transactions. The platform extracts critical business insights (such as total revenue, top products, customer segments, and monthly trends) using the power of distributed computing.

---

## 🚀 1. Project Overview & Objectives

**Domain:** Big Data / Retail Analytics  
**Why this project?** Traditional RDBMS (like MySQL) struggle when data scales to gigabytes or terabytes. This project demonstrates how the **Hadoop Ecosystem** handles massive datasets natively using distributed storage (HDFS) and parallel processing (MapReduce/Pig/Hive/HBase). 

The platform also includes a seamless **Single Page Application (SPA)** front-end to provide an interactive administrative dashboard.

---

## 📊 2. Dataset Details

*   **Source:** [UCI Machine Learning Repository (Online Retail II Dataset)](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
*   **Original Size:** 1,067,371 rows across 2 Excel sheets.
*   **Big Data Simulation:** The original dataset was cleaned and scaled up (duplicated and randomized with ±2% price variation) to **~800+ MB / 8 Million rows** to mimic real Big Data volume. This triggers HDFS to split data into multiple 128MB blocks.

| File | Rows | Size | Description |
|------|-----:|-----:|-------------|
| `dataset/retail_raw.csv` | 1,067,371 | ~150 MB | Original UCI download |
| `dataset/retail_cleaned.csv` | **805,549** | ~110 MB | After cleaning (removed returns/nulls) |
| `dataset/retail_hdfs_ready.csv` | **8,055,490** | **780 MB** | 10× scaled for Big Data simulation (No header) |

---

## 🏗️ 3. Architecture & Working Principle

Here is exactly how data flows from raw origin to the final dashboard:

1. **Python + Pandas (Data Preprocessing):** Reads the raw dataset, handles missing values, and scales the data by 10x to simulate Big Data (`retail_hdfs_ready.csv`).
2. **HDFS (The Storage Layer):** Stores the large CSV across multiple DataNodes in 128MB chunks/blocks.
3. **Apache Pig (MapReduce ETL Layer):** Acts as an abstraction over Java MapReduce. It runs filtering, aggregations, and RFM segmentation directly on HDFS in parallel.
4. **Apache Hive (Data Warehousing Layer):** Provides a SQL-like interface using HiveQL. Features ORC Format & Snappy Compression, along with Partitioning (by `year_month`), to scan data effectively and run comprehensive business intelligence queries.
5. **Apache HBase + ZooKeeper (Real-time Layer):** The NoSQL column-family database running on HDFS, featuring O(1) lookups for Real-time metrics based on row keys like `product_id`. ZooKeeper coordinates the HBase cluster.
6. **Frontend SPA Dashboard:** A Single Page App (SPA) displaying live MapReduce simulations, admin analytical filters, and dataset previews using HTML/CSS/JS and Chart.js.

---

## 📂 4. Project Folder Structure

```
SMART RETAIL ANALYTICS PLATFORM/
├── dataset/
│   ├── preprocess_real_data.py   # Python script to clean & scale UCI data
│   └── retail_hdfs_ready.csv     # 8M rows HDFS-ready
├── setup/
│   ├── install_missing.sh        # Smart script to install Pig/Hive/HBase 
│   └── start_all_services.sh     # Automates service bootup
├── hdfs_ops/
│   └── hdfs_upload.sh            # Upload dataset to HDFS 
├── pig_scripts/
│   ├── 01_clean_transform.pig    # Pipeline step: Clean, transform, type-cast
│   ├── 02_top_products.pig       # Pipeline step: Top 20 products
│   ├── 03_revenue_by_category.pig # Pipeline step: Revenue grouping
│   ├── 04_customer_segmentation.pig # Pipeline step: RFM segmentation
│   └── run_pig_etl.sh            # Execute all Pig scripts globally
├── hive_scripts/
│   ├── 01_create_tables.hql      # DB + Hive table setup (ORC, Partitioned)
│   ├── 02_analytics_queries.hql  # 10 HiveQL advanced business queries
│   └── run_hive.sh               # Execute Hive scripts
├── hbase_scripts/
│   └── hbase_operations.py       # Create tables, load data, real-time fetching
├── python_viz/
│   ├── generate_charts.py        # Chart data generator -> Python to JS
│   └── charts/                   # Resulting analytical PNG outputs
├── dashboard/
│   ├── index.html                # Interactive UI Single-Page Application (SPA)
│   └── data.js                   # Analytical data bound to the UI 
└── docs/
    ├── README.md                 # You are here!
    └── viva_questions.py         # Q&A Viva presentation prep tools
```

---

## ⚙️ 5. Step-by-Step Execution Guide

### Phase 1: Setup & Starting Services
```bash
# Install missing components
bash "setup/install_missing.sh" && source ~/.bashrc

# Start HDFS & YARN
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Start HBase
$HBASE_HOME/bin/start-hbase.sh
```

### Phase 2: HDFS Operations
```bash
# Upload scaled dataset to HDFS
bash hdfs_ops/hdfs_upload.sh
```

### Phase 3: Apache Pig & MapReduce ETL
```bash
# Run all Pig ETL MapReduce Tasks
bash pig_scripts/run_pig_etl.sh
```

### Phase 4: Apache Hive Analytics
```bash
# Format schema (first time only)
$HIVE_HOME/bin/schematool -initSchema -dbType derby

# Execute structured queries
hive -f hive_scripts/01_create_tables.hql
hive -f hive_scripts/02_analytics_queries.hql
```

### Phase 5: HBase Real-Time Interactions
```bash
$HBASE_HOME/bin/hbase thrift start &
python3 hbase_scripts/hbase_operations.py
```

### Phase 6: Visualization and SPA Dashboard
```bash
# Run charts generator
python3 python_viz/generate_charts.py

# Launch Front-End Application
xdg-open dashboard/index.html
```

---

## 📈 6. Key Analytical Insights

| Insight | Value |
|---------|-------|
| **Total Revenue** | £17.7M (Dec 2009 – Dec 2011) |
| **Best-Selling Product** | Regency Cakestand 3 Tier |
| **Top Category** | General Merchandise (~39%) |
| **Peak Season** | November 2011 (holiday seasonality) |
| **Avg Order Value** | £22.03 |
| **Unique Customers** | 5,877 |
| **VIP Base** | ~3% of VIPs generate >20% of total revenue |

---

## 🛠️ 7. Software Status & Troubleshooting

| Component | Status | Location |
|-----------|:------:|----------|
| Java (JDK) | OpenJDK 21.0.10 | Installed |
| Hadoop | 3.3.6 | `~/hadoop-3.3.6` |
| Python 3 | 3.12.3 | Installed (Pandas, Matplotlib, Seaborn) |

***Common fix if tools miss PATH:***
Ensure `.bashrc` represents the loaded states via `source ~/.bashrc`. For Hadoop issues, run `jps` to verify NameNode and DataNode are active.
