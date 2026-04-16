#!/usr/bin/env python3
"""
Smart Retail Analytics Platform — HBase Schema & Data Loader
Uses: happybase (Python HBase client via Thrift)

HBase Schema Design:
  Table: retail_products
    Row Key: product_id (e.g. "PF1CD4")
    Column Families:
      info:     → product_name, category, price_tier
      stats:    → total_orders, total_qty, total_revenue, avg_rating
      meta:     → last_updated, source

  Table: retail_customers
    Row Key: user_id (e.g. "U13085")
    Column Families:
      profile:  → city, gender, segment
      activity: → total_orders, total_spend, last_purchase, avg_order_value
      prefs:    → favorite_category, avg_rating_given

  Table: retail_lookup
    Row Key: category#product_id (composite key for range scans)
    Column Families:
      info:     → product_name, price, quantity
"""

import happybase
import csv
import os
import time

# ──────────────── Config ──────────────────────────────────────────
HBASE_HOST  = 'localhost'
HBASE_PORT  = 9090          # HBase Thrift server port
BATCH_SIZE  = 100           # rows per batch mutation

DATA_DIR    = os.path.join(os.path.dirname(__file__), '..', 'python_viz', 'pig_results')

# ──────────────── Connect ─────────────────────────────────────────
def connect():
    print(f"[INFO] Connecting to HBase at {HBASE_HOST}:{HBASE_PORT}…")
    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=30000)
    conn.open()
    print("[INFO] Connected ✅")
    return conn


# ──────────────── Create Tables ───────────────────────────────────
def create_tables(conn):
    """Create HBase tables with column families."""
    existing = [t.decode() for t in conn.tables()]

    tables_config = {
        'retail_products': {
            'info':  {'max_versions': 1, 'compression': 'GZ'},
            'stats': {'max_versions': 3, 'compression': 'GZ'},
            'meta':  {'max_versions': 1},
        },
        'retail_customers': {
            'profile':  {'max_versions': 1, 'compression': 'GZ'},
            'activity': {'max_versions': 5, 'compression': 'GZ'},
            'prefs':    {'max_versions': 1},
        },
        'retail_lookup': {
            'info': {'max_versions': 1, 'compression': 'GZ'},
        },
    }

    for table_name, families in tables_config.items():
        if table_name in existing:
            print(f"[SKIP] Table '{table_name}' already exists")
        else:
            conn.create_table(table_name, families)
            print(f"[✅]   Created table: {table_name}")
            print(f"       Column families: {list(families.keys())}")


# ──────────────── Load Products ───────────────────────────────────
def load_products(conn):
    """
    Load product data into retail_products table.
    Source: pig_results/top_products.csv
    Schema: product_id, product_name, category, num_orders,
            total_qty, total_revenue, avg_price, avg_rating
    """
    csv_file = os.path.join(DATA_DIR, 'top_products.csv')
    if not os.path.exists(csv_file):
        print(f"[WARN] {csv_file} not found. Run Pig scripts first.")
        return 0

    table  = conn.table('retail_products')
    batch  = table.batch(batch_size=BATCH_SIZE)
    count  = 0

    print("[INFO] Loading products into HBase…")
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 8:
                continue
            product_id, product_name, category, num_orders, \
            total_qty, total_revenue, avg_price, avg_rating = row[:8]

            row_key = product_id.encode()

            batch.put(row_key, {
                b'info:product_name': product_name.encode(),
                b'info:category':     category.encode(),
                b'info:price_tier':   (
                    b'Premium' if float(avg_price or 0) >= 10
                    else b'Mid' if float(avg_price or 0) >= 2
                    else b'Budget'
                ),
                b'stats:num_orders':    str(num_orders).encode(),
                b'stats:total_qty':     str(total_qty).encode(),
                b'stats:total_revenue': str(total_revenue).encode(),
                b'stats:avg_price':     str(avg_price).encode(),
                b'stats:avg_rating':    str(avg_rating).encode(),
                b'meta:last_updated':   str(int(time.time())).encode(),
                b'meta:source':         b'UCI_Online_Retail_II',
            })
            count += 1

    batch.send()
    print(f"[✅]   Loaded {count} products into retail_products")
    return count


# ──────────────── Load Customers ──────────────────────────────────
def load_customers(conn):
    """
    Load customer segments into retail_customers table.
    Source: pig_results/customer_segments.csv
    """
    csv_file = os.path.join(DATA_DIR, 'customer_segments.csv')
    if not os.path.exists(csv_file):
        print(f"[WARN] {csv_file} not found.")
        return 0

    table = conn.table('retail_customers')
    batch = table.batch(batch_size=BATCH_SIZE)
    count = 0

    print("[INFO] Loading customers into HBase…")
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 8:
                continue
            user_id, frequency, monetary, avg_order, \
            last_purchase, first_purchase, avg_rating, segment = row[:8]

            row_key = user_id.encode()

            batch.put(row_key, {
                b'profile:segment':         segment.encode(),
                b'activity:total_orders':   str(frequency).encode(),
                b'activity:total_spend':    str(monetary).encode(),
                b'activity:avg_order_val':  str(avg_order).encode(),
                b'activity:last_purchase':  last_purchase.encode(),
                b'activity:first_purchase': first_purchase.encode(),
                b'prefs:avg_rating_given':  str(avg_rating).encode(),
                b'meta:last_updated':       str(int(time.time())).encode(),
            })
            count += 1

    batch.send()
    print(f"[✅]   Loaded {count} customers into retail_customers")
    return count


# ──────────────── Real-Time Lookup Demos ──────────────────────────
def demo_lookups(conn):
    """Demonstrate HBase real-time single-row lookups."""
    print("\n" + "═"*55)
    print("  HBase Real-Time Lookup Demos")
    print("═"*55)

    # 1. Product lookup by product_id
    prod_table = conn.table('retail_products')
    print("\n[DEMO 1] Single product lookup:")
    for key, data in prod_table.scan(limit=1):
        print(f"  Row Key : {key.decode()}")
        for col, val in data.items():
            print(f"  {col.decode():35s} → {val.decode()}")

    # 2. Customer lookup
    cust_table = conn.table('retail_customers')
    print("\n[DEMO 2] Single customer lookup:")
    for key, data in cust_table.scan(limit=1):
        print(f"  Row Key : {key.decode()}")
        for col, val in data.items():
            print(f"  {col.decode():35s} → {val.decode()}")

    # 3. Scan VIP customers only (column filter)
    print("\n[DEMO 3] All VIP customers:")
    vip_count = 0
    for key, data in cust_table.scan(
        filter=b"SingleColumnValueFilter('profile','segment',=,'binary:VIP')"
    ):
        spend = data.get(b'activity:total_spend', b'N/A').decode()
        print(f"  {key.decode():12s} → spend: £{spend}")
        vip_count += 1
        if vip_count >= 10:
            break

    # 4. Range scan (row keys starting with "U1")
    print("\n[DEMO 4] Range scan (user IDs U1xxx):")
    scan_count = 0
    for key, data in cust_table.scan(row_prefix=b'U1', limit=5):
        print(f"  {key.decode()} | orders: {data.get(b'activity:total_orders', b'?').decode()}")
        scan_count += 1

    print(f"\n[✅]   All lookup demos complete")


# ──────────────── Main ────────────────────────────────────────────
if __name__ == '__main__':
    try:
        conn = connect()

        print("\n─── Step 1: Creating Tables ───")
        create_tables(conn)

        print("\n─── Step 2: Loading Product Data ───")
        p = load_products(conn)

        print("\n─── Step 3: Loading Customer Data ───")
        c = load_customers(conn)

        print("\n─── Step 4: Real-Time Lookups ───")
        if p > 0 or c > 0:
            demo_lookups(conn)

        conn.close()
        print("\n✅  HBase operations complete!")
        print("    Products loaded    :", p)
        print("    Customers loaded   :", c)
        print("\n💡 Why HBase over SQL here?")
        print("   • Single-row lookup in O(1) via row-key scan")
        print("   • Scales to billions of rows horizontally")
        print("   • Flexible schema — add columns without ALTER TABLE")
        print("   • ZooKeeper ensures consistent master election")

    except Exception as e:
        print(f"\n[ERROR] HBase connection failed: {e}")
        print("\n💡 Make sure HBase Thrift server is running:")
        print("   $HBASE_HOME/bin/hbase thrift start &")
        print("   (wait 10 seconds, then retry)")
