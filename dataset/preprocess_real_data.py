#!/usr/bin/env python3
"""
Smart Retail Analytics Platform
Real Dataset Preprocessor — UCI Online Retail II
Source : https://archive.ics.uci.edu/dataset/502/online+retail+ii
Fields  : Invoice, StockCode, Description, Quantity, InvoiceDate,
          Price, Customer ID, Country

Maps to our schema:
  user_id, product_id, product_name, category, price,
  quantity, timestamp, city, gender, rating
"""

import zipfile, os, random, hashlib
import pandas as pd

# ──────────────── Paths ────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
ZIP_FILE   = os.path.join(BASE_DIR, "online_retail_ii.zip")
RAW_CSV    = os.path.join(BASE_DIR, "retail_raw.csv")
CLEAN_CSV  = os.path.join(BASE_DIR, "retail_cleaned.csv")
SCALED_CSV = os.path.join(BASE_DIR, "retail_scaled.csv")     # 10x
HDFS_CSV   = os.path.join(BASE_DIR, "retail_hdfs_ready.csv") # final for HDFS

SCALE_FACTOR = 10   # change to 50 / 100 for larger simulation

# ──────────────── Category lookup (StockCode prefix → category) ────────────
def guess_category(desc):
    """Heuristic category assignment based on product description keywords."""
    desc = str(desc).upper()
    if any(k in desc for k in ["LIGHT","LAMP","BULB","LANTERN","CANDLE","HOLDER"]):
        return "Home & Lighting"
    if any(k in desc for k in ["BAG","PURSE","TOTE","SATCHEL","BACKPACK"]):
        return "Bags & Luggage"
    if any(k in desc for k in ["CARD","WRAP","GIFT","RIBBON","BOW","TAG","PAPER"]):
        return "Gift & Stationery"
    if any(k in desc for k in ["FRAME","CLOCK","MIRROR","WALL","SIGN","PRINT"]):
        return "Home Decor"
    if any(k in desc for k in ["TOY","GAME","DOLL","PUZZLE","PLAY","BALL"]):
        return "Toys & Games"
    if any(k in desc for k in ["MUG","CUP","BOWL","PLATE","LUNCH","BOTTLE","JAR","TIN"]):
        return "Kitchen & Dining"
    if any(k in desc for k in ["CUSHION","THROW","BLANKET","BED","PILLOW"]):
        return "Bedroom & Textiles"
    if any(k in desc for k in ["JEWEL","RING","NECKLACE","BRACELET","EARRING"]):
        return "Jewellery & Accessories"
    if any(k in desc for k in ["GARDEN","BIRD","PLANT","FLOWER","SEED"]):
        return "Garden & Outdoors"
    return "General Merchandise"

CITIES  = ["London","Birmingham","Manchester","Leeds","Glasgow",
           "Sheffield","Liverpool","Edinburgh","Bristol","Cardiff",
           "Newcastle","Leicester","Coventry","Nottingham","Brighton"]
GENDERS = ["M","F"]

random.seed(42)

def stable_user_id(customer_id):
    """Convert Customer ID (float/NaN) to stable U-prefixed string."""
    if pd.isna(customer_id):
        return None
    cid = str(int(customer_id))
    return f"U{cid}"

def stable_product_id(stock_code):
    """Normalise StockCode → P-prefixed 6-char hex."""
    h = hashlib.md5(str(stock_code).encode()).hexdigest()[:5].upper()
    return f"P{h}"

# ──────────────── Step 1 : Extract xlsx from zip ───────────────────────────
print("[1/5] Extracting zip …")
extract_dir = os.path.join(BASE_DIR, "extracted")
os.makedirs(extract_dir, exist_ok=True)

with zipfile.ZipFile(ZIP_FILE, "r") as z:
    names = z.namelist()
    print(f"      Files inside zip: {names}")
    z.extractall(extract_dir)

xlsx_files = [f for f in os.listdir(extract_dir) if f.endswith(".xlsx")]
if not xlsx_files:
    raise FileNotFoundError("No .xlsx found inside the zip!")

xlsx_path = os.path.join(extract_dir, xlsx_files[0])
print(f"      Reading: {xlsx_path}")

# The file has two sheets: Year 2009-2010 and Year 2010-2011
print("      Loading sheet 'Year 2009-2010' …")
df1 = pd.read_excel(xlsx_path, sheet_name="Year 2009-2010", dtype=str)
print("      Loading sheet 'Year 2010-2011' …")
df2 = pd.read_excel(xlsx_path, sheet_name="Year 2010-2011", dtype=str)
df  = pd.concat([df1, df2], ignore_index=True)
print(f"      Total rows loaded: {len(df):,}")

# ──────────────── Step 2 : Save raw CSV ───────────────────────────────────
df.to_csv(RAW_CSV, index=False)
print(f"[2/5] Raw CSV saved → {RAW_CSV}  ({len(df):,} rows)")

# ──────────────── Step 3 : Clean & map to project schema ──────────────────
print("[3/5] Cleaning …")

# Rename columns
df.columns = [c.strip() for c in df.columns]
# Expected: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country

df["Price"]    = pd.to_numeric(df["Price"],    errors="coerce")
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["Customer ID"] = pd.to_numeric(df["Customer ID"], errors="coerce")

# Drop rows with missing critical fields
before = len(df)
df.dropna(subset=["Customer ID","StockCode","Description","Price","Quantity","InvoiceDate"],
          inplace=True)
# Drop negative quantities (returns) and zero prices
df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
after = len(df)
print(f"      Removed {before - after:,} dirty/return rows  |  Clean: {after:,}")

# Build project schema columns
df["user_id"]      = df["Customer ID"].apply(stable_user_id)
df["product_id"]   = df["StockCode"].apply(stable_product_id)
df["product_name"] = df["Description"].str.strip().str.title()
df["category"]     = df["Description"].apply(guess_category)
df["price"]        = df["Price"].round(2)
df["quantity"]     = df["Quantity"].astype(int)

# Parse timestamp properly
df["InvoiceDate"]  = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df.dropna(subset=["InvoiceDate"], inplace=True)
df["timestamp"]    = df["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

# Synthetic city & gender (not in original dataset)
df["city"]   = [random.choice(CITIES)  for _ in range(len(df))]
df["gender"] = [random.choice(GENDERS) for _ in range(len(df))]

# Synthetic rating (1.0–5.0) — derived loosely from price tier
def price_rating(p):
    if p < 1:   return round(random.uniform(1.0, 2.5), 1)
    if p < 5:   return round(random.uniform(2.5, 3.5), 1)
    if p < 20:  return round(random.uniform(3.0, 4.5), 1)
    return           round(random.uniform(3.5, 5.0), 1)

df["rating"] = df["price"].apply(price_rating)

# Final column order
SCHEMA = ["user_id","product_id","product_name","category",
          "price","quantity","timestamp","city","gender","rating"]
clean_df = df[SCHEMA].copy()
clean_df.to_csv(CLEAN_CSV, index=False)
print(f"[3/5] Clean CSV saved → {CLEAN_CSV}  ({len(clean_df):,} rows)")
print(f"      Sample:\n{clean_df.head(3).to_string()}")

# ──────────────── Step 4 : Scale 10× for Big Data simulation ──────────────
print(f"\n[4/5] Scaling {SCALE_FACTOR}× to simulate Big Data …")
scaled_df = pd.concat([clean_df] * SCALE_FACTOR, ignore_index=True)
scaled_df  = scaled_df.sample(frac=1, random_state=42).reset_index(drop=True)

# Add slight price noise to make duplicates less identical
scaled_df["price"] = (
    scaled_df["price"] * (1 + scaled_df.index.map(lambda i: random.uniform(-0.02, 0.02)))
).round(2)

scaled_df.to_csv(SCALED_CSV, index=False)
size_mb = os.path.getsize(SCALED_CSV) / (1024 * 1024)
print(f"      Scaled CSV: {len(scaled_df):,} rows  |  {size_mb:.1f} MB  → {SCALED_CSV}")

# ──────────────── Step 5 : HDFS-ready (no header, comma-delimited) ─────────
print("\n[5/5] Writing HDFS-ready CSV (no header) …")
scaled_df.to_csv(HDFS_CSV, index=False, header=False)
print(f"      HDFS CSV: {HDFS_CSV}")

# ──────────────── Summary ─────────────────────────────────────────────────
print("\n" + "═"*55)
print("✅  Pre-processing complete!")
print(f"   Raw            : {RAW_CSV}")
print(f"   Cleaned        : {CLEAN_CSV}  ({len(clean_df):,} rows)")
print(f"   Scaled ({SCALE_FACTOR}×)    : {SCALED_CSV}  ({len(scaled_df):,} rows, {size_mb:.1f} MB)")
print(f"   HDFS-ready     : {HDFS_CSV}  (no header)")
print("═"*55)

print("\n📊 Category distribution:")
print(clean_df["category"].value_counts().to_string())
print("\n📊 Sample statistics:")
print(clean_df[["price","quantity","rating"]].describe().round(2).to_string())
