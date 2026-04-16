#!/usr/bin/env python3
"""
Smart Retail Analytics Platform
Dataset Generator & Scaler
Generates a realistic retail dataset and scales it to simulate Big Data (10x–100x)
"""

import csv
import random
import os
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
OUTPUT_FILE   = "retail_data_raw.csv"
SCALED_FILE   = "retail_data_scaled.csv"
CLEANED_FILE  = "retail_data_cleaned.csv"
BASE_ROWS     = 10_000   # base dataset size
SCALE_FACTOR  = 10       # multiply to simulate Big Data (use 100 for larger)

# ─────────────────────────────────────────────
# Reference Tables
# ─────────────────────────────────────────────
CATEGORIES = {
    "Electronics":   ["Laptop", "Smartphone", "Tablet", "Smartwatch", "Headphones",
                       "Bluetooth Speaker", "Camera", "Gaming Console", "Monitor", "Keyboard"],
    "Clothing":      ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress",
                       "Hoodie", "Formal Shirt", "Shorts", "Boots", "Saree"],
    "Home & Kitchen":["Mixer Grinder", "Pressure Cooker", "Vacuum Cleaner", "Air Purifier",
                       "Microwave", "Refrigerator", "LED Bulb", "Curtains", "Bed Sheet", "Pillow"],
    "Books":         ["Python Programming", "Data Structures", "Big Data Analytics",
                       "Machine Learning", "Deep Learning", "Cloud Computing",
                       "Algorithms", "Database Systems", "Operating Systems", "Networks"],
    "Sports":        ["Cricket Bat", "Football", "Tennis Racket", "Badminton Set",
                       "Yoga Mat", "Dumbbells", "Cycling Gloves", "Swimming Goggles",
                       "Running Shoes", "Jump Rope"],
    "Grocery":       ["Basmati Rice", "Organic Wheat", "Almonds", "Olive Oil",
                       "Green Tea", "Protein Powder", "Oats", "Honey", "Coffee Beans", "Dark Chocolate"],
    "Beauty":        ["Face Cream", "Shampoo", "Conditioner", "Sunscreen",
                       "Lipstick", "Foundation", "Perfume", "Hair Serum", "Body Lotion", "Face Wash"],
    "Toys":          ["LEGO Set", "Board Game", "RC Car", "Action Figure",
                       "Doll", "Puzzle", "Building Blocks", "Toy Kitchen", "Nerf Gun", "Scrabble"],
}

CATEGORY_PRICE_RANGE = {
    "Electronics":    (500,  80000),
    "Clothing":       (200,   5000),
    "Home & Kitchen": (300,  25000),
    "Books":          (100,   1200),
    "Sports":         (250,  15000),
    "Grocery":        (50,    2000),
    "Beauty":         (100,   3500),
    "Toys":           (150,   4000),
}

CITIES   = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Kolkata",
            "Pune", "Ahmedabad", "Jaipur", "Lucknow"]
GENDERS  = ["M", "F"]


def random_timestamp(start_year=2022, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    delta = end - start
    random_days    = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    return (start + timedelta(days=random_days, seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")


def generate_base_dataset(n=BASE_ROWS):
    """Generate n rows of synthetic retail transaction data."""
    print(f"[INFO] Generating {n:,} base rows...")
    rows = []
    categories = list(CATEGORIES.keys())

    for i in range(1, n + 1):
        user_id    = f"U{random.randint(1000, 9999)}"
        product_id = f"P{random.randint(10000, 99999)}"
        category   = random.choice(categories)
        product    = random.choice(CATEGORIES[category])
        low, high  = CATEGORY_PRICE_RANGE[category]
        price      = round(random.uniform(low, high), 2)
        quantity   = random.randint(1, 5)
        timestamp  = random_timestamp()
        city       = random.choice(CITIES)
        gender     = random.choice(GENDERS)
        rating     = round(random.uniform(1.0, 5.0), 1)

        # Introduce some nulls (5%) to simulate dirty data
        if random.random() < 0.05:
            price = ""
        if random.random() < 0.03:
            user_id = ""
        if random.random() < 0.02:
            category = ""

        rows.append([user_id, product_id, product, category, price,
                     quantity, timestamp, city, gender, rating])

    return rows


def write_csv(filename, header, rows):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"[INFO] Written {len(rows):,} rows → {filename}")


def scale_dataset(input_file, output_file, factor=SCALE_FACTOR):
    """Duplicate dataset factor times to simulate Big Data volumes."""
    print(f"[INFO] Scaling dataset {factor}x → {output_file}")
    with open(input_file, "r", encoding="utf-8") as f:
        reader   = csv.reader(f)
        header   = next(reader)
        all_rows = list(reader)

    scaled_rows = all_rows * factor
    random.shuffle(scaled_rows)

    write_csv(output_file, header, scaled_rows)
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"[INFO] Scaled file size: {size_mb:.2f} MB  |  Total rows: {len(scaled_rows):,}")


def clean_dataset(input_file, output_file):
    """Remove rows with null/empty critical fields."""
    print(f"[INFO] Cleaning dataset → {output_file}")
    critical_cols = [0, 1, 3, 4, 5, 6]  # user_id, product_id, category, price, qty, timestamp
    clean_rows  = []
    dirty_count = 0

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) < 10:
                dirty_count += 1
                continue
            if any(row[i].strip() == "" for i in critical_cols):
                dirty_count += 1
                continue
            try:
                float(row[4])   # price must be numeric
                int(row[5])     # quantity must be integer
            except ValueError:
                dirty_count += 1
                continue
            clean_rows.append(row)

    write_csv(output_file, header, clean_rows)
    print(f"[INFO] Removed {dirty_count:,} dirty rows  |  Clean rows: {len(clean_rows):,}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    HEADER = ["user_id", "product_id", "product_name", "category",
              "price", "quantity", "timestamp", "city", "gender", "rating"]

    base_rows = generate_base_dataset(BASE_ROWS)
    write_csv(OUTPUT_FILE, HEADER, base_rows)

    scale_dataset(OUTPUT_FILE, SCALED_FILE, SCALE_FACTOR)
    clean_dataset(SCALED_FILE, CLEANED_FILE)

    print("\n✅ Dataset generation complete!")
    print(f"   Raw      : {OUTPUT_FILE}")
    print(f"   Scaled   : {SCALED_FILE}  ({SCALE_FACTOR}x)")
    print(f"   Cleaned  : {CLEANED_FILE}")
