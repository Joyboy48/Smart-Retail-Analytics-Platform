#!/usr/bin/env python3
"""
Smart Retail Analytics Platform
Python Visualization — Generates all charts from processed data
Uses: pandas, matplotlib, seaborn, plotly
"""

import os, sys, json
import pandas as pd
import matplotlib
matplotlib.use('Agg')           # headless mode (no display needed)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ──────────────── Config ──────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, 'pig_results')
CHART_DIR   = os.path.join(BASE_DIR, 'charts')
DASH_DATA   = os.path.join(BASE_DIR, '..', 'dashboard', 'data.js')

os.makedirs(CHART_DIR, exist_ok=True)

DATASET_CSV = os.path.join(BASE_DIR, '..', 'dataset', 'retail_cleaned.csv')

# Palette
PALETTE     = ['#6366f1','#8b5cf6','#ec4899','#f59e0b','#10b981',
               '#3b82f6','#ef4444','#14b8a6','#f97316','#84cc16']

plt.rcParams.update({
    'figure.facecolor': '#0f0f1a',
    'axes.facecolor':   '#1a1a2e',
    'axes.edgecolor':   '#334155',
    'text.color':       '#e2e8f0',
    'axes.labelcolor':  '#e2e8f0',
    'xtick.color':      '#94a3b8',
    'ytick.color':      '#94a3b8',
    'grid.color':       '#1e293b',
    'grid.alpha':       0.5,
    'font.family':      'DejaVu Sans',
})

sns.set_palette(PALETTE)

# ──────────────── Load Data ───────────────────────────────────────
def load_data():
    """Load cleaned retail CSV directly (fallback if Pig not run yet)."""
    print("[INFO] Loading dataset…")
    df = pd.read_csv(DATASET_CSV, parse_dates=['timestamp'])
    df['total_revenue'] = df['price'] * df['quantity']
    df['year_month']    = df['timestamp'].dt.to_period('M').astype(str)
    df['year']          = df['timestamp'].dt.year.astype(str)
    df['month_name']    = df['timestamp'].dt.strftime('%b %Y')
    print(f"[INFO] Loaded {len(df):,} rows  |  Columns: {list(df.columns)}")
    return df


# ──────────────── Chart 1: Top 15 Products ────────────────────────
def chart_top_products(df):
    top = (df.groupby('product_name')['total_revenue']
              .sum().nlargest(15).reset_index())
    top.columns = ['product_name', 'revenue']
    top['short'] = top['product_name'].str[:30]

    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.barh(top['short'][::-1], top['revenue'][::-1],
                   color=PALETTE[:15][::-1], edgecolor='none', height=0.7)
    for bar, val in zip(bars, top['revenue'][::-1]):
        ax.text(bar.get_width() + max(top['revenue']) * 0.01,
                bar.get_y() + bar.get_height()/2,
                f'£{val:,.0f}', va='center', fontsize=8, color='#94a3b8')

    ax.set_xlabel('Total Revenue (£)', fontsize=11)
    ax.set_title('🏆 Top 15 Best-Selling Products by Revenue', fontsize=14,
                 fontweight='bold', pad=15, color='#e2e8f0')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'£{x:,.0f}'))
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, '01_top_products.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
    plt.close()
    print(f"[✅] Chart saved: {out}")
    return top


# ──────────────── Chart 2: Revenue by Category ───────────────────
def chart_revenue_category(df):
    cat = (df.groupby('category')['total_revenue']
              .sum().sort_values(ascending=False).reset_index())

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Pie
    wedges, texts, autotexts = axes[0].pie(
        cat['total_revenue'], labels=cat['category'],
        autopct='%1.1f%%', startangle=140,
        colors=PALETTE[:len(cat)], pctdistance=0.8,
        wedgeprops={'edgecolor': '#0f0f1a', 'linewidth': 2}
    )
    for t in texts:     t.set_color('#e2e8f0')
    for t in autotexts: t.set_color('#0f0f1a'); t.set_fontweight('bold')
    axes[0].set_title('Revenue Share by Category', fontsize=13,
                      fontweight='bold', color='#e2e8f0')

    # Bar
    axes[1].bar(cat['category'], cat['total_revenue'],
                color=PALETTE[:len(cat)], edgecolor='none')
    axes[1].set_xticklabels(cat['category'], rotation=35, ha='right', fontsize=9)
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'£{x/1e6:.1f}M'))
    axes[1].set_title('Total Revenue per Category', fontsize=13,
                      fontweight='bold', color='#e2e8f0')
    axes[1].set_ylabel('Revenue', fontsize=11)
    axes[1].grid(axis='y', alpha=0.3)

    fig.suptitle('📦 Revenue by Product Category', fontsize=16,
                 fontweight='bold', color='#e2e8f0', y=1.01)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, '02_revenue_by_category.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
    plt.close()
    print(f"[✅] Chart saved: {out}")
    return cat


# ──────────────── Chart 3: Monthly Revenue Trend ─────────────────
def chart_monthly_trends(df):
    monthly = (df.groupby('year_month')['total_revenue']
                 .sum().reset_index().sort_values('year_month'))
    monthly['revenue_m'] = monthly['total_revenue'] / 1e6

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.fill_between(range(len(monthly)), monthly['revenue_m'],
                    alpha=0.3, color='#6366f1')
    ax.plot(range(len(monthly)), monthly['revenue_m'],
            color='#6366f1', linewidth=2.5, marker='o', markersize=5)

    ax.set_xticks(range(0, len(monthly), max(1, len(monthly)//12)))
    ax.set_xticklabels(monthly['year_month'].iloc[::max(1, len(monthly)//12)],
                       rotation=45, ha='right', fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'£{x:.1f}M'))
    ax.set_title('📈 Monthly Revenue Trend (2009–2011)', fontsize=14,
                 fontweight='bold', pad=15, color='#e2e8f0')
    ax.set_ylabel('Revenue (£ Millions)', fontsize=11)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, '03_monthly_trends.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
    plt.close()
    print(f"[✅] Chart saved: {out}")
    return monthly


# ──────────────── Chart 4: Customer Segments ─────────────────────
def chart_customer_segments(df):
    spend = df.groupby('user_id')['total_revenue'].sum().reset_index()
    spend.columns = ['user_id', 'total_spend']
    spend['segment'] = pd.cut(
        spend['total_spend'],
        bins=[-1, 200, 1000, 5000, float('inf')],
        labels=['Occasional', 'Regular', 'High Value', 'VIP']
    )

    counts = spend['segment'].value_counts().reset_index()
    counts.columns = ['segment', 'count']
    seg_rev = spend.groupby('segment')['total_spend'].sum().reset_index()
    seg_rev.columns = ['segment', 'revenue']

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Customer count donut
    wedges, texts, autos = axes[0].pie(
        counts['count'], labels=counts['segment'],
        autopct='%1.0f%%', startangle=90,
        colors=['#3b82f6','#10b981','#f59e0b','#ef4444'],
        pctdistance=0.75, wedgeprops={'width': 0.5, 'edgecolor': '#0f0f1a', 'linewidth': 2}
    )
    for t in texts: t.set_color('#e2e8f0'); t.set_fontsize(10)
    for t in autos: t.set_color('#0f0f1a'); t.set_fontweight('bold')
    axes[0].set_title('Customer Count by Segment', fontsize=12,
                      fontweight='bold', color='#e2e8f0')

    # Revenue bars
    merged = counts.merge(seg_rev, on='segment')
    x = range(len(merged))
    colors = ['#3b82f6','#10b981','#f59e0b','#ef4444']
    bars = axes[1].bar(merged['segment'], merged['revenue'],
                       color=colors, edgecolor='none', width=0.5)
    for bar, (_, row) in zip(bars, merged.iterrows()):
        axes[1].text(bar.get_x() + bar.get_width()/2,
                     bar.get_height() + max(merged['revenue']) * 0.01,
                     f'{row["count"]:,}\ncustomers',
                     ha='center', fontsize=8, color='#94a3b8')
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'£{x/1e6:.1f}M'))
    axes[1].set_title('Revenue per Customer Segment', fontsize=12,
                      fontweight='bold', color='#e2e8f0')
    axes[1].set_ylabel('Total Revenue', fontsize=11)
    axes[1].grid(axis='y', alpha=0.3)

    fig.suptitle('👥 Customer Segmentation (RFM-based)', fontsize=14,
                 fontweight='bold', color='#e2e8f0', y=1.02)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, '04_customer_segments.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
    plt.close()
    print(f"[✅] Chart saved: {out}")
    return spend


# ──────────────── Chart 5: City Revenue Heatmap ───────────────────
def chart_city_category(df):
    pivot = df.pivot_table(
        index='city', columns='category',
        values='total_revenue', aggfunc='sum', fill_value=0
    ) / 1e3  # in thousands

    fig, ax = plt.subplots(figsize=(16, 7))
    sns.heatmap(pivot, ax=ax, cmap='YlOrRd', fmt='.0f', annot=True,
                linewidths=0.5, linecolor='#0f0f1a',
                cbar_kws={'label': 'Revenue (£ thousands)'})
    ax.set_title('🗺  City × Category Revenue Heatmap (£ thousands)',
                 fontsize=14, fontweight='bold', pad=15, color='#e2e8f0')
    ax.set_xlabel('Category', fontsize=11)
    ax.set_ylabel('City', fontsize=11)
    plt.xticks(rotation=30, ha='right', fontsize=9)
    plt.yticks(rotation=0, fontsize=9)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, '05_city_category_heatmap.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
    plt.close()
    print(f"[✅] Chart saved: {out}")


# ──────────────── Export data.js for dashboard ───────────────────
def export_dashboard_data(df, top_products, cat_revenue, monthly, segments):
    print("[INFO] Generating dashboard/data.js…")

    top10 = top_products.head(10)
    cat   = cat_revenue.head(8)

    monthly_rev = monthly.groupby('year_month')['total_revenue'].sum().reset_index()
    monthly_rev = monthly_rev.sort_values('year_month').tail(24)

    seg_counts = segments['segment'].value_counts()

    kpis = {
        'total_revenue':   round(float(df['total_revenue'].sum()), 2),
        'total_orders':    int(len(df)),
        'unique_customers':int(df['user_id'].nunique()),
        'unique_products': int(df['product_id'].nunique()),
        'avg_order_value': round(float(df['total_revenue'].mean()), 2),
        'top_category':    str(cat.iloc[0]['category']),
        'date_range':      f"{df['timestamp'].min().strftime('%b %Y')} – {df['timestamp'].max().strftime('%b %Y')}",
    }

    data_js = f"""// Auto-generated by python_viz/generate_charts.py
const RETAIL_DATA = {{
  kpis: {json.dumps(kpis, indent=4)},
  topProducts: {{
    labels: {json.dumps(list(top10['product_name'].str[:25]))},
    revenues: {json.dumps(list(top10['revenue'].round(2)))}
  }},
  categoryRevenue: {{
    labels: {json.dumps(list(cat['category']))},
    revenues: {json.dumps(list(cat['total_revenue'].round(2)))}
  }},
  monthlyTrends: {{
    labels: {json.dumps(list(monthly_rev['year_month']))},
    revenues: {json.dumps(list(monthly_rev['total_revenue'].round(2)))}
  }},
  customerSegments: {{
    labels: {json.dumps(list(seg_counts.index))},
    counts: {json.dumps(list(seg_counts.values.tolist()))}
  }}
}};
"""
    os.makedirs(os.path.dirname(DASH_DATA), exist_ok=True)
    with open(DASH_DATA, 'w') as f:
        f.write(data_js)
    print(f"[✅] Dashboard data exported → {DASH_DATA}")


# ──────────────── Main ────────────────────────────────────────────
if __name__ == '__main__':
    print("╔══════════════════════════════════════════════════╗")
    print("║  Smart Retail Analytics — Chart Generator        ║")
    print("╚══════════════════════════════════════════════════╝\n")

    df = load_data()

    print("\n─── Generating Charts ───")
    top = chart_top_products(df)
    cat = chart_revenue_category(df)
    mon = chart_monthly_trends(df)
    seg = chart_customer_segments(df)
    chart_city_category(df)

    print("\n─── Exporting Dashboard Data ───")
    export_dashboard_data(df, top, cat, mon, seg)

    print(f"\n✅  All charts saved to: {CHART_DIR}/")
    print("   01_top_products.png")
    print("   02_revenue_by_category.png")
    print("   03_monthly_trends.png")
    print("   04_customer_segments.png")
    print("   05_city_category_heatmap.png")
    print("\n   Open dashboard/index.html in your browser!")
