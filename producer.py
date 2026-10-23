from kafka import KafkaProducer
import pandas as pd
import json
import math

# ─────────────────────────────────────────────────────────
# Helper: bersihkan nilai NaN / Inf agar bisa di-serialize JSON
# ─────────────────────────────────────────────────────────
def clean_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# ─────────────────────────────────────────────────────────
# Inisiasi Kafka Producer
# ─────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',           # tunggu konfirmasi dari semua replika
    retries=3             # retry otomatis jika gagal
)

# ─────────────────────────────────────────────────────────
# Pemetaan topik Kafka → file CSV
# ─────────────────────────────────────────────────────────
DATASETS = {
    'ecommerce_customers':          'data/olist_customers_dataset.csv',
    'ecommerce_orders':             'data/olist_orders_dataset.csv',
    'ecommerce_order_items':        'data/olist_order_items_dataset.csv',
    'ecommerce_payments':           'data/olist_order_payments_dataset.csv',
    'ecommerce_reviews':            'data/olist_order_reviews_dataset.csv',
    'ecommerce_products':           'data/olist_products_dataset.csv',
    'ecommerce_sellers':            'data/olist_sellers_dataset.csv',
    'ecommerce_geolocation':        'data/olist_geolocation_dataset.csv',
    'ecommerce_category_trans':     'data/product_category_name_translation.csv',
}

BATCH_FLUSH = 5_000   # flush setiap N baris untuk performa

# ─────────────────────────────────────────────────────────
# Kirim setiap dataset ke topik Kafka masing-masing
# ─────────────────────────────────────────────────────────
print("=" * 60)
print("  Olist Kafka Producer — Memulai Ingestion")
print("=" * 60)

try:
    for topic, filepath in DATASETS.items():
        print(f"\n▶  {filepath}  →  topik '{topic}'")
        try:
            df = pd.read_csv(filepath, low_memory=False)
        except FileNotFoundError:
            print(f"   ⚠  File tidak ditemukan: {filepath}. Dilewati.")
            continue

        total = len(df)
        for idx, row in df.iterrows():
            record = {k: clean_value(v) for k, v in row.to_dict().items()}
            producer.send(topic, record)

            if (idx + 1) % BATCH_FLUSH == 0:
                producer.flush()
                print(f"   {idx + 1:>10,} / {total:,} baris terkirim...")

        producer.flush()   # pastikan sisa buffer terkirim
        print(f"   ✓  Selesai — {total:,} baris dikirim ke '{topic}'")

    print("\n" + "=" * 60)
    print("  Ingestion selesai untuk SEMUA dataset.")
    print("=" * 60)
finally:
    producer.close()