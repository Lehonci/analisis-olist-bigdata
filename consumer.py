from kafka import KafkaConsumer
import psycopg2
import json

# Konfigurasi koneksi PostgreSQL
# ─────────────────────────────────────────────────────────
# Koneksi PostgreSQL
# ─────────────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="olist_db", user="postgres", password="postgres", host="localhost", port="5432"
)
cursor = conn.cursor()

# ─────────────────────────────────────────────────────────
# Buat 9 tabel staging (jika belum ada)
# ─────────────────────────────────────────────────────────
CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS staging_customers (
        customer_id            VARCHAR PRIMARY KEY,
        customer_unique_id     VARCHAR,
        customer_zip_code_prefix INT,
        customer_city          VARCHAR,
        customer_state         VARCHAR
    )""",

    """CREATE TABLE IF NOT EXISTS staging_orders (
        order_id                        VARCHAR PRIMARY KEY,
        customer_id                     VARCHAR,
        order_status                    VARCHAR,
        order_purchase_timestamp        VARCHAR,
        order_approved_at               VARCHAR,
        order_delivered_carrier_date    VARCHAR,
        order_delivered_customer_date   VARCHAR,
        order_estimated_delivery_date   VARCHAR
    )""",

    """CREATE TABLE IF NOT EXISTS staging_order_items (
        order_id            VARCHAR,
        order_item_id       INT,
        product_id          VARCHAR,
        seller_id           VARCHAR,
        shipping_limit_date VARCHAR,
        price               DOUBLE PRECISION,
        freight_value       DOUBLE PRECISION,
        PRIMARY KEY (order_id, order_item_id)
    )""",

    """CREATE TABLE IF NOT EXISTS staging_payments (
        order_id              VARCHAR,
        payment_sequential    INT,
        payment_type          VARCHAR,
        payment_installments  INT,
        payment_value         DOUBLE PRECISION,
        PRIMARY KEY (order_id, payment_sequential)
    )""",

    """CREATE TABLE IF NOT EXISTS staging_reviews (
        review_id              VARCHAR,
        order_id               VARCHAR,
        review_score           INT,
        review_creation_date   VARCHAR,
        review_answer_timestamp VARCHAR,
        PRIMARY KEY (review_id, order_id)
    )""",

    """CREATE TABLE IF NOT EXISTS staging_products (
        product_id                 VARCHAR PRIMARY KEY,
        product_category_name      VARCHAR,
        product_name_lenght        INT,
        product_description_lenght INT,
        product_photos_qty         INT,
        product_weight_g           DOUBLE PRECISION,
        product_length_cm          DOUBLE PRECISION,
        product_height_cm          DOUBLE PRECISION,
        product_width_cm           DOUBLE PRECISION
    )""",

    """CREATE TABLE IF NOT EXISTS staging_sellers (
        seller_id               VARCHAR PRIMARY KEY,
        seller_zip_code_prefix  INT,
        seller_city             VARCHAR,
        seller_state            VARCHAR
    )""",

    """CREATE TABLE IF NOT EXISTS staging_geolocation (
        id                          SERIAL PRIMARY KEY,
        geolocation_zip_code_prefix INT,
        geolocation_lat             DOUBLE PRECISION,
        geolocation_lng             DOUBLE PRECISION,
        geolocation_city            VARCHAR,
        geolocation_state           VARCHAR
    )""",

    """CREATE TABLE IF NOT EXISTS staging_category_translation (
        product_category_name         VARCHAR PRIMARY KEY,
        product_category_name_english VARCHAR
    )""",
]

print("Membuat tabel staging...")
for sql in CREATE_TABLES:
    cursor.execute(sql)
conn.commit()
print("✓ Semua tabel staging berhasil dibuat / sudah ada.\n")

# Inisiasi Kafka Consumer
# ─────────────────────────────────────────────────────────
# Query INSERT per topik (named parameters → %(col)s)
# ─────────────────────────────────────────────────────────
INSERT_QUERIES = {
    'ecommerce_customers': """
        INSERT INTO staging_customers
            (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        VALUES
            (%(customer_id)s, %(customer_unique_id)s, %(customer_zip_code_prefix)s,
             %(customer_city)s, %(customer_state)s)
        ON CONFLICT (customer_id) DO NOTHING
    """,

    'ecommerce_orders': """
        INSERT INTO staging_orders
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date,
             order_delivered_customer_date, order_estimated_delivery_date)
        VALUES
            (%(order_id)s, %(customer_id)s, %(order_status)s, %(order_purchase_timestamp)s,
             %(order_approved_at)s, %(order_delivered_carrier_date)s,
             %(order_delivered_customer_date)s, %(order_estimated_delivery_date)s)
        ON CONFLICT (order_id) DO NOTHING
    """,

    'ecommerce_order_items': """
        INSERT INTO staging_order_items
            (order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value)
        VALUES
            (%(order_id)s, %(order_item_id)s, %(product_id)s, %(seller_id)s,
             %(shipping_limit_date)s, %(price)s, %(freight_value)s)
        ON CONFLICT (order_id, order_item_id) DO NOTHING
    """,

    'ecommerce_payments': """
        INSERT INTO staging_payments
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
        VALUES
            (%(order_id)s, %(payment_sequential)s, %(payment_type)s,
             %(payment_installments)s, %(payment_value)s)
        ON CONFLICT (order_id, payment_sequential) DO NOTHING
    """,

    'ecommerce_reviews': """
        INSERT INTO staging_reviews
            (review_id, order_id, review_score, review_creation_date, review_answer_timestamp)
        VALUES
            (%(review_id)s, %(order_id)s, %(review_score)s,
             %(review_creation_date)s, %(review_answer_timestamp)s)
        ON CONFLICT (review_id, order_id) DO NOTHING
    """,

    'ecommerce_products': """
        INSERT INTO staging_products
            (product_id, product_category_name, product_name_lenght,
             product_description_lenght, product_photos_qty,
             product_weight_g, product_length_cm, product_height_cm, product_width_cm)
        VALUES
            (%(product_id)s, %(product_category_name)s, %(product_name_lenght)s,
             %(product_description_lenght)s, %(product_photos_qty)s,
             %(product_weight_g)s, %(product_length_cm)s, %(product_height_cm)s, %(product_width_cm)s)
        ON CONFLICT (product_id) DO NOTHING
    """,

    'ecommerce_sellers': """
        INSERT INTO staging_sellers
            (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        VALUES
            (%(seller_id)s, %(seller_zip_code_prefix)s, %(seller_city)s, %(seller_state)s)
        ON CONFLICT (seller_id) DO NOTHING
    """,

    'ecommerce_geolocation': """
        INSERT INTO staging_geolocation
            (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
             geolocation_city, geolocation_state)
        VALUES
            (%(geolocation_zip_code_prefix)s, %(geolocation_lat)s, %(geolocation_lng)s,
             %(geolocation_city)s, %(geolocation_state)s)
    """,

    'ecommerce_category_trans': """
        INSERT INTO staging_category_translation
            (product_category_name, product_category_name_english)
        VALUES
            (%(product_category_name)s, %(product_category_name_english)s)
        ON CONFLICT (product_category_name) DO NOTHING
    """,
}

# ─────────────────────────────────────────────────────────
# Inisiasi Kafka Consumer — subscribe ke semua 9 topik
# ─────────────────────────────────────────────────────────
TOPICS = list(INSERT_QUERIES.keys())

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=['localhost:9092'],
    group_id='olist-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=30_000,   # berhenti setelah 30 detik tanpa pesan baru
)

print("Menunggu data dari Kafka untuk disimpan ke PostgreSQL...")
print("Menunggu data dari Kafka...")
print(f"Topik yang dimonitor: {', '.join(TOPICS)}\n")

BATCH_SIZE = 500
count = 0
topic_counts = {t: 0 for t in TOPICS}

# ─────────────────────────────────────────────────────────
# Proses pesan masuk
# ─────────────────────────────────────────────────────────
try:
    for message in consumer:
        topic = message.topic
        data = message.value

        try:
            cursor.execute(INSERT_QUERIES[topic], data)
        except Exception as e:
            conn.rollback()
            print(f"  ✗ Error pada topik '{topic}': {e}")
            raise

        count += 1
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

        if count % BATCH_SIZE == 0:
            conn.commit()
            consumer.commit()
            print(f"  {count:>10,} pesan diproses  |  Topik terakhir: {topic}")

    # Commit sisa pesan ke database sebelum mengakui offset Kafka.
    conn.commit()
    consumer.commit()
finally:
    consumer.close()
    cursor.close()
    conn.close()

print("\n" + "=" * 60)
print(f"  Ingestion selesai — Total {count:,} pesan tersimpan.")
print("=" * 60)
print("\nRincian per topik:")
for t, c in topic_counts.items():
    print(f"  {t:<40} {c:>10,} baris")