import json
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
from config import (
    DB_CONN_PARAMS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    STAGING_TABLES,
)

CREATE_TABLES = [
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['customers']} (
        customer_id              VARCHAR PRIMARY KEY,
        customer_unique_id       VARCHAR,
        customer_zip_code_prefix INT,
        customer_city            VARCHAR,
        customer_state           VARCHAR
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['orders']} (
        order_id                      VARCHAR PRIMARY KEY,
        customer_id                   VARCHAR,
        order_status                  VARCHAR,
        order_purchase_timestamp      VARCHAR,
        order_approved_at             VARCHAR,
        order_delivered_carrier_date  VARCHAR,
        order_delivered_customer_date VARCHAR,
        order_estimated_delivery_date VARCHAR
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['order_items']} (
        order_id            VARCHAR,
        order_item_id       INT,
        product_id          VARCHAR,
        seller_id           VARCHAR,
        shipping_limit_date VARCHAR,
        price               DOUBLE PRECISION,
        freight_value       DOUBLE PRECISION,
        PRIMARY KEY (order_id, order_item_id)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['payments']} (
        order_id             VARCHAR,
        payment_sequential   INT,
        payment_type         VARCHAR,
        payment_installments INT,
        payment_value        DOUBLE PRECISION,
        PRIMARY KEY (order_id, payment_sequential)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['reviews']} (
        review_id               VARCHAR,
        order_id                VARCHAR,
        review_score            INT,
        review_comment_title    TEXT,
        review_comment_message  TEXT,
        review_creation_date    VARCHAR,
        review_answer_timestamp VARCHAR,
        PRIMARY KEY (review_id, order_id)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['products']} (
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
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['sellers']} (
        seller_id              VARCHAR PRIMARY KEY,
        seller_zip_code_prefix INT,
        seller_city            VARCHAR,
        seller_state           VARCHAR
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['geolocation']} (
        id                          SERIAL PRIMARY KEY,
        geolocation_zip_code_prefix INT,
        geolocation_lat             DOUBLE PRECISION,
        geolocation_lng             DOUBLE PRECISION,
        geolocation_city            VARCHAR,
        geolocation_state           VARCHAR
    )""",
    f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLES['category_translation']} (
        product_category_name         VARCHAR PRIMARY KEY,
        product_category_name_english VARCHAR
    )""",
]

INSERT_QUERIES = {
    "ecommerce_customers": f"""
        INSERT INTO {STAGING_TABLES['customers']}
            (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        VALUES
            (%(customer_id)s, %(customer_unique_id)s, %(customer_zip_code_prefix)s,
             %(customer_city)s, %(customer_state)s)
        ON CONFLICT (customer_id) DO NOTHING
    """,
    "ecommerce_orders": f"""
        INSERT INTO {STAGING_TABLES['orders']}
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date,
             order_delivered_customer_date, order_estimated_delivery_date)
        VALUES
            (%(order_id)s, %(customer_id)s, %(order_status)s, %(order_purchase_timestamp)s,
             %(order_approved_at)s, %(order_delivered_carrier_date)s,
             %(order_delivered_customer_date)s, %(order_estimated_delivery_date)s)
        ON CONFLICT (order_id) DO NOTHING
    """,
    "ecommerce_order_items": f"""
        INSERT INTO {STAGING_TABLES['order_items']}
            (order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value)
        VALUES
            (%(order_id)s, %(order_item_id)s, %(product_id)s, %(seller_id)s,
             %(shipping_limit_date)s, %(price)s, %(freight_value)s)
        ON CONFLICT (order_id, order_item_id) DO NOTHING
    """,
    "ecommerce_payments": f"""
        INSERT INTO {STAGING_TABLES['payments']}
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
        VALUES
            (%(order_id)s, %(payment_sequential)s, %(payment_type)s,
             %(payment_installments)s, %(payment_value)s)
        ON CONFLICT (order_id, payment_sequential) DO NOTHING
    """,
    "ecommerce_reviews": f"""
        INSERT INTO {STAGING_TABLES['reviews']}
            (review_id, order_id, review_score, review_comment_title,
             review_comment_message, review_creation_date, review_answer_timestamp)
        VALUES
            (%(review_id)s, %(order_id)s, %(review_score)s, %(review_comment_title)s,
             %(review_comment_message)s, %(review_creation_date)s, %(review_answer_timestamp)s)
        ON CONFLICT (review_id, order_id) DO NOTHING
    """,
    "ecommerce_products": f"""
        INSERT INTO {STAGING_TABLES['products']}
            (product_id, product_category_name, product_name_lenght,
             product_description_lenght, product_photos_qty,
             product_weight_g, product_length_cm, product_height_cm, product_width_cm)
        VALUES
            (%(product_id)s, %(product_category_name)s, %(product_name_lenght)s,
             %(product_description_lenght)s, %(product_photos_qty)s,
             %(product_weight_g)s, %(product_length_cm)s, %(product_height_cm)s, %(product_width_cm)s)
        ON CONFLICT (product_id) DO NOTHING
    """,
    "ecommerce_sellers": f"""
        INSERT INTO {STAGING_TABLES['sellers']}
            (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        VALUES
            (%(seller_id)s, %(seller_zip_code_prefix)s, %(seller_city)s, %(seller_state)s)
        ON CONFLICT (seller_id) DO NOTHING
    """,
    "ecommerce_geolocation": f"""
        INSERT INTO {STAGING_TABLES['geolocation']}
            (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
             geolocation_city, geolocation_state)
        VALUES
            (%(geolocation_zip_code_prefix)s, %(geolocation_lat)s, %(geolocation_lng)s,
             %(geolocation_city)s, %(geolocation_state)s)
    """,
    "ecommerce_category_trans": f"""
        INSERT INTO {STAGING_TABLES['category_translation']}
            (product_category_name, product_category_name_english)
        VALUES
            (%(product_category_name)s, %(product_category_name_english)s)
        ON CONFLICT (product_category_name) DO NOTHING
    """,
}


def run_consumer():
    conn = psycopg2.connect(**DB_CONN_PARAMS)
    cursor = conn.cursor()

    print("Membuat / memverifikasi 9 tabel staging di PostgreSQL...")
    for sql in CREATE_TABLES:
        cursor.execute(sql)
    # Pastikan kolom unstructured review_comment_title & review_comment_message tersedia
    cursor.execute(
        f"ALTER TABLE {STAGING_TABLES['reviews']} ADD COLUMN IF NOT EXISTS review_comment_title TEXT;"
    )
    cursor.execute(
        f"ALTER TABLE {STAGING_TABLES['reviews']} ADD COLUMN IF NOT EXISTS review_comment_message TEXT;"
    )
    conn.commit()
    print("✓ Semua tabel staging siap.\n")

    topics = list(INSERT_QUERIES.keys())
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30_000,
    )

    print("Menunggu event dari Apache Kafka untuk disimpan ke PostgreSQL Staging...")
    print(f"Topik dimonitor: {', '.join(topics)}\n")

    batch_size = 1_000
    count = 0
    topic_counts = {t: 0 for t in topics}
    buffers = {t: [] for t in topics}

    def flush_buffers():
        for t, records in buffers.items():
            if records:
                execute_batch(cursor, INSERT_QUERIES[t], records, page_size=500)
                records.clear()
        conn.commit()
        consumer.commit()

    try:
        for message in consumer:
            topic = message.topic
            buffers[topic].append(message.value)
            count += 1
            topic_counts[topic] += 1

            if count % batch_size == 0:
                try:
                    flush_buffers()
                    print(f"  {count:>10,} pesan disimpan  |  Topik terakhir: {topic}")
                except Exception as e:
                    conn.rollback()
                    print(f"  ✗ Error saat batch commit pada topik '{topic}': {e}")
                    raise

        flush_buffers()
    finally:
        consumer.close()
        cursor.close()
        conn.close()

    print("\n" + "=" * 65)
    print(f"  Ingestion selesai — Total {count:,} pesan tersimpan di Staging.")
    print("=" * 65)
    for t, c in topic_counts.items():
        print(f"  {t:<35} {c:>10,} baris")


if __name__ == "__main__":
    run_consumer()
