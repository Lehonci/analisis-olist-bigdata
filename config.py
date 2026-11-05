import os
import sys

# Pastikan output terminal di Windows mendukung karakter UTF-8 tanpa UnicodeEncodeError
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Pastikan HADOOP_HOME terkonfigurasi otomatis pada Windows jika C:\hadoop tersedia
if os.name == "nt" and not os.environ.get("HADOOP_HOME") and os.path.isdir(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# ─────────────────────────────────────────────────────────
# Konfigurasi PostgreSQL (Staging & Analytics Mart)
# ─────────────────────────────────────────────────────────
DB_HOST = os.getenv("OLIST_DB_HOST", "localhost")
DB_PORT = os.getenv("OLIST_DB_PORT", "5432")
DB_NAME = os.getenv("OLIST_DB_NAME", "olist_db")
DB_USER = os.getenv("OLIST_DB_USER", "postgres")
DB_PASSWORD = os.getenv("OLIST_DB_PASSWORD", "postgres")

DB_CONN_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
}

SPARK_JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
SPARK_JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────
# Konfigurasi Apache Kafka & Sumber Data CSV
# ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "olist-consumer-group")
DATA_DIR = os.getenv("OLIST_DATA_DIR", "data")

DATASETS = {
    "ecommerce_customers": f"{DATA_DIR}/olist_customers_dataset.csv",
    "ecommerce_orders": f"{DATA_DIR}/olist_orders_dataset.csv",
    "ecommerce_order_items": f"{DATA_DIR}/olist_order_items_dataset.csv",
    "ecommerce_payments": f"{DATA_DIR}/olist_order_payments_dataset.csv",
    "ecommerce_reviews": f"{DATA_DIR}/olist_order_reviews_dataset.csv",
    "ecommerce_products": f"{DATA_DIR}/olist_products_dataset.csv",
    "ecommerce_sellers": f"{DATA_DIR}/olist_sellers_dataset.csv",
    "ecommerce_geolocation": f"{DATA_DIR}/olist_geolocation_dataset.csv",
    "ecommerce_category_trans": f"{DATA_DIR}/product_category_name_translation.csv",
}

# ─────────────────────────────────────────────────────────
# Daftar Tabel Staging (Raw Data dari Kafka)
# ─────────────────────────────────────────────────────────
STAGING_TABLES = {
    "customers": "staging_customers",
    "orders": "staging_orders",
    "order_items": "staging_order_items",
    "payments": "staging_payments",
    "reviews": "staging_reviews",
    "products": "staging_products",
    "sellers": "staging_sellers",
    "geolocation": "staging_geolocation",
    "category_translation": "staging_category_translation",
}

# ─────────────────────────────────────────────────────────
# Daftar Tabel Analytics Mart (Hasil Olahan Apache Spark)
# ─────────────────────────────────────────────────────────
ANALYTICS_TABLES = {
    "kpi_summary": "analytics_kpi_summary",
    "payment_dist": "analytics_payment_dist",
    "order_status": "analytics_order_status",
    "monthly_trend": "analytics_monthly_trend",
    "top_cities": "analytics_top_cities",
    "top_categories": "analytics_top_categories",
    "kmeans_eval": "analytics_kmeans_eval",
    "state_clusters": "analytics_state_clusters",
    "cluster_summary": "analytics_cluster_summary",
    "cluster_products": "analytics_cluster_products",
    "density_comparison": "analytics_density_comparison",
}
