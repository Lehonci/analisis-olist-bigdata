import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd

# Inisiasi Apache Spark
# ─────────────────────────────────────────────────────────
# 1. Inisiasi Apache Spark
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Olist_BigData_EDA") \
    .appName("Olist_BigData_EDA_Clustering") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Konfigurasi database
spark.sparkContext.setLogLevel("ERROR")

# Konfigurasi database PostgreSQL
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
db_url = "jdbc:postgresql://localhost:5432/olist_db"

# Memuat data dari PostgreSQL (Batch Processing)
df_customers = spark.read.jdbc(url=db_url, table="staging_customers", properties=db_properties)
df_orders = spark.read.jdbc(url=db_url, table="staging_orders", properties=db_properties)
df_payments = spark.read.jdbc(url=db_url, table="staging_payments", properties=db_properties)
print("=" * 70)
print("  Memuat Data dari PostgreSQL Staging...")
print("=" * 70)

# Mendaftarkan DataFrame sebagai Temporary SQL Tables untuk dianalisis dengan Spark SQL
df_customers.createOrReplaceTempView("customers")
df_orders.createOrReplaceTempView("orders")
df_payments.createOrReplaceTempView("payments")
# Memuat tabel-tabel staging dari PostgreSQL
tables = [
    "staging_customers", "staging_orders", "staging_order_items",
    "staging_payments", "staging_reviews", "staging_products",
    "staging_sellers", "staging_category_translation"
]

# ---------------------------------------------------------
# EDA menggunakan Spark SQL (Sesuai Tujuan Analisis Laporan)
# ---------------------------------------------------------
for tbl in tables:
    try:
        df = spark.read.jdbc(url=db_url, table=tbl, properties=db_properties)
        view_name = tbl.replace("staging_", "")
        df.createOrReplaceTempView(view_name)
        print(f"✓ Tabel {tbl:<30} berhasil dimuat sebagai view '{view_name}'")
    except Exception as e:
        print(f"⚠ Gagal memuat tabel {tbl}: {e}")

print("1. Sebaran Pelanggan Berdasarkan Lokasi (Kota)")
# ─────────────────────────────────────────────────────────
# 2. Exploratory Data Analysis (EDA) menggunakan Spark SQL
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  2. HASIL EXPLORATORY DATA ANALYSIS (EDA)")
print("=" * 70)

print("\n--- 1. Top 10 Kota Berdasarkan Jumlah Pelanggan ---")
spark.sql("""
    SELECT customer_city, customer_state, COUNT(customer_id) AS total_customers
    FROM customers
    GROUP BY customer_city, customer_state
    ORDER BY total_customers DESC
    LIMIT 10
""").show(truncate=False)

print("2. Total Volume dan Nilai Transaksi per Wilayah")
print("\n--- 2. Top 10 Wilayah/State Berdasarkan Volume dan Nilai Transaksi ---")
spark.sql("""
    SELECT c.customer_state,
           COUNT(DISTINCT o.order_id) AS total_orders,
           ROUND(SUM(p.payment_value), 2) AS total_revenue,
           ROUND(AVG(p.payment_value), 2) AS avg_order_value
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_state
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

print("\n--- 3. Top 10 Kategori Produk Paling Populer (Bilingual) ---")
spark.sql("""
    SELECT COALESCE(ct.product_category_name_english, pr.product_category_name, 'Other') AS category_name,
           COUNT(oi.order_item_id) AS total_items_sold,
           ROUND(SUM(oi.price), 2) AS total_sales
    FROM order_items oi
    JOIN products pr ON oi.product_id = pr.product_id
    LEFT JOIN category_translation ct ON pr.product_category_name = ct.product_category_name
    GROUP BY 1
    ORDER BY total_items_sold DESC
    LIMIT 10
""").show(truncate=False)

print("\n--- 4. Distribusi Metode Pembayaran ---")
spark.sql("""
    SELECT payment_type,
           COUNT(order_id) AS total_transactions,
           ROUND(SUM(payment_value), 2) AS total_value,
           ROUND(AVG(payment_value), 2) AS avg_value
    FROM payments
    GROUP BY payment_type
    ORDER BY total_transactions DESC
""").show(truncate=False)

# ─────────────────────────────────────────────────────────
# 3. Feature Engineering untuk Clustering Wilayah (State Level)
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  3. FEATURE ENGINEERING UNTUK K-MEANS CLUSTERING")
print("=" * 70)

# Agregasi metrik fitur per customer_state
df_state_features = spark.sql("""
    SELECT 
        c.customer_state,
        CAST(COUNT(DISTINCT c.customer_id) AS DOUBLE) AS total_customers,
        CAST(COUNT(DISTINCT o.order_id) AS DOUBLE) AS total_orders,
        CAST(COALESCE(SUM(p.payment_value), 0.0) AS DOUBLE) AS total_revenue,
        CAST(COALESCE(AVG(p.payment_value), 0.0) AS DOUBLE) AS avg_order_value,
        CAST(COALESCE(COUNT(oi.order_item_id), 0) AS DOUBLE) AS total_items,
        CAST(COALESCE(AVG(oi.freight_value), 0.0) AS DOUBLE) AS avg_freight_value,
        CAST(COALESCE(AVG(r.review_score), 0.0) AS DOUBLE) AS avg_review_score
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'delivered'
    LEFT JOIN payments p ON o.order_id = p.order_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN reviews r ON o.order_id = r.order_id
    GROUP BY c.customer_state
""")

df_state_features = df_state_features.na.fill(0.0)
df_state_features.show(10, truncate=False)

feature_cols = [
    "total_customers", "total_orders", "total_revenue",
    "avg_order_value", "total_items", "avg_freight_value", "avg_review_score"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
df_assembled = assembler.transform(df_state_features)

scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

# ─────────────────────────────────────────────────────────
# 4. Pencarian Nilai K Optimal: Elbow Method & Silhouette Score
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  4. EVALUASI NILAI K (ELBOW METHOD & SILHOUETTE SCORE)")
print("=" * 70)

evaluator = ClusteringEvaluator(
    featuresCol='features',
    metricName='silhouette',
    distanceMeasure='squaredEuclidean'
)

k_range = list(range(2, 9))
elbow_scores = []
silhouette_scores = []

print(f"{'K':<5} | {'WSSSE (Inertia/Cost)':<22} | {'Silhouette Score':<18}")
print("-" * 52)

best_k = 2
best_silhouette = -1.0

for k in k_range:
    kmeans_eval = KMeans(featuresCol="features", k=k, seed=42, maxIter=50)
    model_eval = kmeans_eval.fit(df_scaled)
    predictions_eval = model_eval.transform(df_scaled)
    
    # Elbow Cost (WSSSE)
    cost = model_eval.summary.trainingCost
    elbow_scores.append(cost)
    
    # Silhouette Score
    sil_score = evaluator.evaluate(predictions_eval)
    silhouette_scores.append(sil_score)
    
    print(f"{k:<5} | {cost:<22.4f} | {sil_score:<18.4f}")
    
    if sil_score > best_silhouette:
        best_silhouette = sil_score
        best_k = k

print("-" * 52)
print(f"★ Nilai K Optimal terpilih berdasarkan Silhouette Score tertinggi: K = {best_k} (Silhouette = {best_silhouette:.4f})")

# Simpan hasil metrik evaluasi K ke PostgreSQL
df_eval_metrics = spark.createDataFrame(
    pd.DataFrame({
        "k": k_range,
        "wssse_cost": elbow_scores,
        "silhouette_score": silhouette_scores
    })
)

df_eval_metrics.write.mode("overwrite").jdbc(
    url=db_url,
    table="clustering_eval_metrics",
    properties=db_properties
)
print("✓ Hasil evaluasi Elbow & Silhouette disimpan ke tabel 'clustering_eval_metrics'")

# ─────────────────────────────────────────────────────────
# 5. Pelatihan Model Akhir dengan K Optimal
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print(f"  5. PELATIHAN MODEL K-MEANS DENGAN K = {best_k}")
print("=" * 70)

kmeans_final = KMeans(featuresCol="features", predictionCol="cluster", k=best_k, seed=42, maxIter=100)
model_final = kmeans_final.fit(df_scaled)
clustered_df = model_final.transform(df_scaled)

# Drop kolom vector sebelum diekspor ke PostgreSQL agar kompatibel
export_cols = [
    "customer_state", "cluster",
    "total_customers", "total_orders", "total_revenue",
    "avg_order_value", "total_items", "avg_freight_value", "avg_review_score"
]
df_results_export = clustered_df.select(export_cols)

print("\n--- Profil Cluster Setiap State ---")
df_results_export.orderBy("cluster", "total_revenue", ascending=[True, False]).show(30, truncate=False)

# Simpan hasil cluster ke PostgreSQL
df_results_export.write.mode("overwrite").jdbc(
    url=db_url,
    table="clustering_state_results",
    properties=db_properties
)
print("✓ Hasil cluster wilayah disimpan ke tabel 'clustering_state_results'")

# ─────────────────────────────────────────────────────────
# 6. Analisis Profil Karakteristik Tiap Cluster & Kategori Produk
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  6. PROFIL RINGKASAN KARAKTERISTIK TIAP CLUSTER")
print("=" * 70)

df_results_export.createOrReplaceTempView("cluster_state_view")

spark.sql("""
    SELECT 
        cluster,
        COUNT(customer_state) AS total_states,
        ROUND(AVG(total_customers), 0) AS avg_customers_per_state,
        ROUND(AVG(total_orders), 0) AS avg_orders_per_state,
        ROUND(AVG(total_revenue), 2) AS avg_revenue_per_state,
        ROUND(AVG(avg_order_value), 2) AS avg_order_ticket_size,
        ROUND(AVG(avg_freight_value), 2) AS avg_freight,
        ROUND(AVG(avg_review_score), 2) AS avg_satisfaction
    FROM cluster_state_view
    GROUP BY cluster
    ORDER BY cluster
""").show(truncate=False)

print("\n--- Kategori Produk Paling Dominan per Cluster Wilayah ---")
spark.sql("""
    WITH state_prod AS (
        SELECT 
            cs.cluster,
            COALESCE(ct.product_category_name_english, pr.product_category_name, 'Other') AS category_name,
            COUNT(oi.order_item_id) AS total_sold,
            ROUND(SUM(oi.price), 2) AS total_revenue,
            ROW_NUMBER() OVER (PARTITION BY cs.cluster ORDER BY COUNT(oi.order_item_id) DESC) as rank
        FROM cluster_state_view cs
        JOIN customers c ON cs.customer_state = c.customer_state
        JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'delivered'
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products pr ON oi.product_id = pr.product_id
        LEFT JOIN category_translation ct ON pr.product_category_name = ct.product_category_name
        GROUP BY cs.cluster, 2
    )
    SELECT cluster, rank, category_name, total_sold, total_revenue
    FROM state_prod
    WHERE rank <= 3
    ORDER BY cluster, rank
""").show(truncate=False)

# Simpan analisis produk per cluster ke PostgreSQL
df_prod_per_cluster = spark.sql("""
    WITH state_prod AS (
        SELECT 
            cs.cluster,
            COALESCE(ct.product_category_name_english, pr.product_category_name, 'Other') AS category_name,
            COUNT(oi.order_item_id) AS total_sold,
            ROUND(SUM(oi.price), 2) AS total_revenue,
            ROW_NUMBER() OVER (PARTITION BY cs.cluster ORDER BY COUNT(oi.order_item_id) DESC) as rank
        FROM cluster_state_view cs
        JOIN customers c ON cs.customer_state = c.customer_state
        JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'delivered'
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products pr ON oi.product_id = pr.product_id
        LEFT JOIN category_translation ct ON pr.product_category_name = ct.product_category_name
        GROUP BY cs.cluster, 2
    )
    SELECT cluster, rank, category_name, total_sold, total_revenue
    FROM state_prod
    WHERE rank <= 5
""")

df_prod_per_cluster.write.mode("overwrite").jdbc(
    url=db_url,
    table="clustering_top_products",
    properties=db_properties
)
print("✓ Hasil kategori produk per cluster disimpan ke tabel 'clustering_top_products'")

print("\n" + "=" * 70)
print("  Proses EDA & K-Means Clustering Selesai Sukses!")
print("=" * 70)
spark.stop()