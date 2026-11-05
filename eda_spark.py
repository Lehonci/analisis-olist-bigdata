import os
import math
from config import (
    SPARK_JDBC_URL,
    SPARK_JDBC_PROPERTIES,
    STAGING_TABLES,
    ANALYTICS_TABLES,
)

import findspark

findspark.init()

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


def write_jdbc(df, table_name):
    """Menyimpan Spark DataFrame ke PostgreSQL dengan mode overwrite."""
    df.write.mode("overwrite").jdbc(
        url=SPARK_JDBC_URL,
        table=table_name,
        properties=SPARK_JDBC_PROPERTIES,
    )


def build_spark_session():
    builder = (
        SparkSession.builder.appName("Olist_BigData_EDA_Clustering")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
    )
    ivy_jar = os.path.expanduser(
        r"~\.ivy2.5.2\jars\org.postgresql_postgresql-42.2.18.jar"
    )
    if os.path.isfile(ivy_jar):
        builder = builder.config("spark.driver.extraClassPath", ivy_jar)
    else:
        builder = builder.config(
            "spark.jars.packages", "org.postgresql:postgresql:42.2.18"
        )
    return builder.getOrCreate()


def main():
    # ─────────────────────────────────────────────────────────
    # 1. Inisiasi Apache Spark
    # ─────────────────────────────────────────────────────────
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 75)
    print("  1. MEMUAT DATA DARI POSTGRESQL STAGING LAYER KE APACHE SPARK")
    print("=" * 75)

    for view_name, table_name in STAGING_TABLES.items():
        df = spark.read.jdbc(
            url=SPARK_JDBC_URL,
            table=table_name,
            properties=SPARK_JDBC_PROPERTIES,
        )
        df.createOrReplaceTempView(view_name)
        print(f"✓ Tabel {table_name:<32} terdaftar sebagai Spark View '{view_name}'")

    # ─────────────────────────────────────────────────────────
    # 2. Eksplorasi Data (EDA) & Agregasi KPI via Spark SQL
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  2. EXPLORATORY DATA ANALYSIS (EDA) MENGGUNAKAN SPARK SQL")
    print("=" * 75)

    # 2a. Ringkasan KPI Utama Sistem
    df_kpi_summary = spark.sql("""
        WITH order_pay AS (
            SELECT order_id, SUM(payment_value) AS order_payment_value
            FROM payments
            GROUP BY order_id
        ),
        order_itm AS (
            SELECT order_id, AVG(freight_value) AS avg_order_freight
            FROM order_items
            GROUP BY order_id
        ),
        order_rev AS (
            SELECT order_id, AVG(CAST(review_score AS DOUBLE)) AS avg_order_review
            FROM reviews
            GROUP BY order_id
        )
        SELECT
            CAST(COUNT(DISTINCT c.customer_unique_id) AS LONG) AS total_unique_customers,
            CAST(COUNT(DISTINCT o.order_id) AS LONG) AS total_orders,
            CAST(COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) AS LONG) AS total_delivered_orders,
            ROUND(SUM(CASE WHEN o.order_status = 'delivered' THEN op.order_payment_value ELSE 0.0 END), 2) AS total_revenue,
            ROUND(AVG(CASE WHEN o.order_status = 'delivered' THEN op.order_payment_value END), 2) AS avg_order_value,
            ROUND(AVG(CASE WHEN o.order_status = 'delivered' THEN oi.avg_order_freight END), 2) AS avg_freight_value,
            ROUND(AVG(orv.avg_order_review), 2) AS avg_review_score,
            CAST(COUNT(DISTINCT c.customer_state) AS INT) AS total_states,
            CAST(COUNT(DISTINCT c.customer_city) AS INT) AS total_cities,
            (SELECT CAST(COUNT(DISTINCT seller_id) AS INT) FROM sellers) AS total_sellers,
            (SELECT CAST(COUNT(DISTINCT product_id) AS INT) FROM products) AS total_products
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_pay op ON o.order_id = op.order_id
        LEFT JOIN order_itm oi ON o.order_id = oi.order_id
        LEFT JOIN order_rev orv ON o.order_id = orv.order_id
    """)
    write_jdbc(df_kpi_summary, ANALYTICS_TABLES["kpi_summary"])
    print("\n--- Ringkasan KPI Nasional ---")
    df_kpi_summary.show(truncate=False)

    # 2b. RQ1: Top Kota Berdasarkan Sebaran Pelanggan Unik & Volume Transaksi
    df_top_cities = spark.sql("""
        WITH order_pay AS (
            SELECT order_id, SUM(payment_value) AS order_payment_value
            FROM payments
            GROUP BY order_id
        ),
        order_itm AS (
            SELECT order_id, AVG(freight_value) AS avg_item_freight
            FROM order_items
            GROUP BY order_id
        )
        SELECT
            c.customer_city,
            c.customer_state,
            COUNT(DISTINCT c.customer_unique_id) AS total_unique_customers,
            COUNT(DISTINCT c.customer_id) AS total_customer_records,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(COALESCE(SUM(op.order_payment_value), 0.0), 2) AS total_revenue,
            ROUND(COALESCE(AVG(op.order_payment_value), 0.0), 2) AS avg_order_value,
            ROUND(COALESCE(AVG(oi.avg_item_freight), 0.0), 2) AS avg_freight_value
        FROM customers c
        LEFT JOIN orders o
            ON c.customer_id = o.customer_id
            AND o.order_status = 'delivered'
        LEFT JOIN order_pay op ON o.order_id = op.order_id
        LEFT JOIN order_itm oi ON o.order_id = oi.order_id
        GROUP BY c.customer_city, c.customer_state
        ORDER BY total_unique_customers DESC
        LIMIT 25
    """)
    write_jdbc(df_top_cities, ANALYTICS_TABLES["top_cities"])
    print("\n--- [RQ1] Top 10 Kota Berdasarkan Pelanggan Unik & Nilai Transaksi ---")
    df_top_cities.limit(10).show(truncate=False)

    # 2c. Distribusi Metode Pembayaran
    df_payment_dist = spark.sql("""
        WITH total_pay AS (
            SELECT SUM(payment_value) AS grand_total
            FROM payments
            WHERE payment_type IS NOT NULL AND payment_type <> 'not_defined'
        )
        SELECT
            p.payment_type,
            COUNT(p.order_id) AS total_transactions,
            COUNT(DISTINCT p.order_id) AS total_orders,
            ROUND(SUM(p.payment_value), 2) AS total_value,
            ROUND(AVG(p.payment_value), 2) AS avg_value,
            ROUND(SUM(p.payment_value) * 100.0 / MAX(tp.grand_total), 2) AS pct_value
        FROM payments p
        CROSS JOIN total_pay tp
        WHERE p.payment_type IS NOT NULL AND p.payment_type <> 'not_defined'
        GROUP BY p.payment_type
        ORDER BY total_value DESC
    """)
    write_jdbc(df_payment_dist, ANALYTICS_TABLES["payment_dist"])
    print("\n--- Distribusi Metode Pembayaran ---")
    df_payment_dist.show(truncate=False)

    # 2d. Distribusi Status Pesanan
    df_order_status = spark.sql("""
        WITH total_ord AS (
            SELECT COUNT(order_id) AS all_orders FROM orders
        )
        SELECT
            o.order_status,
            COUNT(o.order_id) AS total_orders,
            ROUND(COUNT(o.order_id) * 100.0 / MAX(t.all_orders), 2) AS pct_orders
        FROM orders o
        CROSS JOIN total_ord t
        GROUP BY o.order_status
        ORDER BY total_orders DESC
    """)
    write_jdbc(df_order_status, ANALYTICS_TABLES["order_status"])

    # 2e. Tren Pembelian Bulanan
    df_monthly_trend = spark.sql("""
        WITH order_pay AS (
            SELECT order_id, SUM(payment_value) AS order_payment_value
            FROM payments
            GROUP BY order_id
        )
        SELECT
            SUBSTRING(o.order_purchase_timestamp, 1, 7) AS order_month,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
            ROUND(COALESCE(SUM(op.order_payment_value), 0.0), 2) AS total_revenue,
            ROUND(COALESCE(AVG(op.order_payment_value), 0.0), 2) AS avg_order_value
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN order_pay op ON o.order_id = op.order_id
        WHERE o.order_status = 'delivered'
          AND o.order_purchase_timestamp IS NOT NULL
          AND SUBSTRING(o.order_purchase_timestamp, 1, 7) >= '2017-01'
          AND SUBSTRING(o.order_purchase_timestamp, 1, 7) <= '2018-08'
        GROUP BY SUBSTRING(o.order_purchase_timestamp, 1, 7)
        ORDER BY order_month
    """)
    write_jdbc(df_monthly_trend, ANALYTICS_TABLES["monthly_trend"])

    # 2f. Kategori Produk Paling Populer Secara Nasional
    df_top_categories = spark.sql("""
        SELECT
            COALESCE(ct.product_category_name_english, pr.product_category_name, 'unclassified') AS category_name,
            COUNT(oi.order_item_id) AS total_items_sold,
            COUNT(DISTINCT oi.order_id) AS total_orders,
            ROUND(SUM(oi.price), 2) AS total_sales,
            ROUND(AVG(oi.price), 2) AS avg_price,
            ROUND(AVG(oi.freight_value), 2) AS avg_freight
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id AND o.order_status = 'delivered'
        JOIN products pr ON oi.product_id = pr.product_id
        LEFT JOIN category_translation ct ON pr.product_category_name = ct.product_category_name
        GROUP BY COALESCE(ct.product_category_name_english, pr.product_category_name, 'unclassified')
        ORDER BY total_items_sold DESC
    """)
    write_jdbc(df_top_categories, ANALYTICS_TABLES["top_categories"])
    print("\n--- Top 10 Kategori Produk Nasional ---")
    df_top_categories.limit(10).show(truncate=False)

    # ─────────────────────────────────────────────────────────
    # 3. Feature Engineering 7 Fitur Wilayah (Bebas Join Fan-Out)
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  3. EKSTRAKSI 7 FITUR AGREGAT WILAYAH (NEGARA BAGIAN) & GEOLOCATION")
    print("=" * 75)

    # CTE mengagregasi payments, order_items, dan reviews per order_id terlebih dahulu
    # agar tidak terjadi duplikasi baris (join fan-out) pada pesanan multi-item/multi-cicilan.
    df_state_features = spark.sql("""
        WITH order_pay AS (
            SELECT
                order_id,
                SUM(payment_value) AS order_payment_value
            FROM payments
            GROUP BY order_id
        ),
        order_itm AS (
            SELECT
                order_id,
                COUNT(order_item_id) AS item_count,
                AVG(freight_value) AS avg_item_freight,
                SUM(freight_value) AS total_order_freight,
                SUM(price) AS total_order_price
            FROM order_items
            GROUP BY order_id
        ),
        order_rev AS (
            SELECT
                order_id,
                AVG(CAST(review_score AS DOUBLE)) AS avg_order_review
            FROM reviews
            GROUP BY order_id
        ),
        state_geo AS (
            SELECT
                geolocation_state AS state_code,
                ROUND(AVG(geolocation_lat), 4) AS lat,
                ROUND(AVG(geolocation_lng), 4) AS lng
            FROM geolocation
            WHERE geolocation_lat BETWEEN -35.0 AND 6.0
              AND geolocation_lng BETWEEN -75.0 AND -30.0
            GROUP BY geolocation_state
        )
        SELECT
            c.customer_state,
            CAST(COUNT(DISTINCT c.customer_unique_id) AS DOUBLE) AS total_customers,
            CAST(COUNT(DISTINCT o.order_id) AS DOUBLE) AS total_orders,
            CAST(COALESCE(SUM(op.order_payment_value), 0.0) AS DOUBLE) AS total_revenue,
            CAST(COALESCE(AVG(op.order_payment_value), 0.0) AS DOUBLE) AS avg_order_value,
            CAST(COALESCE(SUM(oi.item_count), 0.0) AS DOUBLE) AS total_items,
            CAST(COALESCE(AVG(oi.avg_item_freight), 0.0) AS DOUBLE) AS avg_freight_value,
            CAST(COALESCE(AVG(orv.avg_order_review), 0.0) AS DOUBLE) AS avg_review_score,
            CAST(COALESCE(AVG(oi.item_count), 0.0) AS DOUBLE) AS avg_items_per_order,
            CAST(COALESCE(SUM(oi.total_order_freight) * 100.0 / NULLIF(SUM(oi.total_order_price), 0.0), 0.0) AS DOUBLE) AS freight_to_price_pct,
            CAST(COALESCE(AVG(DATEDIFF(TO_TIMESTAMP(o.order_delivered_customer_date), TO_TIMESTAMP(o.order_purchase_timestamp))), 0.0) AS DOUBLE) AS avg_delivery_days,
            COALESCE(g.lat, -14.2350) AS lat,
            COALESCE(g.lng, -51.9253) AS lng
        FROM customers c
        LEFT JOIN orders o
            ON c.customer_id = o.customer_id
            AND o.order_status = 'delivered'
        LEFT JOIN order_pay op ON o.order_id = op.order_id
        LEFT JOIN order_itm oi ON o.order_id = oi.order_id
        LEFT JOIN order_rev orv ON o.order_id = orv.order_id
        LEFT JOIN state_geo g ON c.customer_state = g.state_code
        GROUP BY c.customer_state, g.lat, g.lng
        ORDER BY total_revenue DESC
    """).na.fill(0.0).coalesce(1).cache()

    print("\n--- [RQ1] 10 Negara Bagian dengan Pendapatan & Pelanggan Tertinggi ---")
    df_state_features.orderBy(F.col("total_revenue").desc()).select(
        "customer_state",
        "total_customers",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "total_items",
        "avg_freight_value",
        "avg_review_score",
    ).show(10, truncate=False)

    # Standarisasi Z-Score (Mean = 0, Std = 1) menggunakan PySpark StandardScaler
    feature_cols = [
        "total_customers",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "total_items",
        "avg_freight_value",
        "avg_review_score",
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    df_assembled = assembler.transform(df_state_features)

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled).cache()

    # ─────────────────────────────────────────────────────────
    # 4. Evaluasi Nilai K (Elbow Method / Kneedle & Silhouette Score)
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  4. [RQ2] EVALUASI NILAI K (ELBOW METHOD & SILHOUETTE SCORE, K=2..8)")
    print("=" * 75)

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    k_range = list(range(2, 9))
    eval_records = []

    for k in k_range:
        km = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=1, maxIter=100)
        model = km.fit(df_scaled)
        preds = model.transform(df_scaled)

        wssse = float(model.summary.trainingCost)
        sil = float(evaluator.evaluate(preds))

        # Hitung berapa klaster yang bukan singleton (berisi > 1 negara bagian)
        cluster_sizes = [row["count"] for row in preds.groupBy("prediction").count().collect()]
        non_singleton_clusters = sum(1 for c in cluster_sizes if c > 1)

        eval_records.append({
            "k": k,
            "wssse_cost": wssse,
            "silhouette_score": sil,
            "non_singleton_clusters": non_singleton_clusters,
        })

    # Hitung metrik matematis titik siku (Kneedle perpendicular distance & penurunan WSSSE)
    k_min, k_max = k_range[0], k_range[-1]
    w_min, w_max = eval_records[-1]["wssse_cost"], eval_records[0]["wssse_cost"]

    for i, rec in enumerate(eval_records):
        # Persentase penurunan WSSSE dibanding K sebelumnya
        if i == 0:
            rec["wssse_drop_pct"] = 0.0
        else:
            prev_w = eval_records[i - 1]["wssse_cost"]
            rec["wssse_drop_pct"] = round((prev_w - rec["wssse_cost"]) * 100.0 / prev_w, 2)

        # Jarak tegak lurus terstandarisasi ke garis lurus penghubung (K=2, WSSSE_2) dan (K=8, WSSSE_8)
        x_norm = (rec["k"] - k_min) / (k_max - k_min)
        y_norm = (rec["wssse_cost"] - w_min) / (w_max - w_min) if w_max > w_min else 0.0
        # Garis sekant dari (0, 1) ke (1, 0) adalah x + y - 1 = 0
        elbow_dist = abs(1.0 - x_norm - y_norm) / math.sqrt(2.0)
        rec["elbow_distance"] = round(elbow_dist, 4)
        rec["wssse_cost"] = round(rec["wssse_cost"], 4)
        rec["silhouette_score"] = round(rec["silhouette_score"], 4)

    # Penentuan K Optimal secara algoritmik (kombinasi titik siku Elbow tertinggi & Silhouette
    # pada partisi yang menghasilkan minimal 2 segmen pasar non-outlier):
    # Pada K=2, model hanya memisahkan outlier tunggal (São Paulo, n=1) dari 26 negara bagian lain
    # (non_singleton_clusters == 1), sehingga belum mengelompokkan wilayah di luar SP.
    valid_candidates = [r for r in eval_records if r["non_singleton_clusters"] >= 2]
    best_record = max(
        valid_candidates,
        key=lambda r: (r["elbow_distance"] + r["silhouette_score"]),
    )
    optimal_k = int(best_record["k"])

    for rec in eval_records:
        rec["is_optimal"] = rec["k"] == optimal_k

    print(
        f"{'K':<4} | {'WSSSE (Cost)':<14} | {'Drop (%)':<10} | "
        f"{'Elbow Knee':<12} | {'Silhouette':<12} | {'Status'}"
    )
    print("-" * 75)
    for rec in eval_records:
        marker = "★ OPTIMAL (Elbow + Silhouette)" if rec["is_optimal"] else ""
        print(
            f"{rec['k']:<4} | {rec['wssse_cost']:<14.4f} | {rec['wssse_drop_pct']:<10.2f} | "
            f"{rec['elbow_distance']:<12.4f} | {rec['silhouette_score']:<12.4f} | {marker}"
        )
    print("-" * 75)
    print(
        f"✓ Nilai K Optimal terpilih: K = {optimal_k} "
        f"(WSSSE Drop = {best_record['wssse_drop_pct']:.2f}%, "
        f"Elbow Distance = {best_record['elbow_distance']:.4f}, "
        f"Silhouette = {best_record['silhouette_score']:.4f})"
    )

    df_eval_spark = spark.createDataFrame(pd.DataFrame(eval_records))
    write_jdbc(df_eval_spark, ANALYTICS_TABLES["kmeans_eval"])
    write_jdbc(df_eval_spark, "clustering_eval_metrics")

    # ─────────────────────────────────────────────────────────
    # 5. Pelatihan Model K-Means Optimal & Penamaan Klaster Dinamis
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print(f"  5. PELATIHAN K-MEANS (K = {optimal_k}) & PROFIL SEGMENTASI WILAYAH")
    print("=" * 75)

    kmeans_final = KMeans(
        featuresCol="features",
        predictionCol="raw_cluster",
        k=optimal_k,
        seed=1,
        maxIter=100,
    )
    model_final = kmeans_final.fit(df_scaled)
    df_clustered_raw = model_final.transform(df_scaled)
    df_clustered_raw.createOrReplaceTempView("raw_clustered_states")

    # Urutkan nomor ID klaster (0, 1, 2, ...) secara dinamis berdasarkan rata-rata jumlah pelanggan
    # per negara bagian (terbesar -> terkecil) menggunakan Spark SQL Window Function,
    # sehingga urutan ID klaster konsisten dan tidak bergantung pada inisialisasi acak centroid.
    df_cluster_mapping = spark.sql("""
        WITH raw_stats AS (
            SELECT
                raw_cluster,
                AVG(total_customers) AS mean_cust,
                AVG(avg_freight_value) AS mean_freight
            FROM raw_clustered_states
            GROUP BY raw_cluster
        ),
        ranked AS (
            SELECT
                raw_cluster,
                CAST(DENSE_RANK() OVER (ORDER BY mean_cust DESC) - 1 AS INT) AS ordered_cluster,
                COUNT(*) OVER () AS total_k
            FROM raw_stats
        )
        SELECT
            raw_cluster,
            ordered_cluster AS cluster,
            CASE
                WHEN ordered_cluster = 0 THEN 'Pusat Megamarket'
                WHEN ordered_cluster = total_k - 1 THEN 'Wilayah Perifer Logistik Tinggi'
                ELSE 'Pasar Sekunder Berkembang'
            END AS cluster_name,
            CASE
                WHEN ordered_cluster = 0 THEN 'Optimalisasi pengiriman instan/same-day, otomatisasi gudang pusat, dan retensi pelanggan frekuensi tinggi.'
                WHEN ordered_cluster = total_k - 1 THEN 'Subsidi ongkos kirim bersyarat (minimum nilai pesanan), konsolidasi kargo regional, dan kemitraan kurir lokal.'
                ELSE 'Penempatan stok kategori populer di hub distribusi regional (Tier-2) dan promosi bundling untuk menaikkan nilai pesanan.'
            END AS strategy_recommendation
        FROM ranked
    """)
    df_cluster_mapping.createOrReplaceTempView("cluster_mapping")

    df_state_clusters = spark.sql("""
        SELECT
            r.customer_state,
            m.cluster,
            m.cluster_name,
            ROUND(r.total_customers, 0) AS total_customers,
            ROUND(r.total_orders, 0) AS total_orders,
            ROUND(r.total_revenue, 2) AS total_revenue,
            ROUND(r.avg_order_value, 2) AS avg_order_value,
            ROUND(r.total_items, 0) AS total_items,
            ROUND(r.avg_items_per_order, 2) AS avg_items_per_order,
            ROUND(r.avg_freight_value, 2) AS avg_freight_value,
            ROUND(r.freight_to_price_pct, 2) AS freight_to_price_pct,
            ROUND(r.avg_delivery_days, 2) AS avg_delivery_days,
            ROUND(r.avg_review_score, 2) AS avg_review_score,
            r.lat,
            r.lng
        FROM raw_clustered_states r
        JOIN cluster_mapping m ON r.raw_cluster = m.raw_cluster
        ORDER BY m.cluster ASC, r.total_revenue DESC
    """).cache()

    df_state_clusters.createOrReplaceTempView("cluster_state_view")
    write_jdbc(df_state_clusters, ANALYTICS_TABLES["state_clusters"])
    write_jdbc(df_state_clusters, "clustering_state_results")

    print("\n--- [RQ2] Hasil Pengelompokan 27 Negara Bagian Brasil ---")
    df_state_clusters.select(
        "customer_state",
        "cluster",
        "cluster_name",
        "total_customers",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "avg_freight_value",
        "avg_review_score",
    ).show(30, truncate=False)

    # Ringkasan Profil Tiap Klaster (Tabel Utama RQ2)
    df_cluster_summary = spark.sql("""
        SELECT
            cs.cluster,
            cs.cluster_name,
            FIRST(cm.strategy_recommendation) AS strategy_recommendation,
            CAST(COUNT(cs.customer_state) AS INT) AS total_states,
             Concat_ws(', ', COLLECT_LIST(cs.customer_state)) AS states_list,
            ROUND(SUM(cs.total_customers), 0) AS sum_customers,
            ROUND(AVG(cs.total_customers), 1) AS avg_customers_per_state,
            ROUND(SUM(cs.total_orders), 0) AS sum_orders,
            ROUND(AVG(cs.total_orders), 1) AS avg_orders_per_state,
            ROUND(SUM(cs.total_revenue), 2) AS sum_revenue,
            ROUND(AVG(cs.total_revenue), 2) AS avg_revenue_per_state,
            ROUND(AVG(cs.avg_order_value), 2) AS avg_order_ticket_size,
            ROUND(AVG(cs.total_items), 1) AS avg_items_per_state,
            ROUND(AVG(cs.avg_freight_value), 2) AS avg_freight,
            ROUND(AVG(cs.freight_to_price_pct), 2) AS avg_freight_ratio_pct,
            ROUND(AVG(cs.avg_delivery_days), 2) AS avg_delivery_days,
            ROUND(AVG(cs.avg_review_score), 2) AS avg_satisfaction
        FROM cluster_state_view cs
        JOIN cluster_mapping cm ON cs.cluster = cm.cluster
        GROUP BY cs.cluster, cs.cluster_name
        ORDER BY cs.cluster
    """)
    write_jdbc(df_cluster_summary, ANALYTICS_TABLES["cluster_summary"])

    print("\n--- [RQ2] Profil Rata-Rata Karakteristik Tiap Klaster ---")
    df_cluster_summary.select(
        "cluster",
        "cluster_name",
        "total_states",
        "avg_customers_per_state",
        "avg_orders_per_state",
        "avg_revenue_per_state",
        "avg_order_ticket_size",
        "avg_freight",
        "avg_satisfaction",
        "states_list",
    ).show(truncate=False)

    # ─────────────────────────────────────────────────────────
    # 6. RQ3: Kategori Produk Dominan pada Masing-Masing Klaster
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  6. [RQ3] ANALISIS KATEGORI PRODUK DOMINAN PER KLASTER WILAYAH")
    print("=" * 75)

    df_cluster_products = spark.sql("""
        WITH cluster_totals AS (
            SELECT
                cs.cluster,
                COUNT(oi.order_item_id) AS cluster_total_items
            FROM cluster_state_view cs
            JOIN customers c ON cs.customer_state = c.customer_state
            JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'delivered'
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY cs.cluster
        ),
        state_prod AS (
            SELECT
                cs.cluster,
                cs.cluster_name,
                COALESCE(ct.product_category_name_english, pr.product_category_name, 'unclassified') AS category_name,
                COUNT(oi.order_item_id) AS total_sold,
                ROUND(SUM(oi.price), 2) AS total_revenue,
                ROUND(AVG(oi.price), 2) AS avg_price,
                ROUND(AVG(oi.freight_value), 2) AS avg_freight,
                ROW_NUMBER() OVER (
                    PARTITION BY cs.cluster
                    ORDER BY COUNT(oi.order_item_id) DESC
                ) AS rank
            FROM cluster_state_view cs
            JOIN customers c ON cs.customer_state = c.customer_state
            JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'delivered'
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products pr ON oi.product_id = pr.product_id
            LEFT JOIN category_translation ct ON pr.product_category_name = ct.product_category_name
            GROUP BY cs.cluster, cs.cluster_name, COALESCE(ct.product_category_name_english, pr.product_category_name, 'unclassified')
        )
        SELECT
            sp.cluster,
            sp.cluster_name,
            CAST(sp.rank AS INT) AS rank,
            sp.category_name,
            sp.total_sold,
            ROUND(sp.total_sold * 100.0 / ct.cluster_total_items, 2) AS pct_of_cluster_items,
            sp.total_revenue,
            sp.avg_price,
            sp.avg_freight
        FROM state_prod sp
        JOIN cluster_totals ct ON sp.cluster = ct.cluster
        WHERE sp.rank <= 5
        ORDER BY sp.cluster, sp.rank
    """)
    write_jdbc(df_cluster_products, ANALYTICS_TABLES["cluster_products"])
    write_jdbc(df_cluster_products, "clustering_top_products")

    df_cluster_products.show(20, truncate=False)

    # ─────────────────────────────────────────────────────────
    # 7. RQ4: Analisis Perbedaan Pola Konsumsi Kepadatan Tinggi vs Rendah
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  7. [RQ4] PERBANDINGAN POLA KONSUMSI KEPADATAN TINGGI VS RENDAH")
    print("=" * 75)

    df_density_comparison = spark.sql("""
        WITH national_totals AS (
            SELECT
                SUM(total_customers) AS nat_customers,
                SUM(total_orders) AS nat_orders,
                SUM(total_revenue) AS nat_revenue
            FROM cluster_state_view
        )
        SELECT
            cs.cluster,
            CASE
                WHEN cs.cluster = 0 THEN 'Kepadatan Tinggi (Pusat Megamarket)'
                WHEN cs.cluster = 1 THEN 'Kepadatan Menengah (Pasar Sekunder)'
                ELSE 'Kepadatan Rendah (Wilayah Perifer)'
            END AS density_tier,
            CAST(COUNT(cs.customer_state) AS INT) AS total_states,
            ROUND(SUM(cs.total_customers) * 100.0 / MAX(nt.nat_customers), 2) AS share_customers_pct,
            ROUND(SUM(cs.total_orders) * 100.0 / MAX(nt.nat_orders), 2) AS share_orders_pct,
            ROUND(SUM(cs.total_revenue) * 100.0 / MAX(nt.nat_revenue), 2) AS share_revenue_pct,
            ROUND(AVG(cs.total_customers), 1) AS avg_customers_per_state,
            ROUND(AVG(cs.avg_order_value), 2) AS avg_order_value,
            ROUND(AVG(cs.avg_items_per_order), 2) AS avg_items_per_order,
            ROUND(AVG(cs.avg_freight_value), 2) AS avg_freight_value,
            ROUND(AVG(cs.freight_to_price_pct), 2) AS freight_to_price_pct,
            ROUND(AVG(cs.avg_delivery_days), 2) AS avg_delivery_days,
            ROUND(AVG(cs.avg_review_score), 2) AS avg_review_score
        FROM cluster_state_view cs
        CROSS JOIN national_totals nt
        GROUP BY cs.cluster
        ORDER BY cs.cluster
    """)
    write_jdbc(df_density_comparison, ANALYTICS_TABLES["density_comparison"])
    df_density_comparison.show(truncate=False)

    print("\n" + "=" * 75)
    print("  ✓ SELURUH TABEL ANALYTICS MART SPARK BERHASIL DISIMPAN KE POSTGRESQL!")
    print("=" * 75)
    spark.stop()


if __name__ == "__main__":
    main()
