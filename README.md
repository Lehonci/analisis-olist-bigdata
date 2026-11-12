# Proyek Teknologi Big Data: Segmentasi Wilayah Pelanggan E-Commerce Olist (K-Means & Apache Spark)

Implementasi arsitektur sistem Big Data *end-to-end* untuk analisis eksploratif dan pengelompokan wilayah pelanggan (negara bagian) menggunakan dataset **Brazilian E-Commerce Public Dataset by Olist** (~1,55 juta baris, 9 berkas CSV).

---

## 🏗️ Arsitektur & Skema Dua Lapis (*Two-Layer Architecture*)

Sistem ini menerapkan pemisahan tegas antara penyimpanan data mentah (*Staging Layer*) dan hasil pemrosesan analitik terdistribusi (*Spark Analytics Mart Layer*). Seluruh komputasi agregasi, *join*, *feature engineering*, dan *K-Means Clustering* dikerjakan **100% menggunakan Apache Spark (`Spark SQL` & `PySpark MLlib`)**, tanpa kueri pengolahan SQL biasa di lapisan aplikasi dan tanpa data *hardcode*.

```
[ 9 File CSV Olist (data/) ]
             │
             ▼
     [ producer.py ]  ───►  Apache Kafka (9 Topik Event JSON)
                                     │
                                     ▼
                             [ consumer.py ]
                                     │
                                     ▼
            ┌────────────────────────────────────────────────┐
            │ PostgreSQL — Layer 1: Staging (staging_*)      │
            │  • staging_customers       • staging_orders     │
            │  • staging_order_items     • staging_payments   │
            │  • staging_reviews         • staging_products   │
            │  • staging_sellers         • staging_geolocation│
            │  • staging_category_translation                │
            └────────────────────────┬───────────────────────┘
                                     │ (Spark JDBC Read)
                                     ▼
            ┌────────────────────────────────────────────────┐
            │ Apache Spark Batch Processing (eda_spark.py)   │
            │  • Spark SQL (Agregasi CTE Bebas Join Fan-Out) │
            │  • StandardScaler (Z-Score Normalization)      │
            │  • MLlib K-Means (Evaluasi Elbow & Silhouette) │
            │  • Analisis Pertanyaan Penelitian RQ1 – RQ4    │
            └────────────────────────┬───────────────────────┘
                                     │ (Spark JDBC Write)
                                     ▼
            ┌────────────────────────────────────────────────┐
            │ PostgreSQL — Layer 2: Analytics Mart           │
            │  • analytics_kpi_summary                       │
            │  • analytics_top_cities          (RQ1)         │
            │  • analytics_payment_dist                      │
            │  • analytics_order_status                      │
            │  • analytics_monthly_trend                     │
            │  • analytics_top_categories                    │
            │  • analytics_kmeans_eval         (RQ2)         │
            │  • analytics_state_clusters      (RQ1 & RQ2)   │
            │  • analytics_cluster_summary     (RQ2)         │
            │  • analytics_cluster_products    (RQ3)         │
            │  • analytics_density_comparison  (RQ4)         │
            └────────────────────────┬───────────────────────┘
                                     │
                                     ▼
            [ dashboard.py ] (Streamlit Interactive Dashboard)
```

---

## 📁 Struktur Direktori Proyek

```
proyek/
├── data/                                      # 9 berkas CSV dataset Olist
├── config.py                                  # Konfigurasi terpusat (DB, Kafka, nama tabel)
├── producer.py                                # Kafka Producer (CSV -> 9 Topik Kafka)
├── consumer.py                                # Kafka Consumer (9 Topik Kafka -> staging_*)
├── eda_spark.py                               # Spark SQL EDA & MLlib K-Means -> analytics_*
├── dashboard.py                               # Streamlit Dashboard (Membaca analytics_*)
└── README.md                                  # Dokumentasi proyek
```

---

## 🚀 Cara Menjalankan Pipeline End-to-End

### 1. Instalasi Dependensi
```bash
pip install -r requirements.txt
```

### 2. Tahap Ingestion (Apache Kafka $\rightarrow$ PostgreSQL Staging)
Jalankan `consumer.py` dan `producer.py` untuk mengalirkan 9 berkas CSV ke tabel `staging_*`:
```bash
# Terminal 1
python consumer.py

# Terminal 2
python producer.py
```

### 3. Tahap Processing & K-Means Clustering (Apache Spark)
Jalankan `eda_spark.py` untuk memproses data dari `staging_*` menggunakan Spark SQL & PySpark MLlib, mengevaluasi nilai $K = 2 \dots 8$ (*Elbow Method* & *Silhouette Score*), serta menyimpan seluruh tabel hasil ke `analytics_*`:
```bash
python eda_spark.py
```

### 4. Tahap Visualisasi (Streamlit Dashboard)
```bash
streamlit run dashboard.py
```
Buka `http://localhost:8501` di peramban untuk menjelajahi 4 tab interaktif yang menjawab **RQ1, RQ2, RQ3, dan RQ4**.
