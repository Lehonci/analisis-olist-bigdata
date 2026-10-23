# Proyek Praktikum Teknologi Big Data: Olist Brazilian E-Commerce

Sistem Big Data End-to-End untuk analisis dan segmentasi wilayah pelanggan menggunakan dataset **Brazilian E-Commerce Public Dataset by Olist**.

---

## 🏗️ Arsitektur Sistem

```
[ 9 File CSV Data Source ]
           │
           ▼
   [ Apache Kafka ] (Ingestion 9 Topik)
           │
           ▼
  [ PostgreSQL Staging ] (9 Tabel Staging Relasional)
           │
           ▼
    [ Apache Spark ] (Batch Processing & Feature Engineering)
           │
           ▼
 [ Spark SQL + MLlib K-Means ] (Evaluasi Elbow & Silhouette Score)
           │
           ▼
 [ Streamlit Dashboard ] (Visualisasi Interaktif & Analisis Strategi)
```

---

## 📁 Struktur File Proyek

```
proyek/
├── data/                                      # Folder 9 file CSV Olist
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   └── product_category_name_translation.csv
├── producer.py                                # Kafka Producer (Ingestion 9 CSV ke 9 topik)
├── consumer.py                                # Kafka Consumer (Simpan event ke PostgreSQL Staging)
├── eda_spark.py                               # Spark SQL EDA & MLlib K-Means (Elbow + Silhouette)
├── dashboard.py                               # Interactive Streamlit Web Dashboard
├── requirements.txt                           # Library & dependencies Python
└── README.md                                  # Dokumentasi dan panduan eksekusi
```

---

## 🚀 Panduan Menjalankan Sistem

### 1. Prasyarat & Instalasi
Pastikan dependensi telah terinstal:
```bash
pip install -r requirements.txt
```

Pastikan layanan berikut berjalan di lokal:
1. **PostgreSQL** di port `5432` dengan database `olist_db`, user `postgres`, password `password`.
2. **Apache Kafka & Zookeeper** di port `9092`.

---

### 2. Jalankan Ingestion Layer (Kafka Producer & Consumer)

**Terminal 1 (Jalankan Consumer untuk menangkap event & buat tabel staging):**
```bash
python consumer.py
```

**Terminal 2 (Jalankan Producer untuk mempublikasikan 9 dataset ke Kafka):**
```bash
python producer.py
```

---

### 3. Jalankan Batch Processing, EDA & K-Means (Apache Spark)

```bash
python eda_spark.py
```
*Script ini akan:*
- Membaca 9 tabel dari PostgreSQL staging.
- Menjalankan analisis Spark SQL (distribusi wilayah, kategori produk terpopuler, dsb.).
- Melakukan feature engineering & scaling per wilayah (State).
- Menghitung **Elbow Method (WSSSE Cost)** dan **Silhouette Score** untuk $K = 2 \dots 8$.
- Melatih model K-Means dengan $K$ optimal dan menyimpan hasil cluster ke PostgreSQL.

---

### 4. Jalankan Dashboard Visualisasi (Streamlit)

```bash
streamlit run dashboard.py
```

Buka browser pada alamat: `http://localhost:8501`

**Fitur Dashboard:**
1. **📊 Ringkasan & KPI**: Total customer, revenue, rating, status order, dan metode pembayaran.
2. **🗺️ Distribusi Geografis**: Top 10 state per transaksi, rata-rata ongkir logistik, dan tabel 27 negara bagian Brazil.
3. **📦 Analisis Kategori Produk**: 15 kategori terlaris berdasarkan kuantitas & nominal penjualan.
4. **🤖 K-Means Clustering & Evaluasi (Elbow + Siluet)**:
   - Kurva **Elbow Method** dan grafik **Silhouette Score per K**.
   - Scatter plot interaktif cluster wilayah (Orders vs Revenue).
   - Tabel profil & persona cluster serta rekomendasi strategi bisnis.
   - Kategori produk paling diminati pada masing-masing kelompok cluster.
