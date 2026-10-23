# PANDUAN INSTALASI DAN MENJALANKAN PROYEK BIG DATA OLIST
======================================================

## A. GAMBARAN UMUM

Proyek ini menggunakan:
1. Python untuk producer, consumer, dan dashboard.
2. Apache Kafka untuk mengirim data.
3. PostgreSQL sebagai database.
4. Apache Spark untuk Exploratory Data Analysis (EDA) dan K-Means Clustering.
5. Streamlit untuk menampilkan dashboard.

Alur sistem:

Dataset CSV -> Kafka Producer -> Kafka -> Kafka Consumer -> PostgreSQL -> PySpark EDA dan Clustering -> PostgreSQL -> Streamlit Dashboard

Semua anggota tim harus menginstal dependensi yang diperlukan di komputer masing-masing. Instalasi PostgreSQL, Kafka, Java, dan konfigurasi sistem tidak otomatis dilakukan melalui requirements.txt.

## B. PERSIAPAN FILE PROYEK

1. Install Git dari:
   https://git-scm.com/downloads

2. Install Visual Studio Code dari:
   https://code.visualstudio.com/

3. Buka terminal PowerShell di VS Code.

4. Clone repository GitHub:

   git clone https://github.com/Lehonci/olist.git

5. Masuk ke folder proyek:

   cd olist

6. Periksa isi folder:

   Get-ChildItem

   File yang diperlukan:
   - producer.py
   - consumer.py
   - eda_spark.py
   - dashboard.py
   - requirements.txt
   - README.md
   - Folder data yang berisi dataset CSV

Jika folder data tidak ditemukan, pastikan dataset tersedia sebelum menjalankan pipeline.

## C. INSTALL PYTHON DAN DEPENDENSI

1. Install Python 3.10.x dari:
   https://www.python.org/downloads/

   Gunakan versi Python 3.10 untuk mengikuti lingkungan pengembangan proyek.

2. Pastikan Python terdeteksi:

   python --version

   Jika perintah tersebut tidak berfungsi, coba:

   py -3.10 --version

3. Buat virtual environment di folder proyek:

   py -3.10 -m venv .venv

4. Aktifkan virtual environment:

   .\.venv\Scripts\Activate.ps1

   Jika PowerShell menolak aktivasi karena kebijakan eksekusi, jalankan perintah berikut untuk sesi terminal saat ini:

   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

   Kemudian ulangi aktivasi virtual environment.

5. Perbarui pip:

   python -m pip install --upgrade pip

6. Install seluruh dependensi Python:

   pip install -r requirements.txt

7. Pastikan instalasi berhasil:

   pip list

Catatan:
- Jalankan perintah Python setelah virtual environment aktif.
- Virtual environment hanya menyimpan dependensi Python, bukan Java, Kafka, PostgreSQL, atau Hadoop.
- Jika terjadi error instalasi, periksa versi Python dan pesan error sebelum melanjutkan.

## D. INSTALL JAVA JDK

1. Install Eclipse Temurin JDK 21 dari:

   https://adoptium.net/

2. Setelah instalasi, sesuaikan JAVA_HOME dengan lokasi JDK di komputer masing-masing.

   Contoh untuk PowerShell:

   $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"
   $env:Path = "$env:JAVA_HOME\bin;$env:Path"

   Ganti lokasi tersebut jika folder instalasi JDK berbeda.

3. Periksa Java:

   java -version

   javac -version

4. Pastikan keduanya menampilkan versi Java yang sesuai.

Catatan:
Variabel lingkungan di atas hanya berlaku untuk sesi PowerShell yang sedang digunakan. Jika terminal baru dibuka, atur kembali variabel tersebut atau konfigurasi melalui Environment Variables Windows.

## E. INSTALASI DAN KONFIGURASI KAFKA

1. Download Apache Kafka 4.2.2 dari:

   https://kafka.apache.org/downloads

2. Ekstrak arsip Kafka ke folder yang mudah diakses.

   Contoh:

   C:\kafka_2.13-4.2.2

3. Buka PowerShell dan masuk ke folder Kafka:

   cd C:\kafka_2.13-4.2.2

4. Kafka 4.x menggunakan mode KRaft. Untuk instalasi baru, buat UUID penyimpanan:

   .\bin\windows\kafka-storage.bat random-uuid

5. Salin UUID yang dihasilkan, kemudian format penyimpanan Kafka:

   .\bin\windows\kafka-storage.bat format -t "UUID_HASIL_PERINTAH" -c .\config\server.properties

   Ganti UUID_HASIL_PERINTAH dengan UUID yang sebenarnya.

   PENTING:
   - Format penyimpanan hanya dilakukan sekali untuk direktori Kafka baru.
   - Jangan mengulangi format pada penyimpanan Kafka yang sudah berisi data.
   - Konfigurasi dapat berbeda jika menggunakan konfigurasi KRaft khusus.

6. Jalankan Kafka:

   .\bin\windows\kafka-server-start.bat .\config\server.properties

7. Biarkan terminal Kafka tetap terbuka selama pipeline dijalankan.

8. Buka terminal PowerShell kedua, masuk ke folder Kafka, lalu periksa daftar topic:

   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list

Catatan:
Jika Kafka gagal dijalankan di Windows, periksa pesan error dan konfigurasi Kafka. Pada beberapa lingkungan Windows, skrip Kafka juga dapat mengalami masalah terkait WMIC. Perbaiki sesuai pesan error yang muncul.

## F. INSTALL POSTGRESQL

1. Download PostgreSQL dari:

   https://www.postgresql.org/download/windows/

2. Install PostgreSQL. Gunakan PostgreSQL 18 jika ingin mengikuti lingkungan pengembangan proyek.

3. Saat instalasi, catat:
   - Username PostgreSQL.
   - Password PostgreSQL.
   - Port PostgreSQL, biasanya 5432.

4. Buka pgAdmin atau SQL Shell (psql).

5. Buat database dengan nama:

   olist_db

   Jika database sudah tersedia, tidak perlu membuatnya kembali.

6. Pastikan PostgreSQL sedang berjalan.

7. Sesuaikan konfigurasi koneksi PostgreSQL di producer/consumer atau file Python yang menggunakannya, jika diperlukan.

   Periksa nilai:
   - host
   - port
   - database
   - user
   - password

   Nilai koneksi harus sesuai dengan konfigurasi PostgreSQL lokal masing-masing.

8. Jangan mengunggah password asli ke GitHub. Gunakan environment variable atau konfigurasi lokal yang tidak ikut di-commit.

## G. KONFIGURASI HADOOP UNTUK PYSPARK DI WINDOWS

Spark pada Windows dapat memerlukan winutils.exe untuk operasi filesystem Hadoop tertentu.

1. Pastikan instalasi Java dan dependensi PySpark sudah selesai.

2. Periksa apakah proyek memerlukan konfigurasi HADOOP_HOME.

3. Jika error menyebut winutils.exe atau HADOOP_HOME, siapkan instalasi Hadoop Windows yang kompatibel dan sumber winutils.exe yang tepercaya.

4. Atur variabel lingkungan sesuai lokasi instalasi, misalnya:

   $env:HADOOP_HOME = "C:\hadoop"
   $env:Path = "$env:HADOOP_HOME\bin;$env:Path"

5. Pastikan file berikut tersedia jika memang dibutuhkan:

   C:\hadoop\bin\winutils.exe

Catatan:
Jangan menganggap winutils.exe dari instalasi komputer lain pasti cocok untuk semua lingkungan. Gunakan sumber yang tepercaya dan konfigurasi yang kompatibel dengan versi Spark yang digunakan.

## H. MENJALANKAN PIPELINE DATA

Sebelum memulai, pastikan:
- Virtual environment aktif.
- Java terdeteksi.
- Kafka berjalan.
- PostgreSQL berjalan.
- Database olist_db tersedia.
- Konfigurasi koneksi di kode sesuai dengan komputer masing-masing.
- Dataset CSV tersedia di folder data.

LANGKAH 1 — Jalankan Kafka

Pastikan server Kafka aktif pada localhost:9092.

LANGKAH 2 — Jalankan Consumer

Buka terminal baru di folder proyek dan aktifkan virtual environment:

   .\.venv\Scripts\Activate.ps1

Jalankan:

   python consumer.py

Consumer harus berjalan sebelum producer mengirim data agar pesan dapat diproses sesuai konfigurasi consumer.

Perhatikan log untuk memastikan consumer berhasil terhubung ke Kafka dan PostgreSQL.

LANGKAH 3 — Jalankan Producer

Buka terminal baru di folder proyek dan aktifkan virtual environment.

Jalankan:

   python producer.py

Producer akan membaca dataset CSV dan mengirimkan data ke topic Kafka sesuai konfigurasi di producer.py.

Tunggu hingga proses pengiriman data selesai. Perhatikan log producer dan consumer.

LANGKAH 4 — Pastikan Data Masuk ke PostgreSQL

Buka pgAdmin, pilih database olist_db, lalu periksa tabel staging yang dibuat oleh proyek.

Tabel staging yang digunakan:
- staging_customers
- staging_orders
- staging_order_items
- staging_payments
- staging_reviews
- staging_products
- staging_sellers
- staging_geolocation
- staging_category_translation

Pastikan data telah masuk sebelum melanjutkan.

PENTING:
Jangan menjalankan producer berulang kali tanpa memeriksa mekanisme deduplikasi dan isi tabel. Pengiriman ulang dapat menyebabkan data duplikat.

LANGKAH 5 — Jalankan EDA dan K-Means Clustering

Setelah proses ingest selesai, buka terminal baru.

Aktifkan virtual environment, lalu atur JAVA_HOME dan HADOOP_HOME jika diperlukan.

Jalankan:

   python eda_spark.py

Tunggu sampai proses EDA dan clustering selesai. Periksa log untuk memastikan hasil berhasil disimpan ke PostgreSQL.

Tabel hasil yang digunakan:
- clustering_eval_metrics
- clustering_state_results
- clustering_top_products

Jika Spark gagal dijalankan, periksa Java, HADOOP_HOME, koneksi JDBC, dan konfigurasi database.

LANGKAH 6 — Jalankan Dashboard

Buka terminal baru di folder proyek.

Aktifkan virtual environment:

   .\.venv\Scripts\Activate.ps1

Atur konfigurasi lingkungan yang diperlukan jika terminal baru belum memiliki JAVA_HOME atau HADOOP_HOME.

Jalankan:

   streamlit run dashboard.py

Streamlit akan menampilkan URL lokal di terminal, biasanya:

   http://localhost:8501

Buka URL tersebut di browser.

Pastikan dashboard dapat membaca data PostgreSQL dan menampilkan metrik, grafik, serta hasil clustering.

## I. URUTAN TERMINAL SAAT MENJALANKAN PROYEK

Terminal 1:
Menjalankan Kafka server.

Terminal 2:
Menjalankan consumer.py.

Terminal 3:
Menjalankan producer.py.

Terminal 4:
Menjalankan eda_spark.py setelah ingest selesai.

Terminal 5:
Menjalankan dashboard.py setelah tabel hasil analisis tersedia.

PostgreSQL harus tetap aktif selama proses yang membutuhkan koneksi database berlangsung.

## J. TROUBLESHOOTING

1. ModuleNotFoundError
   Pastikan virtual environment aktif dan jalankan:

   pip install -r requirements.txt

2. Kafka connection refused
   Pastikan Kafka berjalan dan alamat bootstrap server sesuai konfigurasi kode.

3. PostgreSQL connection refused
   Pastikan PostgreSQL aktif, database tersedia, dan port serta kredensial benar.

4. Spark gagal menemukan Java
   Periksa JAVA_HOME dan jalankan:

   java -version

5. Error winutils.exe
   Periksa HADOOP_HOME dan ketersediaan winutils.exe jika diperlukan.

6. Dashboard gagal membaca tabel
   Pastikan database berisi tabel staging dan tabel hasil analisis. Periksa konfigurasi koneksi di dashboard.py.

7. Data duplikat
   Jangan langsung mengulang producer. Periksa isi tabel dan mekanisme deduplikasi terlebih dahulu.

8. Port sudah digunakan
   Periksa apakah proses Kafka, PostgreSQL, atau Streamlit sebelumnya masih berjalan.

## K. CHECKLIST AKHIR

[ ] Git, VS Code, dan Python terpasang.
[ ] Virtual environment berhasil dibuat.
[ ] requirements.txt berhasil diinstal.
[ ] Java JDK terdeteksi.
[ ] Kafka berhasil dijalankan.
[ ] PostgreSQL berhasil dijalankan.
[ ] Database olist_db tersedia.
[ ] Konfigurasi koneksi sesuai.
[ ] Dataset CSV tersedia.
[ ] Consumer dan producer berhasil memproses data.
[ ] Tabel staging tersedia dan berisi data.
[ ] eda_spark.py berhasil dijalankan.
[ ] Tabel hasil clustering tersedia.
[ ] dashboard.py berhasil dijalankan.
[ ] Dashboard tampil tanpa error.

SELESAI

Dashboard dapat diakses melalui browser lokal setelah semua dependensi, layanan, dan tabel database yang diperlukan tersedia.
