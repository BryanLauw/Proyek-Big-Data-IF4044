# IF4044 Proyek Big Data — Kode

Arsitektur: **TPC-H CSV → MinIO (S3) → Apache Iceberg (Lakehouse) → dbt → Data Mart**

Stack: MinIO · Apache Spark 3.5 · Apache Iceberg 1.4.3 · dbt-spark · Jupyter

---

## Daftar Isi

- [Prasyarat](#prasyarat)
- [1. Generate Data TPC-H](#1-generate-data-tpc-h)
- [2. Download JAR Dependencies](#2-download-jar-dependencies)
- [3. Jalankan Docker Stack](#3-jalankan-docker-stack)
- [4. Upload Data ke MinIO](#4-upload-data-ke-minio)
- [5. Load Data ke Lakehouse](#5-load-data-ke-lakehouse-csv--icebergparquet)
- [6. Test Koneksi](#6-test-koneksi-opsional)
- [7. Jalankan dbt](#7-jalankan-dbt)
- [Struktur Folder](#struktur-folder)

---

## Prasyarat

- Docker Desktop (dengan WSL2 backend di Windows)
- Git
- Python 3.10+
- ~15 GB ruang disk bebas (untuk data TPC-H dan Docker images)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## 1. Generate Data TPC-H

Data tidak disertakan di repo karena terlalu besar. Generate menggunakan DBGEN:

```bash
# Clone DBGEN
git clone https://github.com/gregrahn/tpch-kit.git
cd tpch-kit/dbgen

# Compile (Linux/macOS/WSL)
make

# Generate data ~1 GB (scale factor 1)
./dbgen -s 1

# Generate data ~10 GB (scale factor 10) — sesuai requirement
./dbgen -s 10
```

Output: file `.tbl` untuk 8 tabel TPC-H:
`customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, `supplier`

Pindahkan file `.tbl` ke folder `Kode/data/tpch-csv/`:

```bash
mkdir -p Kode/data/tpch-csv
mv *.tbl Kode/data/tpch-csv/
```

---

## 2. Download JAR Dependencies

JAR files tidak disertakan di repo karena ukurannya besar (total ~316 MB). Download via script:

```bash
cd Kode
bash scripts/download-jars.sh
```

---

## 3. Jalankan Docker Stack

```bash
cd Kode
docker compose up -d
```

Tunggu semua service sehat (±30 detik):

| Service | URL |
|---|---|
| MinIO Console | http://localhost:9001 (user: `minioadmin` / pass: `minioadmin123`) |
| Spark Master UI | http://localhost:8080 |
| Jupyter Lab | http://localhost:8888 |
| Spark Thrift (dbt) | `localhost:10001` |

**Menghentikan stack:**

```bash
# Stop sementara — container dan data MinIO tetap ada
docker compose down

# Stop + hapus semua data MinIO (volume dihapus)
docker compose down -v
```

---

## 4. Upload Data ke MinIO

Upload file `.tbl` dari lokal ke bucket `tpch-raw` di MinIO:

**Git Bash:**
```bash
cd Kode
bash scripts/upload-tpch-to-minio.sh
```

**PowerShell:**
```powershell
docker run --rm `
    --network kode_bigdata-net `
    -v "${PWD}/data:/data" `
    minio/mc:latest `
    /bin/sh -c "mc alias set local http://minio:9000 minioadmin minioadmin123 && mc mb --ignore-existing local/tpch-raw && mc cp /data/tpch-csv/*.tbl local/tpch-raw/"
```

Verifikasi upload di MinIO Console: http://localhost:9001 → bucket `tpch-raw`

---

## 5. Load Data ke Lakehouse (CSV → Iceberg/Parquet)

Buka Jupyter di http://localhost:8888, jalankan notebook:

```
work/load_and_read.ipynb
```

Notebook ini membaca `.tbl` dari `s3a://tpch-raw/`, menulis ke Iceberg (`s3a://lakehouse/`), membaca kembali, dan menampilkan perbandingan ukuran CSV vs Parquet.

---

## 6. Test Koneksi (Opsional)

Buka Jupyter di http://localhost:8888, lalu jalankan notebook:

```
work/test_connection.ipynb
```

Notebook ini memverifikasi koneksi ke Spark, baca/tulis ke MinIO, dan membuat Iceberg table sederhana.

---

## 7. Jalankan dbt

Pastikan Docker stack sudah berjalan (`docker compose up -d`) sebelum menjalankan dbt.

**Prasyarat: buat namespace Iceberg** yang dibutuhkan dbt. Buka Jupyter di http://localhost:8888 dan jalankan:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.tpch_mart")
```

> Catatan: namespace ini sudah dibuat otomatis saat `docker compose up` (startup script `spark-thrift`), tapi perlu dibuat ulang jika lakehouse di-reset.

**Masuk ke folder dbt:**

```bash
cd Kode/dbt
```

**Test koneksi ke Spark Thrift Server:**

```bash
dbt debug
```

Output yang diharapkan: `All checks passed!`

**Jalankan transformasi (buat Data Mart):**

```bash
dbt run
```

Perintah ini membuat tabel `tpch_mart.sales_summary` di Iceberg (tersimpan di MinIO `s3a://lakehouse/`). Model menggabungkan 5 tabel (`orders`, `lineitem`, `customer`, `nation`, `region`) dan menghitung total revenue per region, nation, tanggal order, dan status.

**Jalankan data quality tests:**

```bash
dbt test
```

Menjalankan pengecekan `not_null` pada semua kolom output model `sales_summary`.

**Verifikasi hasil di Jupyter (opsional):**

Buka Jupyter di http://localhost:8888, lalu jalankan:

```python
spark.sql("SELECT * FROM local.tpch_mart.sales_summary LIMIT 10").show()
```

**Generate dokumentasi dbt (opsional):**

```bash
dbt docs generate
dbt docs serve
```

Buka http://localhost:8080 untuk melihat dokumentasi interaktif model dan lineage.

---

### Struktur dbt Project

```
dbt/
├── dbt_project.yml            # konfigurasi project dan materialization
├── profiles.yml               # koneksi ke Spark Thrift Server (localhost:10001)
├── macros/
│   └── revenue.sql            # macro calculate_revenue(extendedprice, discount)
└── models/
    ├── staging/
    │   └── _sources.yml       # definisi source tables dari local.tpch.*
    └── marts/
        ├── sales_summary.sql  # model utama — join 5 tabel, agregasi revenue
        └── _models.yml        # deskripsi kolom + dbt tests (not_null)
```

---

## Struktur Folder

```
Kode/
├── docker-compose.yml
├── requirements.txt
├── scripts/
│   ├── download-jars.sh          # download JAR dependencies dari Maven
│   └── upload-tpch-to-minio.sh   # upload .tbl ke MinIO bucket tpch-raw
├── spark/
│   ├── conf/spark-defaults.conf  # konfigurasi Spark + Iceberg + S3A
│   └── jars/                     # JAR dependencies (gitignored, download via script)
├── notebooks/
│   ├── load_and_read.ipynb       # load tpch-raw → Iceberg, baca, bandingkan ukuran
│   └── test_connection.ipynb     # tes koneksi Spark + MinIO + Iceberg
├── data/                         # gitignored — generate lokal dengan DBGEN
│   └── tpch-csv/                 # file .tbl hasil DBGEN
└── dbt/                          # dbt project
    ├── dbt_project.yml           # konfigurasi project dan materialization
    ├── profiles.yml              # koneksi Spark Thrift Server
    ├── macros/
    │   └── revenue.sql           # macro calculate_revenue(extendedprice, discount)
    └── models/
        ├── staging/
        │   └── _sources.yml      # source definitions (local.tpch.*)
        └── marts/
            ├── sales_summary.sql # mart utama — join 5 tabel, agregasi revenue
            └── _models.yml       # deskripsi kolom + dbt tests
```
