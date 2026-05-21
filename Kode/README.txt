PETUNJUK PENGGUNAAN
==============================

Arsitektur: TPC-H CSV -> MinIO (S3) -> Apache Iceberg (Lakehouse) -> dbt -> Data Mart
Stack     : MinIO · Apache Spark 3.5 · Apache Iceberg 1.4.3 · dbt-spark · Jupyter


==============================================================================
1. PRASYARAT
==============================================================================

Install Python dependencies:

    pip install -r requirements.txt


==============================================================================
2. GENERATE DATA TPC-H
==============================================================================

Data tidak disertakan di repo karena terlalu besar. Generate menggunakan DBGEN:

    git clone https://github.com/gregrahn/tpch-kit.git
    cd tpch-kit/dbgen
    make

    # Generate data ~1 GB (scale factor 1)
    ./dbgen -s 1

    # Generate data ~10 GB (scale factor 10) -- sesuai requirement
    ./dbgen -s 10

Output: file .tbl untuk 8 tabel TPC-H:
  customer, lineitem, nation, orders, part, partsupp, region, supplier

Pindahkan file .tbl ke folder Kode/data/tpch-csv/:

    mkdir -p Kode/data/tpch-csv
    mv *.tbl Kode/data/tpch-csv/


==============================================================================
3. DOWNLOAD JAR DEPENDENCIES
==============================================================================

Download via script:

    cd Kode
    bash scripts/download-jars.sh


==============================================================================
4. JALANKAN DOCKER STACK
==============================================================================

    cd Kode
    docker compose up -d

Tunggu semua service siap. Nantinya service dapat diakses di:

  Service           URL / Akses
  -------           -----------
  MinIO Console     http://localhost:9001
                    user: minioadmin / pass: minioadmin123
  Spark Master UI   http://localhost:8080
  Jupyter Lab       http://localhost:8888
  Spark Thrift      localhost:10001 (untuk dbt)

Menghentikan stack:

    # Stop sementara -- container dan data MinIO tetap ada
    docker compose down

    # Stop + hapus semua data MinIO (volume dihapus)
    docker compose down -v


==============================================================================
5. UPLOAD DATA KE MINIO
==============================================================================

Upload file .tbl dari lokal ke bucket tpch-raw di MinIO.

[Git Bash]
    cd Kode
    bash scripts/upload-tpch-to-minio.sh

[PowerShell]
    docker run --rm `
        --network kode_bigdata-net `
        -v "${PWD}/data:/data" `
        minio/mc:latest `
        /bin/sh -c "mc alias set local http://minio:9000 minioadmin minioadmin123 && mc mb --ignore-existing local/tpch-raw && mc cp /data/tpch-csv/*.tbl local/tpch-raw/"

Verifikasi upload di MinIO Console: http://localhost:9001 -> bucket tpch-raw


==============================================================================
6. LOAD DATA KE LAKEHOUSE (CSV -> ICEBERG/PARQUET)
==============================================================================

Buka Jupyter di http://localhost:8888, jalankan notebook:

    work/load_and_read.ipynb

Notebook ini membaca .tbl dari s3a://tpch-raw/, menulis ke Iceberg
(s3a://lakehouse/), membaca kembali, dan menampilkan perbandingan ukuran
CSV vs Parquet.


==============================================================================
7. JALANKAN DBT
==============================================================================

Pastikan Docker stack sudah berjalan sebelum menjalankan dbt.

Masuk ke folder dbt:

    cd Kode/dbt

Test koneksi ke Spark Thrift Server:

    dbt debug

Output yang diharapkan: All checks passed!

Jalankan transformasi (buat Data Mart):

    dbt run

Jalankan data quality tests:

    dbt test

Verifikasi hasil di Jupyter (opsional):

    spark.sql("SELECT * FROM local.tpch_mart.sales_summary LIMIT 10").show()

Generate dokumentasi dbt (opsional):

    dbt docs generate
    dbt docs serve


STRUKTUR DBT PROJECT
--------------------

  dbt/
  +-- dbt_project.yml          konfigurasi project dan materialization
  +-- profiles.yml             koneksi ke Spark Thrift Server (localhost:10001)
  +-- macros/
  |   +-- net_revenue.sql      macro kalkulasi revenue (reusable)
  +-- models/
      +-- staging/
      |   +-- _sources.yml     definisi source tables dari local.tpch.*
      +-- marts/
          +-- revenue_by_segment_region.sql   model utama
          +-- _models.yml      deskripsi kolom + dbt tests (not_null)