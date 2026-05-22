# Benchmark Lakehouse vs Raw CSV (Spark + Iceberg)

## Tujuan
Benchmark ini membandingkan performa query analytics antara:
- Raw CSV pipeline (Spark membaca file .tbl/.csv langsung)
- Lakehouse pipeline (Spark membaca tabel Apache Iceberg dalam Parquet + Snappy)

Fokus perbandingan hanya pada format dan metadata (CSV vs Iceberg). Engine, dataset, dan object storage dibuat sama.

## Prinsip fairness dan reproducibility
- Dataset sama (TPC-H), skala sama, dan storage sama (MinIO).
- Spark config sama untuk kedua pipeline.
- Query set identik untuk CSV dan Iceberg.
- Ada warm-up dan multiple runs untuk averaging.
- Cache Spark dibersihkan sebelum tiap run.

## Struktur folder

benchmark/
  README.md
  run_benchmark.py
  plot_results.py
  queries/
    q1_groupby.sql
    q3_top10.sql
    q6_discount.sql
  results/
    .gitkeep
  templates/
    benchmark_report.md
    example_results.json

## Prasyarat
1. Docker stack berjalan (MinIO, Spark, Jupyter).
2. File .tbl sudah di-upload ke s3a://tpch-raw/
3. Tabel Iceberg sudah dibuat di local.tpch (lihat notebooks/load_and_read.ipynb).

## Menjalankan benchmark

Disarankan menjalankan via container Spark agar versi JVM dan Spark konsisten:

docker compose exec spark-master /opt/spark/bin/spark-submit /opt/spark/benchmark/run_benchmark.py --mode both --runs 3 --warmup 1

Jika ingin menjalankan dari host, pastikan PySpark dan Spark JVM versinya cocok.

Jalankan benchmark untuk CSV dan Iceberg:

python benchmark/run_benchmark.py --mode both --runs 3 --warmup 1

Hanya CSV:

python benchmark/run_benchmark.py --mode csv --runs 3 --warmup 1

Hanya Iceberg:

python benchmark/run_benchmark.py --mode iceberg --runs 3 --warmup 1

Output utama tersimpan di benchmark/results/*.json dan ringkasan di benchmark/results/summary_*.csv.

## Cara mengukur execution time
- Timing memakai time.perf_counter() agar presisi tinggi.
- Query dieksekusi dengan action (count) untuk memaksa eksekusi penuh.
- Warm-up dilakukan terlebih dahulu, lalu dilakukan beberapa kali run.
- Rata-rata, min, max, dan standard deviation dihitung dari hasil runs.

## Warm-up query
- Warm-up 1-2 kali cukup untuk memastikan Spark sudah inisialisasi dan metadata Iceberg siap.
- Hasil warm-up tidak dimasukkan ke statistik akhir.

## Averaging runtime
- Gunakan 3-5 kali run untuk tiap query.
- Gunakan rata-rata (avg) sebagai angka utama, serta min/max sebagai konteks variasi.

## Menghindari cache bias Spark
- Jangan menggunakan .cache() atau .persist() untuk benchmark.
- Jalankan spark.catalog.clearCache() sebelum tiap run.
- Hindari menjalankan query yang sama bolak-balik tanpa jeda jika ingin cold-ish cache.
- Jika ingin benar-benar cold, restart SparkSession di antara set query (lebih lama, tapi paling bersih).

## Query benchmark (TPC-H inspired)
Query berada di benchmark/queries. Contoh:
- q1_groupby.sql: agregasi lineitem (Q1-style)
- q6_discount.sql: filter + sum (Q6-style)
- q3_top10.sql: join customer, orders, lineitem (Q3-style)

## Storage efficiency
Script mengukur ukuran bucket:
- s3a://tpch-raw/ (CSV)
- s3a://lakehouse/ (Parquet/Iceberg)
Rasio kompresi dihitung sebagai csv_bytes / parquet_bytes.

## Row count validation
Script menghitung jumlah baris per tabel (CSV vs Iceberg) untuk validasi pipeline.

## Template hasil benchmark
Lihat benchmark/templates/benchmark_report.md untuk format laporan.

## Visualisasi benchmark
Gunakan plot_results.py untuk membuat grafik bar dari summary CSV:

python benchmark/plot_results.py --input benchmark/results/summary_YYYYMMDD_HHMMSS.csv --output benchmark/results/benchmark_plot.png

## Interpretasi hasil (ringkas)
- Parquet bersifat columnar dan terkompresi sehingga scan dan agregasi lebih efisien.
- Iceberg menyimpan metadata sehingga optimisasi query lebih efektif.
- CSV membutuhkan parsing text dan membaca semua kolom sehingga lebih lambat untuk query analytics.

## Best practices benchmarking Spark + Iceberg
- Pastikan spark.sql.shuffle.partitions konsisten.
- Gunakan dataset skala cukup besar agar perbedaan terlihat.
- Hindari benchmark dengan data yang sudah ter-cache.
- Catat versi Spark, Iceberg, dan konfigurasi utama.
- Simpan semua hasil ke file agar reproducible.
