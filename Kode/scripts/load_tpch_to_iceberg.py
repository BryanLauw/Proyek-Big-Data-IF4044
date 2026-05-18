"""
load_tpch_to_iceberg.py
=======================
Script PySpark untuk load data TPC-H dari CSV ke Apache Iceberg (Data Lakehouse).

Cara pakai:
  1. Copy file CSV TPC-H ke folder ./data/tpch-csv/
  2. Jalankan: docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                /opt/spark/data/load_tpch_to_iceberg.py

Struktur file CSV yang diharapkan (output dari DBGEN):
  ./data/tpch-csv/lineitem.tbl
  ./data/tpch-csv/orders.tbl
  ./data/tpch-csv/customer.tbl
  ./data/tpch-csv/part.tbl
  ./data/tpch-csv/supplier.tbl
  ./data/tpch-csv/partsupp.tbl
  ./data/tpch-csv/nation.tbl
  ./data/tpch-csv/region.tbl
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("TPC-H Load to Iceberg Lakehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("TPC-H Data Loader: CSV -> Apache Iceberg")

# ============================================================
# Schema Definitions - TPC-H Standard Schema
# ============================================================

# DBGEN menghasilkan CSV dengan delimiter '|' dan trailing '|'
CSV_OPTIONS = {
    "sep": "|",
    "header": "false",
    "inferSchema": "false",
}

# Path CSV (di dalam container Spark)
CSV_BASE = "s3a://tpch-raw/"

# Schema per tabel sesuai TPC-H specification
SCHEMAS = {
    "lineitem": StructType([
        StructField("l_orderkey",      LongType(),    False),
        StructField("l_partkey",       LongType(),    False),
        StructField("l_suppkey",       LongType(),    False),
        StructField("l_linenumber",    IntegerType(), False),
        StructField("l_quantity",      DecimalType(15, 2), False),
        StructField("l_extendedprice", DecimalType(15, 2), False),
        StructField("l_discount",      DecimalType(15, 2), False),
        StructField("l_tax",           DecimalType(15, 2), False),
        StructField("l_returnflag",    StringType(),  False),
        StructField("l_linestatus",    StringType(),  False),
        StructField("l_shipdate",      DateType(),    False),
        StructField("l_commitdate",    DateType(),    False),
        StructField("l_receiptdate",   DateType(),    False),
        StructField("l_shipinstruct",  StringType(),  False),
        StructField("l_shipmode",      StringType(),  False),
        StructField("l_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),  # trailing '|' dari DBGEN
    ]),
    "orders": StructType([
        StructField("o_orderkey",      LongType(),    False),
        StructField("o_custkey",       LongType(),    False),
        StructField("o_orderstatus",   StringType(),  False),
        StructField("o_totalprice",    DecimalType(15, 2), False),
        StructField("o_orderdate",     DateType(),    False),
        StructField("o_orderpriority", StringType(),  False),
        StructField("o_clerk",         StringType(),  False),
        StructField("o_shippriority",  IntegerType(), False),
        StructField("o_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "customer": StructType([
        StructField("c_custkey",       LongType(),    False),
        StructField("c_name",          StringType(),  False),
        StructField("c_address",       StringType(),  False),
        StructField("c_nationkey",     LongType(),    False),
        StructField("c_phone",         StringType(),  False),
        StructField("c_acctbal",       DecimalType(15, 2), False),
        StructField("c_mktsegment",    StringType(),  False),
        StructField("c_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "part": StructType([
        StructField("p_partkey",       LongType(),    False),
        StructField("p_name",          StringType(),  False),
        StructField("p_mfgr",          StringType(),  False),
        StructField("p_brand",         StringType(),  False),
        StructField("p_type",          StringType(),  False),
        StructField("p_size",          IntegerType(), False),
        StructField("p_container",     StringType(),  False),
        StructField("p_retailprice",   DecimalType(15, 2), False),
        StructField("p_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "supplier": StructType([
        StructField("s_suppkey",       LongType(),    False),
        StructField("s_name",          StringType(),  False),
        StructField("s_address",       StringType(),  False),
        StructField("s_nationkey",     LongType(),    False),
        StructField("s_phone",         StringType(),  False),
        StructField("s_acctbal",       DecimalType(15, 2), False),
        StructField("s_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "partsupp": StructType([
        StructField("ps_partkey",      LongType(),    False),
        StructField("ps_suppkey",      LongType(),    False),
        StructField("ps_availqty",     IntegerType(), False),
        StructField("ps_supplycost",   DecimalType(15, 2), False),
        StructField("ps_comment",      StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "nation": StructType([
        StructField("n_nationkey",     LongType(),    False),
        StructField("n_name",          StringType(),  False),
        StructField("n_regionkey",     LongType(),    False),
        StructField("n_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
    "region": StructType([
        StructField("r_regionkey",     LongType(),    False),
        StructField("r_name",          StringType(),  False),
        StructField("r_comment",       StringType(),  False),
        StructField("_trailing",       StringType(),  True),
    ]),
}

# ============================================================
# Buat Database di Iceberg
# ============================================================
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.tpch")
print("\nNamespace 'local.tpch' siap.\n")

# ============================================================
# Load setiap tabel TPC-H
# ============================================================

def load_table(table_name: str, schema: StructType):
    """Load satu tabel dari CSV ke Iceberg format Parquet."""
    print(f"Loading table: {table_name}")
    start = time.time()

    # Baca CSV dari MinIO bucket tpch-raw
    csv_path = f"{CSV_BASE}{table_name}.tbl"
    
    df = spark.read \
        .format("csv") \
        .options(**CSV_OPTIONS) \
        .schema(schema) \
        .load(csv_path)

    # Hapus kolom trailing yang dibuat DBGEN
    cols_to_keep = [f.name for f in schema.fields if f.name != "_trailing"]
    df = df.select(cols_to_keep)
    
    # Atur jumlah partisi (biar parallel & output file rapi)
    df = df.repartition(4)

    row_count = df.count()
    print(f"Rows read from CSV: {row_count:,}")

    # Tulis ke Iceberg (format Parquet by default)
    df.writeTo(f"local.tpch.{table_name}") \
      .tableProperty("write.format.default", "parquet") \
      .tableProperty("write.parquet.compression-codec", "snappy") \
      .createOrReplace()

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s -> local.tpch.{table_name}")
    return row_count


# Urutan load: referential integrity (kecil dulu)
load_order = ["region", "nation", "part", "supplier", "customer",
              "partsupp", "orders", "lineitem"]

total_rows = 0
for table in load_order:
    try:
        rows = load_table(table, SCHEMAS[table])
        total_rows += rows
        print()
    except Exception as e:
        print(f"Error loading {table}: {e}\n")

# ============================================================
# Verifikasi hasil load
# ============================================================
print("Verifikasi Tabel di Iceberg:")

spark.sql("SHOW TABLES IN local.tpch").show()

for table in load_order:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.tpch.{table}").collect()[0]["cnt"]
        print(f"{table:12s}: {count:>12,} rows")
    except Exception as e:
        print(f"{table:12s}: ERROR - {e}")

print(f"\nTotal rows loaded: {total_rows:,}")
print("Data TPC-H berhasil dimuat ke Apache Iceberg!")
print("Cek MinIO Console: http://localhost:9001")
print("Bucket: lakehouse/tpch/")

spark.stop()