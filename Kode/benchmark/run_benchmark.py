#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import glob
import json
import os
import statistics
import time
from typing import Dict, List

from pyspark.sql import SparkSession

CSV_BASE = "s3a://tpch-raw"
ICEBERG_BASE = "local.tpch"

TABLE_SCHEMAS = {
    "customer": "c_custkey LONG, c_name STRING, c_address STRING, c_nationkey LONG, c_phone STRING, c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING, _trailing STRING",
    "orders": "o_orderkey LONG, o_custkey LONG, o_orderstatus STRING, o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING, _trailing STRING",
    "lineitem": "l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, l_shipmode STRING, l_comment STRING, _trailing STRING",
    "part": "p_partkey LONG, p_name STRING, p_mfgr STRING, p_brand STRING, p_type STRING, p_size INT, p_container STRING, p_retailprice DECIMAL(15,2), p_comment STRING, _trailing STRING",
    "supplier": "s_suppkey LONG, s_name STRING, s_address STRING, s_nationkey LONG, s_phone STRING, s_acctbal DECIMAL(15,2), s_comment STRING, _trailing STRING",
    "partsupp": "ps_partkey LONG, ps_suppkey LONG, ps_availqty INT, ps_supplycost DECIMAL(15,2), ps_comment STRING, _trailing STRING",
    "nation": "n_nationkey LONG, n_name STRING, n_regionkey LONG, n_comment STRING, _trailing STRING",
    "region": "r_regionkey LONG, r_name STRING, r_comment STRING, _trailing STRING",
}

TABLE_LIST = list(TABLE_SCHEMAS.keys())


def resolve_jars(jar_dirs: List[str]) -> List[str]:
    patterns = [
        "iceberg-spark-runtime-*.jar",
        "hadoop-aws-*.jar",
        "aws-java-sdk-bundle-*.jar",
    ]
    for jar_dir in jar_dirs:
        if not os.path.isdir(jar_dir):
            continue
        jars = []
        for pattern in patterns:
            matches = sorted(glob.glob(os.path.join(jar_dir, pattern)))
            if matches:
                jars.append(matches[-1])
        if jars:
            return jars
    return []


def build_spark(master: str, shuffle_partitions: int) -> SparkSession:
    jar_dirs = [
        os.environ.get("BENCH_JAR_DIR", ""),
        "/home/jovyan/extra-jars",
        "/opt/spark/extra-jars",
    ]
    jars = resolve_jars([d for d in jar_dirs if d])
    if jars:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {','.join(jars)} pyspark-shell"

    builder = (
        SparkSession.builder
        .appName("tpch-benchmark")
        .master(master)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    if jars:
        classpath = os.pathsep.join(jars)
        builder = (
            builder
            .config("spark.jars", ",".join(jars))
            .config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
        )
    if shuffle_partitions > 0:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    return builder.getOrCreate()


def read_csv_table(spark: SparkSession, table: str):
    schema = TABLE_SCHEMAS[table]
    return (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .schema(schema)
        .csv(f"{CSV_BASE}/{table}.tbl")
        .drop("_trailing")
    )


def create_csv_views(spark: SparkSession, tables: List[str]) -> None:
    for table in tables:
        df = read_csv_table(spark, table)
        df.createOrReplaceTempView(f"csv_{table}")


def load_query_templates(query_dir: str) -> Dict[str, str]:
    queries = {}
    for path in sorted(glob.glob(os.path.join(query_dir, "*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        with open(path, "r", encoding="utf-8") as f:
            queries[name] = f.read().strip()
    return queries


def render_sql(template: str, table_map: Dict[str, str]) -> str:
    sql = template
    for key, value in table_map.items():
        sql = sql.replace(f"{{{{{key}}}}}", value)
    return sql


def timed_query(spark: SparkSession, sql_text: str, warmup: int, runs: int, clear_cache: bool):
    for _ in range(warmup):
        spark.sql(sql_text).count()

    durations = []
    row_count = None
    for _ in range(runs):
        if clear_cache:
            spark.catalog.clearCache()
        start = time.perf_counter()
        count = spark.sql(sql_text).count()
        elapsed = time.perf_counter() - start
        durations.append(elapsed)
        if row_count is None:
            row_count = count

    stats = {
        "runs": runs,
        "warmup": warmup,
        "avg_sec": sum(durations) / len(durations),
        "min_sec": min(durations),
        "max_sec": max(durations),
        "stdev_sec": statistics.pstdev(durations) if len(durations) > 1 else 0.0,
        "row_count": row_count,
        "times_sec": durations,
    }
    return stats


def bucket_size_bytes(spark: SparkSession, bucket_uri: str) -> int:
    from py4j.java_gateway import java_import

    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    java_import(spark._jvm, "java.net.URI")

    conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.FileSystem.get(spark._jvm.URI(bucket_uri), conf)
    path = spark._jvm.Path(bucket_uri)
    return fs.getContentSummary(path).getLength()


def validate_row_counts(spark: SparkSession, tables: List[str]) -> Dict[str, Dict[str, int]]:
    results = {}
    for table in tables:
        csv_count = spark.table(f"csv_{table}").count()
        iceberg_count = spark.table(f"{ICEBERG_BASE}.{table}").count()
        results[table] = {
            "csv": int(csv_count),
            "iceberg": int(iceberg_count),
            "diff": int(csv_count) - int(iceberg_count),
        }
    return results


def write_summary_csv(path: str, query_names: List[str], results: Dict) -> None:
    rows = []
    for q in query_names:
        csv_stats = results.get("queries", {}).get("csv", {}).get(q)
        ice_stats = results.get("queries", {}).get("iceberg", {}).get(q)
        csv_avg = csv_stats["avg_sec"] if csv_stats else None
        ice_avg = ice_stats["avg_sec"] if ice_stats else None
        speedup = (csv_avg / ice_avg) if (csv_avg and ice_avg) else None
        row_count = None
        if csv_stats:
            row_count = csv_stats.get("row_count")
        elif ice_stats:
            row_count = ice_stats.get("row_count")
        rows.append({
            "query": q,
            "csv_avg_sec": csv_avg,
            "iceberg_avg_sec": ice_avg,
            "speedup": speedup,
            "csv_min_sec": csv_stats["min_sec"] if csv_stats else None,
            "iceberg_min_sec": ice_stats["min_sec"] if ice_stats else None,
            "csv_max_sec": csv_stats["max_sec"] if csv_stats else None,
            "iceberg_max_sec": ice_stats["max_sec"] if ice_stats else None,
            "csv_stdev_sec": csv_stats["stdev_sec"] if csv_stats else None,
            "iceberg_stdev_sec": ice_stats["stdev_sec"] if ice_stats else None,
            "row_count": row_count,
        })

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_query_dir = os.path.join(base_dir, "queries")
    default_outdir = os.path.join(base_dir, "results")

    parser = argparse.ArgumentParser(description="Benchmark Spark CSV vs Iceberg")
    parser.add_argument("--mode", choices=["csv", "iceberg", "both"], default="both")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--query-dir", default=default_query_dir)
    parser.add_argument("--outdir", default=default_outdir)
    parser.add_argument("--skip-row-count", action="store_true")
    parser.add_argument("--no-clear-cache", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    spark = build_spark(args.master, args.shuffle_partitions)
    queries = load_query_templates(args.query_dir)
    if not queries:
        raise RuntimeError("No SQL files found in query-dir")

    need_csv = (args.mode in ["csv", "both"]) or (not args.skip_row_count)
    if need_csv:
        create_csv_views(spark, TABLE_LIST)

    table_map_csv = {name: f"csv_{name}" for name in TABLE_LIST}
    table_map_ice = {name: f"{ICEBERG_BASE}.{name}" for name in TABLE_LIST}

    results = {
        "meta": {
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "mode": args.mode,
            "runs": args.runs,
            "warmup": args.warmup,
            "master": args.master,
            "shuffle_partitions": args.shuffle_partitions,
        },
        "queries": {},
    }

    clear_cache = not args.no_clear_cache

    if args.mode in ["csv", "both"]:
        results["queries"]["csv"] = {}
        for name, template in queries.items():
            sql_text = render_sql(template, table_map_csv)
            results["queries"]["csv"][name] = timed_query(
                spark, sql_text, args.warmup, args.runs, clear_cache
            )

    if args.mode in ["iceberg", "both"]:
        results["queries"]["iceberg"] = {}
        for name, template in queries.items():
            sql_text = render_sql(template, table_map_ice)
            results["queries"]["iceberg"][name] = timed_query(
                spark, sql_text, args.warmup, args.runs, clear_cache
            )

    try:
        csv_bytes = bucket_size_bytes(spark, "s3a://tpch-raw/")
        ice_bytes = bucket_size_bytes(spark, "s3a://lakehouse/")
        results["storage"] = {
            "csv_bytes": int(csv_bytes),
            "iceberg_bytes": int(ice_bytes),
            "compression_ratio": float(csv_bytes / ice_bytes) if ice_bytes else None,
        }
    except Exception as e:
        results["storage_error"] = str(e)

    if not args.skip_row_count:
        try:
            results["row_counts"] = validate_row_counts(spark, TABLE_LIST)
        except Exception as e:
            results["row_count_error"] = str(e)

    stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(args.outdir, f"benchmark_{stamp}.json")
    summary_path = os.path.join(args.outdir, f"summary_{stamp}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    write_summary_csv(summary_path, list(queries.keys()), results)

    print(f"Results JSON: {json_path}")
    print(f"Summary CSV: {summary_path}")

    spark.stop()


if __name__ == "__main__":
    main()
