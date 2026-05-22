# Benchmark Report - Lakehouse vs Raw CSV

## 1. Setup
- Dataset: TPC-H (scale factor: <isi>)
- Spark version: <isi>
- Iceberg version: <isi>
- Storage: MinIO (s3a)
- Hardware: CPU/RAM <isi>

## 2. Query Set
| Query | Deskripsi | File |
| --- | --- | --- |
| Q1 | Group-by lineitem | benchmark/queries/q1_groupby.sql |
| Q6 | Filter + sum | benchmark/queries/q6_discount.sql |
| Q3 | Join + top revenue | benchmark/queries/q3_top10.sql |

## 3. Hasil Execution Time
| Query | CSV avg (s) | Iceberg avg (s) | Speedup |
| --- | --- | --- | --- |
| Q1 | <isi> | <isi> | <isi>x |
| Q6 | <isi> | <isi> | <isi>x |
| Q3 | <isi> | <isi> | <isi>x |

## 4. Storage Efficiency
| Format | Ukuran | Rasio |
| --- | --- | --- |
| CSV (tpch-raw) | <isi> | - |
| Parquet/Iceberg (lakehouse) | <isi> | <isi>x lebih kecil |

## 5. Row Count Validation
| Table | CSV | Iceberg | Diff |
| --- | --- | --- | --- |
| lineitem | <isi> | <isi> | <isi> |
| orders | <isi> | <isi> | <isi> |
| customer | <isi> | <isi> | <isi> |
| part | <isi> | <isi> | <isi> |
| supplier | <isi> | <isi> | <isi> |
| partsupp | <isi> | <isi> | <isi> |
| nation | <isi> | <isi> | <isi> |
| region | <isi> | <isi> | <isi> |

## 6. Interpretasi
- <isi ringkasan hasil>
- <isi analisis kenapa Iceberg/Parquet lebih cepat>
