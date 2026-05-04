#!/bin/bash
set -e

echo "======================================="
echo " Upload TPC-H Data ke MinIO (tpch-raw)"
echo "======================================="
echo ""

# Config

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin123"

LOCAL_DATA_DIR="./data/tpch-csv"
BUCKET_NAME="tpch-raw"

# Check folder

if [ ! -d "$LOCAL_DATA_DIR" ]; then
echo "Folder $LOCAL_DATA_DIR tidak ditemukan"
exit 1
fi

echo "Source folder: $LOCAL_DATA_DIR"
echo "Bucket tujuan: $BUCKET_NAME"
echo ""

# Jalankan MinIO client dalam container

docker run --rm 
--network bigdata-net 
-v "$(pwd)/data:/data" 
minio/mc:latest 
/bin/sh -c "
echo 'Setup koneksi ke MinIO...'
mc alias set $MINIO_ALIAS $MINIO_URL $MINIO_USER $MINIO_PASS;

```
echo 'Membuat bucket jika belum ada...'
mc mb --ignore-existing $MINIO_ALIAS/$BUCKET_NAME;

echo 'Upload semua file .tbl...'
mc cp /data/tpch-csv/*.tbl $MINIO_ALIAS/$BUCKET_NAME/;

echo '';
echo 'List file di bucket:'
mc ls $MINIO_ALIAS/$BUCKET_NAME;
```

"

echo ""
echo "✅ Upload selesai!"
echo "Cek di MinIO Console: http://localhost:9001"
echo "Bucket: $BUCKET_NAME"
