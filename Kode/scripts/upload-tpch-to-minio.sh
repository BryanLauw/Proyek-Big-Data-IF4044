#!/bin/bash
set -e
export MSYS_NO_PATHCONV=1

echo "======================================="
echo " Upload TPC-H Data ke MinIO (tpch-raw)"
echo "======================================="

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin123"
LOCAL_DATA_DIR="./data/tpch-csv"
BUCKET_NAME="tpch-raw"

if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "ERROR: Folder $LOCAL_DATA_DIR tidak ditemukan"
    exit 1
fi

echo "Source : $LOCAL_DATA_DIR"
echo "Bucket : $BUCKET_NAME"

docker run --rm \
    --network kode_bigdata-net \
    -v "$(pwd)/data:/data" \
    --entrypoint /bin/sh \
    minio/mc:latest \
    -c "
        mc alias set $MINIO_ALIAS $MINIO_URL $MINIO_USER $MINIO_PASS &&
        mc mb --ignore-existing $MINIO_ALIAS/$BUCKET_NAME &&
        mc cp /data/tpch-csv/*.tbl $MINIO_ALIAS/$BUCKET_NAME/ &&
        echo 'File di bucket:' &&
        mc ls $MINIO_ALIAS/$BUCKET_NAME
    "

echo "Upload selesai. Cek: http://localhost:9001 → bucket $BUCKET_NAME"