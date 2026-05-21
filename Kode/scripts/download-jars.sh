set -e

JARS_DIR="./spark/jars"
mkdir -p "$JARS_DIR"

echo "Downloading required JARs..."
echo ""

ICEBERG_VERSION="1.4.3"
HADOOP_VERSION="3.3.5"
AWS_SDK_VERSION="1.12.375"
SCALA_VERSION="2.12"

BASE_MVN="https://repo1.maven.org/maven2"

download_jar() {
    local url=$1
    local filename=$(basename "$url")

    if [ -f "$JARS_DIR/$filename" ]; then
        echo "Already exists: $filename"
    else
        echo "Downloading: $filename"
        curl -L -o "$JARS_DIR/$filename" "$url"
        echo "Done: $filename"
    fi
}

echo "Apache Iceberg JARs..."
download_jar "$BASE_MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"

echo ""
echo "AWS S3A / Hadoop JARs..."
download_jar "$BASE_MVN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"
download_jar "$BASE_MVN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

echo ""
echo "Additional dependencies..."
download_jar "$BASE_MVN/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar"

echo ""
echo "Semua JAR berhasil didownload ke: $JARS_DIR"
echo ""
echo "File yang didownload:"
ls -lh "$JARS_DIR"
