"""Bronze -> Silver: OHLCV.

Đọc toàn bộ Bronze (mọi ingest_date), khử trùng lặp theo (symbol, time) —
cần thiết vì Airflow DAG ingest cửa sổ trailing 5 ngày mỗi lần chạy nên cùng
1 giao dịch có thể nằm ở nhiều ingest_date khác nhau — giữ bản ghi có
_ingested_at mới nhất. Tính lại toàn bộ mỗi lần chạy (createOrReplace):
dữ liệu còn nhỏ nên full recompute đơn giản và đúng hơn logic merge/upsert.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "s3a://lakehouse/bronze/ohlcv/"
SILVER_TABLE = "iceberg.silver.ohlcv"


def main() -> None:
    spark = SparkSession.builder.appName("silver_ohlcv").getOrCreate()

    bronze = spark.read.parquet(BRONZE_PATH)

    dedup_window = Window.partitionBy("symbol", "time").orderBy(F.col("_ingested_at").desc())
    silver = (
        bronze.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("symbol"),
            F.col("time"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("_source").alias("source"),
            F.col("_ingested_at").alias("ingested_at"),
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    silver.writeTo(SILVER_TABLE).using("iceberg").partitionedBy("symbol").createOrReplace()

    print(f"==> Da ghi {silver.count()} dong vao {SILVER_TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
