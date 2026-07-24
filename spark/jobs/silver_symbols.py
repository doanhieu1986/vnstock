"""Bronze -> Silver: danh sách mã niêm yết.

Bảng tham chiếu (không có trục thời gian giao dịch) — mỗi symbol chỉ giữ lại
bản ghi mới nhất theo _ingested_at. Tính lại toàn bộ mỗi lần chạy.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "s3a://lakehouse/bronze/symbols/"
SILVER_TABLE = "iceberg.silver.symbols"


def main() -> None:
    spark = SparkSession.builder.appName("silver_symbols").getOrCreate()

    bronze = spark.read.parquet(BRONZE_PATH)

    dedup_window = Window.partitionBy("symbol").orderBy(F.col("_ingested_at").desc())
    silver = (
        bronze.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("symbol"),
            F.col("organ_name"),
            F.col("_source").alias("source"),
            F.col("_ingested_at").alias("ingested_at"),
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    silver.writeTo(SILVER_TABLE).using("iceberg").createOrReplace()

    print(f"==> Da ghi {silver.count()} dong vao {SILVER_TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
