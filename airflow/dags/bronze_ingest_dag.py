"""DAG: nạp vnstock -> Bronze hằng ngày sau phiên, sync bảng Trino, rồi build Silver.

Gọi lại CLI/job có sẵn qua BashOperator thay vì viết lại logic:
- scripts/run_bronze_ingest.py, scripts/register_trino_tables.py: tự
  sys.path.insert theo vị trí file nên chạy độc lập bằng full path.
- spark/jobs/silver_*.py: chạy trong container `spark` riêng (không phải
  container Airflow này) -> gọi qua `docker exec`. Cần mount Docker socket
  vào container Airflow (xem docker-compose.yml, service airflow-scheduler).
  Đây là pattern CHỈ DÙNG CHO DEV/LOCAL (xem README mục Orchestration) —
  production nên dùng SparkSubmitOperator/K8s qua network thay vì exec
  thẳng vào container khác.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/vnstock"

with DAG(
    dag_id="bronze_ingest_daily",
    description="Nạp danh sách mã + OHLCV VN30 vào Bronze, sync bảng Trino",
    schedule="30 15 * * 1-5",  # 15:30 giờ VN, thứ 2-6 (sau giờ đóng cửa phiên ~15:00)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["bronze", "vnstock"],
) as dag:
    ingest_symbols = BashOperator(
        task_id="ingest_symbols",
        bash_command=f"python {PROJECT_DIR}/scripts/run_bronze_ingest.py symbols",
    )

    ingest_ohlcv_vn30 = BashOperator(
        task_id="ingest_ohlcv_vn30",
        bash_command=(
            f"python {PROJECT_DIR}/scripts/run_bronze_ingest.py ohlcv --vn30 "
            "--start {{ (data_interval_end - macros.timedelta(days=5)).strftime('%Y-%m-%d') }} "
            "--end {{ data_interval_end.strftime('%Y-%m-%d') }}"
        ),
    )

    register_trino_tables = BashOperator(
        task_id="register_trino_tables",
        bash_command=f"python {PROJECT_DIR}/scripts/register_trino_tables.py",
    )

    spark_silver_symbols = BashOperator(
        task_id="spark_silver_symbols",
        bash_command="docker exec lakehouse-spark /opt/spark/bin/spark-submit /opt/spark-jobs/silver_symbols.py",
    )

    spark_silver_ohlcv = BashOperator(
        task_id="spark_silver_ohlcv",
        bash_command="docker exec lakehouse-spark /opt/spark/bin/spark-submit /opt/spark-jobs/silver_ohlcv.py",
    )

    # register_trino_tables chỉ sync Bronze/Hive, độc lập với Silver/Iceberg
    # -> không cần chờ nhau, chạy song song sau khi ingest xong.
    ingest_symbols >> register_trino_tables
    ingest_ohlcv_vn30 >> register_trino_tables
    ingest_symbols >> spark_silver_symbols
    ingest_ohlcv_vn30 >> spark_silver_ohlcv
