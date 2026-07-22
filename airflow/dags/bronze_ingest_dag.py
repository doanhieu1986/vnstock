"""DAG: nạp vnstock -> Bronze hằng ngày sau phiên, rồi sync bảng Trino.

Gọi lại CLI có sẵn (scripts/run_bronze_ingest.py, scripts/register_trino_tables.py)
qua BashOperator thay vì viết lại logic ingest — cả hai script tự sys.path.insert
theo vị trí file nên chạy độc lập bằng full path, không cần chỉnh PYTHONPATH.
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

    [ingest_symbols, ingest_ohlcv_vn30] >> register_trino_tables
