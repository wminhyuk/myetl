from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator

import os
import pendulum


# 분리한 모듈에서 필요한 함수들을 임포트합니다.
from myetl.utils import get_data_path
from myetl.data_processing import load_data, agg_data

with DAG(
    dag_id="myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # make_data: bash 스크립트를 실행하여 CSV 파일 생성
    make_data = BashOperator(
        task_id="make_data",
        requirements=["git+https://github.com/wminhyuk/myetl.git@main"],
        bash_command=(
            "bash /home/airflow/make_data.sh "
            "/home/data/{{ data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH') }}"
        )
    )
    
    # load_data: 분리된 함수를 호출하여 CSV -> Parquet 변환
    load_data_task = PythonVirtualenvOperator(
        task_id="load_data",
        requirements=["git+https://github.com/wminhyuk/myetl.git@main"],
        python_callable=lambda **kwargs: load_data(get_data_path(**kwargs)),
        provide_context=True,
    )
    
    # agg_data: 분리된 함수를 호출하여 Parquet 파일 집계 후 agg.csv 저장
    agg_data_task = PythonVirtualenvOperator(
        task_id="agg_data",
        requirements=["git+https://github.com/wminhyuk/myetl.git@main"],
        python_callable=lambda **kwargs: agg_data(get_data_path(**kwargs)),
        provide_context=True,
    )
    
    # 태스크 간 의존성 설정
    start >> make_data >> load_data_task >> agg_data_task >> end