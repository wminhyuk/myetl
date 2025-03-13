import os

def get_data_path(**kwargs) -> str:
    """
    Airflow 컨텍스트의 data_interval_start 값을 이용해
    /home/data/YYYY/MM/DD/HH 경로를 생성합니다.
    """
    base_path = "/home/data"
    execution_time = kwargs["data_interval_start"].in_tz("Asia/Seoul")
    path = os.path.join(
        base_path,
        execution_time.strftime("%Y"),
        execution_time.strftime("%m"),
        execution_time.strftime("%d"),
        execution_time.strftime("%H"),
    )
    return path
