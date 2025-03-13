import os
import pandas as pd

def load_data(data_path):
    """CSV -> Parquet 변환

    주어진 data_path 경로에 있는 data.csv 파일을 읽어,
    같은 경로에 data.parquet 파일로 저장합니다.
    """
    csv_file = os.path.join(data_path, "data.csv")
    parquet_file = os.path.join(data_path, "data.parquet")
    
    print(f"[load_data] Reading CSV from {csv_file}")
    df = pd.read_csv(csv_file)
    
    print(f"[load_data] Converting to Parquet -> {parquet_file}")
    df.to_parquet(parquet_file, index=False)
    print("[load_data] Done.")

def agg_data(data_path):
    """
    Parquet 파일을 불러와서, name과 value별로 그룹바이 후 카운트하여
    결과를 agg.csv로 저장합니다.
    """
    parquet_file = os.path.join(data_path, "data.parquet")
    agg_csv = os.path.join(data_path, "agg.csv")
    
    print(f"[agg_data] Reading Parquet from {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    agg_df = df.groupby(["name", "value"]).size().reset_index(name="count")
    
    print(f"[agg_data] Saving aggregated result -> {agg_csv}")
    agg_df.to_csv(agg_csv, index=False)
    print("[agg_data] Done.")