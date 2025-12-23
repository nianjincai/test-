import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# 假設的原始浮動車輛 GPS 數據 (約每分鐘一筆)
data = {
    'timestamp': pd.to_datetime(['2025-12-16 08:00:15', '2025-12-16 08:00:45', '2025-12-16 08:01:30',
                                 '2025-12-16 09:05:00', '2025-12-16 09:05:45', '2025-12-16 09:06:10']),
    'latitude': [25.033, 25.034, 25.035, 25.050, 25.051, 25.052],
    'longitude': [121.565, 121.566, 121.567, 121.580, 121.581, 121.582],
    'speed_kmh': [45, 30, 25, 60, 55, 50],
    'road_segment_id': ['R101', 'R101', 'R101', 'R205', 'R205', 'R205']
}
df_raw = pd.DataFrame(data)
df_raw = df_raw.set_index('timestamp')

# --- 核心程式碼片段：數據聚合與抽樣概念 ---

def aggregate_traffic_data(df: pd.DataFrame, time_interval='1H') -> pd.DataFrame:
    """
    將高頻率交通數據依據時間區間和路段 ID 聚合，計算平均速度。
    """
    # 1. 時間序列重採樣 (抽樣/聚合到小時)
    # 根據 'road_segment_id' 分組，然後對 'speed_kmh' 進行重採樣聚合
    df_agg = df.groupby('road_segment_id')['speed_kmh'].resample(time_interval).mean().reset_index()
    df_agg.rename(columns={'speed_kmh': 'avg_speed_kmh'}, inplace=True)
    
    # 計算該時間區間內的數據點數量 (體現巨量資料處理的樣本數)
    df_count = df.groupby('road_segment_id')['speed_kmh'].resample(time_interval).count().reset_index()
    df_agg['data_points_count'] = df_count['speed_kmh']

    # 數據清洗：移除數據點過少的聚合結果 (簡易抽樣檢查)
    df_agg = df_agg[df_agg['data_points_count'] >= 2]
    
    return df_agg

# 執行聚合
df_hourly_avg = aggregate_traffic_data(df_raw, time_interval='1H')

print("--- 原始數據（部分） ---")
print(df_raw.head())

print("\n--- 聚合後數據 (每小時平均速度) ---")
print(df_hourly_avg)


