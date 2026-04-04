import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime

# ==========================================
# 1. DB 연결 설정
# ==========================================
DB_PARAMS = {
    "host": "localhost",
    "database": "Jeonse_capstone",
    "user": "postgres",
    "password": "9241",
    "port": "5432"
}


def get_raw_data():
    conn = psycopg2.connect(**DB_PARAMS)

    # 층수, 전용면적, 지하철 거리 등 ML에 필요한 모든 원본 데이터를 DB에서 집계합니다.
    query = """
        SELECT 
            h.house_id,
            h.region_code,
            h.building_type,
            h.build_year,
            h.dist_to_subway,
            h.is_station_area,
            AVG(t.exclusive_area) AS exclusive_area,
            AVG(t.floor) AS floor,
            AVG(CASE WHEN t.deal_type = '매매' THEN t.deal_amount END) AS avg_sale_price,
            AVG(CASE WHEN t.deal_type = '전세' THEN t.deposit END) AS avg_jeonse_deposit,
            COUNT(CASE WHEN t.deal_type = '매매' THEN 1 END) AS sale_tx_count,
            COUNT(CASE WHEN t.deal_type = '전세' THEN 1 END) AS jeonse_tx_count
        FROM houses h
        JOIN transactions t ON h.house_id = t.house_id
        WHERE h.is_station_area IS NOT NULL
        GROUP BY h.house_id, h.region_code, h.building_type, h.build_year, h.dist_to_subway, h.is_station_area
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def assign_risk_label(jeonse_rate):
    if pd.isna(jeonse_rate): return np.nan
    if 50 <= jeonse_rate <= 70:
        return 0
    elif (40 <= jeonse_rate < 50) or (70 < jeonse_rate <= 80):
        return 1
    else:
        return 2


def main():
    print("1. DB에서 원본 데이터를 불러오는 중...")
    df = get_raw_data()

    # 매매가와 전세가 기록이 모두 있는 주택만 필터링
    df_valid = df.dropna(subset=['avg_sale_price', 'avg_jeonse_deposit']).copy()
    print(f"매매/전세 이력이 모두 존재하는 {len(df_valid)}건의 주택으로 피처를 생성합니다.")

    # ==========================================
    # 🌟 피처 엔지니어링 (Feature Engineering)
    # ==========================================
    current_year = datetime.now().year

    # 1. 기본 특성
    df_valid['jeonse_rate'] = (df_valid['avg_jeonse_deposit'] / df_valid['avg_sale_price']) * 100
    df_valid['risk_label'] = df_valid['jeonse_rate'].apply(assign_risk_label)
    df_valid['build_age'] = current_year - df_valid['build_year']
    df_valid['build_age'] = df_valid['build_age'].fillna(df_valid['build_age'].median())

    type_mapping = {'아파트': 0, '연립다세대': 1, '오피스텔': 2, '단독다가구': 3}
    df_valid['building_type_code'] = df_valid['building_type'].map(type_mapping)
    df_valid['total_tx_count'] = df_valid['sale_tx_count'] + df_valid['jeonse_tx_count']

    # 결측치 처리 (층수가 없는 경우 1층으로, 면적이 없는 경우 중앙값으로)
    df_valid['floor'] = df_valid['floor'].fillna(1)
    df_valid['exclusive_area'] = df_valid['exclusive_area'].fillna(df_valid['exclusive_area'].median())

    # 무자본 갭투자 판별기: 갭 절대 금액
    df_valid['gap_amount'] = df_valid['avg_sale_price'] - df_valid['avg_jeonse_deposit']

    # 업계약서 판별기: '유사 조건 그룹' 대비 전세가율 편차
    # (같은 구, 같은 건물 유형, 같은 역세권 여부를 묶어서 평균 전세가율 계산)
    df_valid['group_avg_jeonse_rate'] = df_valid.groupby(['region_code', 'building_type_code', 'is_station_area'])[
        'jeonse_rate'].transform('mean')
    df_valid['jeonse_rate_deviation'] = df_valid['jeonse_rate'] - df_valid['group_avg_jeonse_rate']

    # 결측치 제거 (극소수)
    df_valid = df_valid.dropna()

    # 모델에 들어갈 최종 컬럼 리스트
    ml_columns = [
        'building_type_code', 'build_age', 'exclusive_area', 'floor',
        'dist_to_subway', 'is_station_area',
        'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount',
        'total_tx_count', 'group_avg_jeonse_rate', 'jeonse_rate_deviation',
        'risk_label'  # Target 정답지
    ]

    final_df = df_valid[ml_columns]
    csv_filename = "ml_training_data.csv"
    final_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    print("\n데이터 전처리 및 피처 엔지니어링 완벽하게 완료!")
    print(f"최종 생성된 머신러닝 학습 데이터: {len(final_df)}건")
    print("=" * 40)
    print("타겟 라벨(위험도) 분포:")
    print(final_df['risk_label'].value_counts().sort_index().rename({0: '적정(0)', 1: '주의(1)', 2: '위험(2)'}))
    print("=" * 40)
    print(f"데이터가 {csv_filename} 파일로 저장되었습니다. 이제 인공지능을 학습시킬 준비가 되었습니다!")

if __name__ == "__main__":
    main()