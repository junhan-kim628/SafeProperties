import pandas as pd
import psycopg2

DB_PARAMS = {
    "host": "localhost", "database": "Jeonse_capstone",
    "user": "postgres", "password": "9241", "port": "5432"
}

def main():
    # 1. CSV 파일 읽기 (공공데이터 특성상 인코딩 오류 방지를 위해 예외처리 추가)
    try:
        df = pd.read_csv('subway.csv', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv('subway.csv', encoding='cp949')

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # 2. 데이터 DB 삽입
    for index, row in df.iterrows():
        station_name = row['역사명']
        line_name = row['호선']
        lat = row['위도']
        lon = row['경도']

        # PostGIS의 ST_MakePoint 함수를 사용해 경도, 위도를 Geometry 타입으로 변환
        insert_query = """
            INSERT INTO subway_stations (station_name, line_name, geom)
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        """
        cursor.execute(insert_query, (station_name, line_name, lon, lat))

    conn.commit()
    conn.close()
    print("지하철역 데이터가 성공적으로 DB에 저장되었습니다!")


if __name__ == "__main__":
    main()