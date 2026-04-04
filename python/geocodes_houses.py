import requests
import psycopg2
import time

# ==========================================
# 1. 설정 정보
# ==========================================
KAKAO_API_KEY = "101580fc28cac2b6309abde0e60aa1d4"

DB_PARAMS = {
    "host": "localhost",
    "database": "Jeonse_capstone",
    "user": "postgres",
    "password": "9241",  # 본인의 DB 비밀번호
    "port": "5432"
}


def get_coordinates(address):
    """카카오 로컬 API를 사용하여 주소를 위경도 좌표로 변환합니다."""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        result = response.json()

        if result['documents']:
            # 가장 정확도가 높은 첫 번째 검색 결과의 x(경도), y(위도)를 가져옵니다.
            lon = float(result['documents'][0]['x'])
            lat = float(result['documents'][0]['y'])
            return lon, lat
        else:
            return None, None
    except Exception as e:
        print(f"API 요청 에러 ({address}): {e}")
        return None, None


def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # 아직 좌표(geom)가 없는 주택 데이터만 불러옵니다.
    # 정확한 검색을 위해 regional_stats의 '서울특별시 OOO구'와 houses의 'OO동 123-4'를 합칩니다.
    select_query = """
        SELECT h.house_id, rs.region_name, h.jibun_address
        FROM houses h
        JOIN regional_stats rs ON h.region_code = rs.region_code
        WHERE h.geom IS NULL;
    """
    cursor.execute(select_query)
    target_houses = cursor.fetchall()
    total_count = len(target_houses)

    print(f"총 {total_count}개의 주택 좌표 변환을 시작합니다...")

    success_count = 0
    fail_count = 0

    for i, (house_id, region_name, jibun_address) in enumerate(target_houses, 1):
        # 예: "서울특별시 강서구" + " " + "화곡동 123-4"
        full_address = f"{region_name} {jibun_address}"

        lon, lat = get_coordinates(full_address)

        if lon and lat:
            # PostGIS 함수를 사용하여 좌표 업데이트
            update_query = """
                UPDATE houses 
                SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                WHERE house_id = %s
            """
            cursor.execute(update_query, (lon, lat, house_id))
            success_count += 1
        else:
            fail_count += 1

        # 100건마다 진행 상황 출력 및 DB Commit (중간에 멈춰도 데이터가 날아가지 않음)
        if i % 100 == 0:
            conn.commit()
            print(f"진행 상황: {i}/{total_count} (성공: {success_count}, 실패: {fail_count})")

        # 카카오 서버에 무리를 주지 않기 위해 아주 짧은 대기 시간 추가
        time.sleep(0.05)

    # 남은 데이터 최종 커밋
    conn.commit()
    conn.close()

    print("\n좌표 변환 작업이 모두 완료되었습니다!")
    print(f"최종 결과 -> 성공: {success_count}건, 실패(검색 불가 주소): {fail_count}건")


if __name__ == "__main__":
    main()