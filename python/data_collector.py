import requests
import xmltodict
import psycopg2
import time
from datetime import datetime

# ==========================================
# 1. 환경 설정 및 API 정보
# ==========================================
API_KEY = "1yxixfObrK/iZ+cdWrx0xzZA8aIl5mifDFcE6rR9yEubodK1qo7WP+zvQbjprnEkBzq/EsVAvv8LbUD9EOCB7g=="

# 수집할 8개의 API 엔드포인트 목록
ENDPOINTS = {
    # 매매 (Trade)
    "APT_TRADE": "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "RH_TRADE": "http://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "OFFI_TRADE": "http://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "SH_TRADE": "http://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",

    # 전월세 (Rent)
    "APT_RENT": "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "RH_RENT": "http://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "OFFI_RENT": "http://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "SH_RENT": "http://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"
}

# 서울특별시 25개 구 법정동 코드 앞 5자리 (LAWD_CD)
SEOUL_LAWD_CDS = [
    "11110", "11140", "11170", "11200", "11215", "11230", "11260", "11290",
    "11305", "11320", "11350", "11380", "11410", "11440", "11470", "11500",
    "11530", "11545", "11560", "11590", "11620", "11650", "11680", "11710", "11740"
]

# 타겟 기간 (36개월)
TARGET_YMDS = [f"{year}{month:02d}" for year in range(2023, 2026) for month in range(1, 13)]

# PostgreSQL 연결 정보 (본인의 DB 정보로 수정)
DB_PARAMS = {
    "host": "localhost",
    "database": "Jeonse_capstone",
    "user": "postgres",
    "password": "9241",
    "port": "5432"
}


# ==========================================
# 2. 데이터 파싱 및 DB 저장 함수
# ==========================================
def parse_and_save(api_name, items, conn):
    cursor = conn.cursor()

    # API 결과가 1건일 경우 dict로 오고, 여러 건일 경우 list로 옴
    if not isinstance(items, list):
        items = [items]

    for item in items:
        try:
            # 1. 주택 정보 추출 및 정규화
            lawd_cd = item.get('sggCd', '')
            umd_nm = item.get('umdNm', '')
            jibun = item.get('jibun', '')

            # 건물명은 API마다 키값이 다름 (aptNm, mhouseNm, offiNm 등)
            building_name = item.get('aptNm') or item.get('mhouseNm') or item.get('offiNm') or '단독/다가구'

            # 건물 용도 판별
            if 'APT' in api_name:
                building_type = '아파트'
            elif 'RH' in api_name:
                building_type = '연립다세대'
            elif 'OFFI' in api_name:
                building_type = '오피스텔'
            else:
                building_type = '단독다가구'

            build_year = int(item.get('buildYear')) if item.get('buildYear') else None

            # 2. Houses 테이블에 삽입 (이미 존재하면 id만 가져오기)
            # PostgreSQL의 훌륭한 기능인 ON CONFLICT 무시 구문을 사용하여 중복 방지
            insert_house_query = """
                INSERT INTO houses (region_code, jibun_address, building_name, building_type, build_year)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (region_code, jibun_address, building_name) DO NOTHING
                RETURNING house_id;
            """
            cursor.execute(insert_house_query,
                           (lawd_cd, f"{umd_nm} {jibun}".strip(), building_name, building_type, build_year))

            house_id_result = cursor.fetchone()
            if house_id_result:
                house_id = house_id_result[0]
            else:
                # 이미 존재해서 INSERT가 무시된 경우 ID를 다시 조회
                select_house_query = "SELECT house_id FROM houses WHERE region_code=%s AND jibun_address=%s AND building_name=%s"
                cursor.execute(select_house_query, (lawd_cd, f"{umd_nm} {jibun}".strip(), building_name))
                house_id = cursor.fetchone()[0]

            # 3. 거래 내역 추출
            deal_date = f"{item['dealYear']}-{item['dealMonth'].zfill(2)}-{item['dealDay'].zfill(2)}"
            exclusive_area = float(item.get('excluUseAr') or item.get('totalFloorAr') or 0)
            floor = int(item.get('floor')) if item.get('floor') else None

            # 금액 정제 (쉼표 제거 및 만원 단위 -> 원 단위 변환)
            deal_amount = deposit = monthly_rent = 0
            if 'TRADE' in api_name:
                deal_type = '매매'
                deal_amount = int(item.get('dealAmount', '0').replace(',', '')) * 10000
            else:
                monthly_rent_val = int(item.get('monthlyRent', '0').replace(',', ''))
                deal_type = '월세' if monthly_rent_val > 0 else '전세'
                deposit = int(item.get('deposit', '0').replace(',', '')) * 10000
                monthly_rent = monthly_rent_val * 10000

            # 4. Transactions 테이블에 삽입
            insert_tx_query = """
                INSERT INTO transactions (house_id, deal_date, deal_type, deal_amount, deposit, monthly_rent, exclusive_area, floor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_tx_query,
                           (house_id, deal_date, deal_type, deal_amount, deposit, monthly_rent, exclusive_area, floor))

        except Exception as e:
            # 에러 발생 시 현재 트랜잭션을 롤백하여 다음 데이터가 정상적으로 들어갈 수 있게 복구
            conn.rollback()
            print(f"데이터 파싱 에러 발생: {e}")
            continue

    # for문(아이템 반복)이 무사히 다 끝난 후 한 번에 커밋
    conn.commit()


# ==========================================
# 3. 메인 수집 루프
# ==========================================
def main():
    conn = psycopg2.connect(**DB_PARAMS)
    print("DB 연결 성공, 수집을 시작합니다.")

    for api_name, base_url in ENDPOINTS.items():
        print(f"[{api_name}] 데이터 수집 시작...")

        for lawd_cd in SEOUL_LAWD_CDS:
            for ymd in TARGET_YMDS:
                params = {
                    "serviceKey": API_KEY,
                    "LAWD_CD": lawd_cd,
                    "DEAL_YMD": ymd,
                    "numOfRows": "1000",
                    "pageNo": "1"
                }

                try:
                    response = requests.get(base_url, params=params, timeout=10)
                    # XML 응답을 파이썬 딕셔너리로 변환
                    xml_dict = xmltodict.parse(response.content)

                    header = xml_dict['response']['header']
                    if header['resultCode'] == '000':
                        body = xml_dict['response']['body']
                        total_count = int(body['totalCount'])

                        if total_count > 0:
                            items = body['items']['item']
                            parse_and_save(api_name, items, conn)
                            print(f"{lawd_cd}구 / {ymd}월 : {total_count}건 적재 완료")
                        else:
                            print(f"{lawd_cd}구 / {ymd}월 : 거래 건수 없음")
                    else:
                        print(f"API 에러: {header['resultMsg']}")

                except Exception as e:
                    print(f"요청 실패 ({lawd_cd}/{ymd}): {e}")

                # 공공데이터포털 서버 부하 방지 및 차단 예방
                time.sleep(0.5)

    conn.close()
    print("모든 수집이 완료되었습니다!")


if __name__ == "__main__":
    main()