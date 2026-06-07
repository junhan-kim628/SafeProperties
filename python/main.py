import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import psycopg2
import xgboost as xgb
import pandas as pd
import shap
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="전세 사기 예측 대시보드 API")

# 허용할 origin 목록을 환경변수에서 읽음 (콤마 구분)
_raw_origins = os.getenv("ALLOW_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000")
ALLOW_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "Jeonse_capstone"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432"),
}

# 국토부 건축물대장 API
BUILDING_API_KEY = os.getenv(
    "BUILDING_API_KEY",
    "1yxixfObrK/iZ+cdWrx0xzZA8aIl5mifDFcE6rR9yEubodK1qo7WP+zvQbjprnEkBzq/EsVAvv8LbUD9EOCB7g=="
)
BUILDING_API_URL = "http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo"

# AI 모델 로드
model = xgb.XGBClassifier()
try:
    model.load_model("jeonse_risk_model.json")
    print("✅ AI 모델 로드 성공!")
except Exception as e:
    print(f"🚨 모델 로드 실패: {e}")

# SHAP Explainer 초기화 (서버 시작 시 1회)
shap_explainer = None
try:
    shap_explainer = shap.TreeExplainer(model)
    print("✅ SHAP Explainer 초기화 성공!")
except Exception as e:
    print(f"⚠️ SHAP Explainer 초기화 실패: {e}")

# 피처 한글 이름 매핑
FEATURE_NAMES_KR = {
    "building_type_code": "건물 유형",
    "build_age":          "건물 노후도",
    "exclusive_area":     "전용 면적",
    "floor":              "층수",
    "dist_to_subway":     "지하철 거리",
    "is_station_area":    "역세권 여부",
    "avg_sale_price":     "평균 매매가",
    "avg_jeonse_deposit": "평균 전세가",
    "gap_amount":         "갭 금액",
    "total_tx_count":     "거래 건수",
    "group_avg_jeonse_rate":  "지역 전세가율",
    "jeonse_rate_deviation":  "전세가율 편차",
}

# ==========================================
# 🌟 기본 접속 화면 (index.html 렌더링)
# ==========================================
@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>index.html 파일을 찾을 수 없습니다. 파일 위치를 확인해주세요.</h1>"

# [E안] 마커 응답 서버 캐시 — 데이터가 거의 변하지 않으므로 최초 1회만 DB 조회
_markers_cache = None

# ==========================================
# 📍 API 1: 지도 마커 데이터 가져오기 (전세 가격 포함)
# ==========================================
@app.get("/api/markers")
def get_map_markers(limit: int = 50000):
    global _markers_cache

    # [E안] 캐시 히트 시 즉시 반환 (DB·예측 완전 생략)
    if _markers_cache is not None:
        print("✅ 마커 캐시 히트 — DB 조회 생략")
        return _markers_cache

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # [A안] risk_label을 DB에서 직접 읽어 XGBoost 예측 생략 → 서버 처리 시간 단축
        query = """
            SELECT h.house_id, h.building_name, h.jibun_address,
                   ST_X(h.geom) AS lon, ST_Y(h.geom) AS lat,
                   a.risk_label,
                   a.avg_jeonse_deposit
            FROM houses h
            JOIN house_analysis_data a ON h.house_id = a.house_id
            WHERE h.geom IS NOT NULL
            LIMIT %s;
        """
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        markers = [
            {
                "house_id": r[0],
                "building_name": r[1],
                "address": r[2],
                "lon": r[3],
                "lat": r[4],
                "risk": int(r[5]) if r[5] is not None else 0,
                "jeonse": float(r[6]) if r[6] else 0,
            }
            for r in rows
        ]

        _markers_cache = {"status": "success", "data": markers}
        print(f"✅ 마커 {len(markers):,}건 캐시 저장 완료")
        return _markers_cache
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

# ==========================================
# 🧠 API 2: 특정 매물 AI 위험도 예측하기
# ==========================================
@app.get("/api/predict/{house_id}")
def predict_risk(house_id: int):
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']

        # house_id로 해당 매물의 분석 데이터를 직접 조회
        query = f"SELECT {', '.join(features)} FROM house_analysis_data WHERE house_id = %s;"
        cursor.execute(query, (house_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        X_input = pd.DataFrame([dict(zip(features, row))])

        # AI 예측 수행
        predicted_class = int(model.predict(X_input)[0])
        probabilities = model.predict_proba(X_input)[0]
        risk_labels = {0: "적정", 1: "주의", 2: "위험"}

        # SHAP 기여도 계산 (위험 클래스 기준으로 통일)
        # 양수 = 위험을 높이는 요인 / 음수 = 위험을 낮추는 요인
        shap_contributions = []
        if shap_explainer:
            try:
                shap_vals = shap_explainer.shap_values(X_input)
                # shap_vals: list[3] 각 (1, 12) 또는 ndarray (1, 12, 3)
                if isinstance(shap_vals, list):
                    danger_shap = shap_vals[2][0]   # 위험(class 2) 기여도
                else:
                    danger_shap = shap_vals[0, :, 2]

                shap_contributions = sorted(
                    [{"feature": FEATURE_NAMES_KR.get(f, f), "shap": round(float(v), 4)}
                     for f, v in zip(features, danger_shap)],
                    key=lambda x: abs(x["shap"]),
                    reverse=True
                )[:5]
            except Exception as e:
                print(f"SHAP 계산 오류: {e}")

        return {
            "status": "success",
            "house_id": house_id,
            "prediction": risk_labels[predicted_class],
            "probability": {
                "적정": round(float(probabilities[0]) * 100, 2),
                "주의": round(float(probabilities[1]) * 100, 2),
                "위험": round(float(probabilities[2]) * 100, 2)
            },
            "details": X_input.to_dict(orient="records")[0],
            "shap_contributions": shap_contributions
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

# ==========================================
# 📈 API 3: 연도별 매매가·전세가 추이 조회
# ==========================================
@app.get("/api/trend/{house_id}")
def get_price_trend(house_id: int):
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        query = """
            SELECT
                EXTRACT(YEAR FROM deal_date)::int AS year,
                AVG(CASE WHEN deal_type = '매매' THEN deal_amount END) AS avg_sale,
                AVG(CASE WHEN deal_type = '전세' THEN deposit END) AS avg_jeonse
            FROM transactions
            WHERE house_id = %s
            GROUP BY year
            ORDER BY year;
        """
        cursor.execute(query, (house_id,))
        rows = cursor.fetchall()
        return {
            "status": "success",
            "data": [
                {
                    "year": r[0],
                    "avg_sale": float(r[1]) if r[1] else None,
                    "avg_jeonse": float(r[2]) if r[2] else None,
                }
                for r in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

# ==========================================
# 🔍 API 4: 건물명 및 주소 검색 (자동완성용)
# ==========================================
@app.get("/api/search")
def search_buildings(keyword: str = ""):
    # 2글자 미만이면 빈 결과 반환 (서버 부하 방지)
    if not keyword or len(keyword) < 2:
        return {"status": "success", "data": []}

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # 건물명이나 주소에 '검색어'가 포함된 집을 최대 10개만 빠르게 조회
        query = """
            SELECT house_id, building_name, jibun_address, ST_X(geom) as lon, ST_Y(geom) as lat
            FROM houses
            WHERE building_name LIKE %s OR jibun_address LIKE %s
            LIMIT 10;
        """
        search_term = f"%{keyword}%"
        cursor.execute(query, (search_term, search_term))
        rows = cursor.fetchall()

        results = []
        for r in rows:
            results.append({
                "house_id": r[0],
                "building_name": r[1] if r[1] else "이름 없는 주택",
                "address": r[2],
                "lon": r[3],
                "lat": r[4]
            })

        return {"status": "success", "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

# ==========================================
# 🏛️ API 5: 국토부 건축물대장 API 중계
# ==========================================
@app.get("/api/building-info")
def get_building_info(sigungu_cd: str = "", bjdong_cd: str = "", bun: str = "0", ji: str = "0"):
    """국토부 건축물대장 표제부 조회 — 위반건축물 여부, 용도, 가구수, 주차, 엘리베이터"""
    if not sigungu_cd or not bjdong_cd:
        raise HTTPException(status_code=400, detail="sigungu_cd, bjdong_cd는 필수 파라미터입니다.")

    try:
        params = {
            "serviceKey": BUILDING_API_KEY,
            "sigunguCd": sigungu_cd,
            "bjdongCd":  bjdong_cd,
            "bun":       bun.zfill(4),   # 본번 4자리 패딩
            "ji":        ji.zfill(4),    # 부번 4자리 패딩
            "numOfRows": "100",
            "pageNo":    "1",
            "_type":     "json",
        }
        resp = requests.get(BUILDING_API_URL, params=params, timeout=5)
        resp.raise_for_status()
        json_data = resp.json()

        # 응답 구조: response.body.items.item
        body      = json_data.get("response", {}).get("body", {})
        items_obj = body.get("items", {}) or {}
        item_list = items_obj.get("item", [])

        # 단건 응답(dict)을 리스트로 통일
        if isinstance(item_list, dict):
            item_list = [item_list]

        if not item_list:
            return {
                "status": "success",
                "data": {
                    "violation":     "정보 없음",
                    "purpose":       "정보 없음",
                    "household_cnt": "0",
                    "parking_cnt":   "0",
                    "elevator_cnt":  "0",
                    "is_danger":     False,
                }
            }

        # 첫 번째 항목 사용 (표제부 대표 데이터)
        item = item_list[0]
        violation_raw = str(item.get("vltnBldYn", "N")).strip().upper()
        is_danger = violation_raw in ("Y", "1")

        return {
            "status": "success",
            "data": {
                "violation":     "위반건축물" if is_danger else "적법",
                "purpose":       str(item.get("mainPurpsCdNm") or "정보 없음"),
                "household_cnt": str(item.get("hhldCnt") or "0"),
                "parking_cnt":   str(item.get("prkcnt")  or "0"),
                "elevator_cnt":  str(item.get("elvtCnt") or "0"),
                "is_danger":     is_danger,
            }
        }
    except Exception as e:
        # 정부 API가 불안정하거나 키 미등록 시 → 에러를 전파하지 않고 "정보 없음"으로 처리
        # 프론트엔드가 "API 통신 실패" 대신 "정보 없음"을 표시하도록 함
        print(f"⚠️ 건축물대장 API 조회 실패 (sigungu={sigungu_cd}, bjdong={bjdong_cd}): {e}")
        return {
            "status": "success",
            "data": {
                "violation":     "정보 없음",
                "purpose":       "정보 없음",
                "household_cnt": "0",
                "parking_cnt":   "0",
                "elevator_cnt":  "0",
                "is_danger":     False,
            }
        }