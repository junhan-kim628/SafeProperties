from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import psycopg2
import xgboost as xgb
import pandas as pd
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

# AI 모델 로드
model = xgb.XGBClassifier()
try:
    model.load_model("jeonse_risk_model.json")
    print("✅ AI 모델 로드 성공!")
except Exception as e:
    print(f"🚨 모델 로드 실패: {e}")

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

# ==========================================
# 📍 API 1: 지도 마커 데이터 가져오기 (전세 가격 포함)
# ==========================================
@app.get("/api/markers")
def get_map_markers(limit: int = 500):
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # houses와 house_analysis_data를 house_id로 JOIN하여 한 번에 조회
        query = """
            SELECT h.house_id, h.building_name, h.jibun_address,
                   ST_X(h.geom) AS lon, ST_Y(h.geom) AS lat,
                   a.building_type_code, a.build_age, a.exclusive_area, a.floor,
                   a.dist_to_subway, a.is_station_area, a.avg_sale_price,
                   a.avg_jeonse_deposit, a.gap_amount, a.total_tx_count,
                   a.group_avg_jeonse_rate, a.jeonse_rate_deviation
            FROM houses h
            JOIN house_analysis_data a ON h.house_id = a.house_id
            WHERE h.geom IS NOT NULL
            LIMIT %s;
        """
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']
        col_names = ['house_id', 'building_name', 'address', 'lon', 'lat'] + features

        markers = []
        for r in rows:
            row = dict(zip(col_names, r))
            X_input = pd.DataFrame([{f: row[f] for f in features}])
            pred = int(model.predict(X_input)[0])
            markers.append({
                "house_id": row['house_id'],
                "building_name": row['building_name'],
                "address": row['address'],
                "lon": row['lon'],
                "lat": row['lat'],
                "risk": pred,
                "jeonse": float(row['avg_jeonse_deposit']) if row['avg_jeonse_deposit'] else 0,
            })

        return {"status": "success", "data": markers}
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

        return {
            "status": "success",
            "house_id": house_id,
            "prediction": risk_labels[predicted_class],
            "probability": {
                "적정": round(float(probabilities[0]) * 100, 2),
                "주의": round(float(probabilities[1]) * 100, 2),
                "위험": round(float(probabilities[2]) * 100, 2)
            },
            "details": X_input.to_dict(orient="records")[0]
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