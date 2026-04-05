from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import psycopg2
import xgboost as xgb
import pandas as pd

app = FastAPI(title="전세 사기 예측 대시보드 API")

# 프론트엔드 통신을 위한 CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🌟 사용자님의 데이터베이스 접속 정보
DB_PARAMS = {
    "host": "localhost",
    "database": "Jeonse_capstone",
    "user": "postgres",
    "password": "9241",
    "port": "5432"
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
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        query = "SELECT house_id, building_name, jibun_address, ST_X(geom) as lon, ST_Y(geom) as lat FROM houses WHERE geom IS NOT NULL LIMIT %s;"
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        df = pd.read_sql("SELECT * FROM house_analysis_data", conn)
        conn.close()

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']

        markers = []
        for r in rows:
            h_id = r[0]
            row_index = h_id % len(df)
            X_input = df.iloc[[row_index]][features]

            pred = int(model.predict(X_input)[0])

            # 프론트엔드 라벨에 띄울 전세 가격 추출
            jeonse_price = float(X_input['avg_jeonse_deposit'].values[0])

            markers.append({
                "house_id": h_id,
                "building_name": r[1],
                "address": r[2],
                "lon": r[3],
                "lat": r[4],
                "risk": pred,
                "jeonse": jeonse_price  # 가격 데이터 추가 전송
            })

        return {"status": "success", "data": markers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# 🧠 API 2: 특정 매물 AI 위험도 예측하기
# ==========================================
@app.get("/api/predict/{house_id}")
def predict_risk(house_id: int):
    try:
        conn = psycopg2.connect(**DB_PARAMS)

        # 마커 API와 동일하게 DB 전체 개수를 기준으로 매칭
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM house_analysis_data;")
        total_rows = cursor.fetchone()[0]

        row_index = house_id % total_rows

        # 매칭된 id의 데이터를 DB에서 정확히 1건 가져오기
        query = "SELECT * FROM house_analysis_data WHERE id = %s;"
        df_row = pd.read_sql(query, conn, params=(row_index,))
        conn.close()

        if df_row.empty:
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']
        X_input = df_row[features]

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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# 🔍 API 3: 건물명 및 주소 검색 (자동완성용)
# ==========================================
@app.get("/api/search")
def search_buildings(keyword: str = ""):
    # 2글자 미만이면 빈 결과 반환 (서버 부하 방지)
    if not keyword or len(keyword) < 2:
        return {"status": "success", "data": []}

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
        conn.close()

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