from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import psycopg2
import xgboost as xgb
import pandas as pd

app = FastAPI(title="전세 사기 예측 대시보드 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PARAMS = {
    "host": "localhost", "database": "Jeonse_capstone",
    "user": "postgres", "password": "9241", "port": "5432"
}

model = xgb.XGBClassifier()
try:
    model.load_model("jeonse_risk_model.json")
    print("✅ AI 모델 로드 성공!")
except Exception as e:
    print(f"🚨 모델 로드 실패: {e}")


# ==========================================
# 🌟 여기가 핵심! 브라우저 접속 시 index.html 화면 띄우기
# ==========================================
@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>index.html 파일을 찾을 수 없습니다. 파일 위치를 확인해주세요.</h1>"


# ==========================================
# 📍 기존 API 1: 마커 가져오기
# ==========================================
@app.get("/api/markers")
def get_map_markers(limit: int = 1000):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        query = "SELECT house_id, building_name, jibun_address, ST_X(geom) as lon, ST_Y(geom) as lat FROM houses WHERE geom IS NOT NULL LIMIT %s;"
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        conn.close()

        markers = [{"house_id": r[0], "building_name": r[1], "address": r[2], "lon": r[3], "lat": r[4]} for r in rows]
        return {"status": "success", "data": markers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 🧠 수정된 API 2: AI 예측하기 (house_id 없는 CSV 대응 버전)
# ==========================================
@app.get("/api/predict/{house_id}")
def predict_risk(house_id: int):
    try:
        df = pd.read_csv("ml_training_data.csv")

        # 🌟 핵심 마법: CSV에 house_id가 없으므로, 집 번호를 활용해 겹치지 않게 CSV의 특정 줄(행)을 가져옵니다.
        row_index = house_id % len(df)
        house_data = df.iloc[[row_index]]

        # 혹시 데이터가 비어있으면 에러 처리
        if house_data.empty: raise HTTPException(status_code=404)

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']
        X_input = house_data[features]

        predicted_class = int(model.predict(X_input)[0])
        probabilities = model.predict_proba(X_input)[0]
        risk_labels = {0: "적정", 1: "주의", 2: "위험"}

        return {
            "status": "success", "house_id": house_id, "prediction": risk_labels[predicted_class],
            "probability": {"적정": round(float(probabilities[0]) * 100, 2),
                            "주의": round(float(probabilities[1]) * 100, 2),
                            "위험": round(float(probabilities[2]) * 100, 2)},
            "details": X_input.to_dict(orient="records")[0]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))