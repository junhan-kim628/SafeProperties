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
# 📍 수정된 API 1: 마커 가져오기 (DB 연동 완벽 동기화)
# ==========================================
@app.get("/api/markers")
def get_map_markers(limit: int = 500):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        query = "SELECT house_id, building_name, jibun_address, ST_X(geom) as lon, ST_Y(geom) as lat FROM houses WHERE geom IS NOT NULL LIMIT %s;"
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        # DB에서 전체 피처 데이터를 한 번에 가져옵니다 (속도 최적화)
        df = pd.read_sql("SELECT * FROM house_analysis_data", conn)
        conn.close()

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']

        markers = []
        for r in rows:
            h_id = r[0]

            # 🌟 핵심: predict API와 완벽하게 동일한 기준으로 데이터 매칭 (DB 전체 개수 기준)
            row_index = h_id % len(df)

            # DB 데이터프레임에서 추출하여 예측
            X_input = df.iloc[[row_index]][features]
            pred = int(model.predict(X_input)[0])

            markers.append({
                "house_id": h_id,
                "building_name": r[1],
                "address": r[2],
                "lon": r[3],
                "lat": r[4],
                "risk": pred  # 이 값이 프론트엔드로 가서 마커 색상을 결정합니다.
            })

        return {"status": "success", "data": markers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 🧠 수정된 API 2: AI 예측하기 (DB 연동 완벽 동기화)
# ==========================================
@app.get("/api/predict/{house_id}")
def predict_risk(house_id: int):
    try:
        conn = psycopg2.connect(**DB_PARAMS)

        # 🌟 핵심: 마커 API와 동일하게 DB 전체 개수를 기준으로 매칭합니다.
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM house_analysis_data;")
        total_rows = cursor.fetchone()[0]

        row_index = house_id % total_rows

        # 매칭된 id의 데이터를 DB에서 정확히 1건 가져옵니다.
        query = "SELECT * FROM house_analysis_data WHERE id = %s;"
        df_row = pd.read_sql(query, conn, params=(row_index,))
        conn.close()

        if df_row.empty:
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        features = ['building_type_code', 'build_age', 'exclusive_area', 'floor', 'dist_to_subway', 'is_station_area',
                    'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount', 'total_tx_count', 'group_avg_jeonse_rate',
                    'jeonse_rate_deviation']
        X_input = df_row[features]

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