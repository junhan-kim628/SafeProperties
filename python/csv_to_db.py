import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_db_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'Jeonse_capstone')}"
    )
    return create_engine(url)

def migrate_csv_to_db():
    try:
        print("CSV 파일을 읽어오는 중입니다...")
        df = pd.read_csv("ml_training_data.csv")

        # 3. DB에 테이블 생성 및 데이터 삽입
        # 테이블 이름은 'house_analysis_data'로 설정합니다.
        # if_exists='replace'는 이미 테이블이 있으면 새로 덮어쓴다는 의미입니다.
        print("데이터를 DB로 전송 중입니다. 잠시만 기다려 주세요...")
        engine = get_db_engine()
        df.to_sql('house_analysis_data', engine, if_exists='replace', index=True, index_label='id')

        print("✅ 데이터 마이그레이션 성공! 이제 DB에서 데이터를 불러올 수 있습니다.")
    except Exception as e:
        print(f"❌ 에러 발생: {e}")


if __name__ == "__main__":
    migrate_csv_to_db()