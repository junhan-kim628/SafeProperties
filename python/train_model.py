import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import matplotlib.pyplot as plt
import seaborn as sns

# 한글 폰트 깨짐 방지 (윈도우 기준)
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False


def main():
    print("1. 학습 데이터(ml_training_data.csv)를 불러옵니다...")
    try:
        df = pd.read_csv("ml_training_data.csv")
    except FileNotFoundError:
        print("🚨 에러: ml_training_data.csv 파일이 없습니다. 전처리 스크립트를 먼저 실행해주세요.")
        return

    # 2. 사용할 파생 변수(Feature)와 정답지(Target) 분리
    feature_columns = [
        'building_type_code', 'build_age', 'exclusive_area', 'floor',
        'dist_to_subway', 'is_station_area',
        'avg_sale_price', 'avg_jeonse_deposit', 'gap_amount',
        'total_tx_count', 'group_avg_jeonse_rate', 'jeonse_rate_deviation'
    ]

    X = df[feature_columns]
    y = df['risk_label']

    print(f"총 {len(df)}건의 데이터로 학습을 준비합니다.\n")

    # 3. 학습/테스트 데이터 분리
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # 4. XGBoost 모델 정의 및 학습
    print("2. XGBoost 모델 학습을 시작합니다... (잠시만 기다려주세요)")
    model = xgb.XGBClassifier(
        objective='multi:softprob',
        num_class=3,
        eval_metric='mlogloss',
        max_depth=6,
        learning_rate=0.1,
        n_estimators=200,
        random_state=42
    )

    model.fit(X_train, y_train)
    print("✅ 학습 완료!\n")

    # 5. 모델 평가
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    print("=" * 40)
    print(f"🎯 최종 모델 예측 정확도(Accuracy): {accuracy * 100:.2f}%\n")
    print("[ 분류 성능 상세 리포트 ]")
    print(classification_report(y_test, y_pred, target_names=['적정(0)', '주의(1)', '위험(2)']))
    print("=" * 40)

    print("4. 학습된 모델을 저장합니다...")
    model.save_model("jeonse_risk_model.json")
    print("✅ jeonse_risk_model.json 저장 완료!\n")

    # 6. Feature Importance 시각화
    print("5. 변수 중요도(Feature Importance)를 시각화합니다.")
    importance = model.feature_importances_
    fi_df = pd.DataFrame({'Feature': feature_columns, 'Importance': importance})
    fi_df = fi_df.sort_values(by='Importance', ascending=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='Importance', y='Feature', data=fi_df, palette='viridis')
    plt.title('전세 사기 예측 모델 핵심 변수 중요도')
    plt.xlabel('중요도 (기여도)')
    plt.ylabel('파생 변수')
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()