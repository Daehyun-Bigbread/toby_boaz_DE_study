from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import random

# 1. 기본 설정
default_args = {
    'owner': 'BOAZ',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 2. DAG 정의
with DAG(
    dag_id='BOAZ_simple_etl_practice',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@once", # 한 번만 실행 (실습용)
    catchup=False,
    tags=['BOAZ', 'practice']
) as dag:

    # [STEP 1] 데이터 추출 (Extract)
    @task
    def extract_member_data():
        members = ["보아즈", "데이터", "엔지니어링"]
        selected = random.choice(members)
        base_score = random.randint(50, 100)
        print(f"📥 수집 완료: {selected} (기본 점수: {base_score})")
        return {"name": selected, "score": base_score}

    # [STEP 2] 데이터 가공 (Transform) - XCom으로 데이터 전달받음
    @task
    def transform_boost_score(member_info):
        # BOAZ 활동 가산점 +20점 부여

        if member_info['name'] == '보아즈':
            bonus = 50
        else:
            bonus = 20

        boosted_score = member_info['score'] + bonus
        result = {
            "name": member_info['name'],
            "final_score": boosted_score,
            "is_vip": boosted_score >= 100
        }
        print(f"가공 완료!: {result}")
        return result

    # [STEP 3] 데이터 적재/출력 (Load)
    @task
    def load_result(final_data):
        name = final_data['name']
        score = final_data['final_score']
        status = "🔥🐘 우수 활동자" if final_data['is_vip'] else "✨🐘 일반 활동자"
        print(f"💾 [결과] {name}님 / 최종 점수: {score} / 등급: {status}")

    # 3. 흐름 연결 (Dependency)
    raw_data = extract_member_data()
    processed_data = transform_boost_score(raw_data)
    load_result(processed_data)