import json
import time
import random
from kafka import KafkaProducer

# 1. 카프카 프로듀서 설정
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    # 한글 깨짐 방지 및 JSON 직렬화
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# ⚔️ BOAZ 30기 엔지니어링 용사들
users = [
    '이지훈', '강민석', '김대현', '남민서', '박준철', 
    '백지유', '성준영', '손채민', '은비', '이다빈', 
    '이수연', '이주영', '장민주', '장수연', '장은재', 
    '조성빈', '최윤혁', '함하경', '문혁준'
]

# 🪄 취준 & 데이터 엔지니어 맞춤형 스킬셋
skills = [
    '무한 자소서 복붙 📋', 
    '새벽 코딩 카페인 샷 ☕', 
    '파이프라인 무한 루프 🌊', 
    '서류 합격 기원 기도 🛐', 
    '면접관 압박 질문 방어 🛡️',
    '분산 처리 벼락 맞기 ⚡',
    '막학기 학점 방어전 🎓',
    '에러 없는 클린 코드 💎',
    '팀플 버스 하드 캐리 🚌',
    '데이터 유실 복구 🛠️',
    '쿼리 튜닝 한 방 🚀',
    '새벽 3시 디버깅 퀭 👁️'
]

print("==========================================")
print(f"🚀 BOAZ 26기 엔지니어링 레이드 시작!")
print(f"현재 참여 인원: {len(users)}명")
print("==========================================")

try:
    while True:
        # 데이터 생성
        attacker = random.choice(users)
        skill_used = random.choice(skills)
        damage_dealt = random.randint(500, 2500) # 대미지 대폭 상향!
        
        data = {
            "user_id": attacker,
            "skill": skill_used,
            "damage": damage_dealt,
            "ts": time.strftime('%H:%M:%S')
        }
        
        # 2. 카프카 전송 (Topic: boss-attacks)
        # 한글 Key 전송 시 인코딩 주의
        producer.send(
            'boss-attacks', 
            key=attacker.encode('utf-8'), 
            value=data
        )
        
        # 터미널 출력 (시연용)
        print(f"[{data['ts']}] ⚔️ {attacker:.<4} | {skill_used:.<15} | {damage_dealt:,} DMG")
        
        # 공격 속도 (0.7초마다 전송)
        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("\n[INFO] 용사들이 휴식을 취하러 떠났습니다. 레이드 종료.")
    producer.flush() # 남은 메시지 모두 전송
    producer.close()