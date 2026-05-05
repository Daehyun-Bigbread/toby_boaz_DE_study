import json
import time
import random
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC = 'payments'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

users  = ['김철수', '이영희', '박민준', '최서연', '정도윤']
stores = ['스타벅스', '올리브영', '쿠팡', '마켓컬리', '배달의민족']

print("[Producer] 결제 이벤트 생성 시작...")

while True:
    user   = random.choice(users)
    amount = random.randint(3000, 200000)
    data   = {
        "user_id": user,
        "store":   random.choice(stores),
        "amount":  amount,
        "ts":      time.strftime('%H:%M:%S')
    }
    producer.send(TOPIC, key=user.encode('utf-8'), value=data)
    print(f"[SEND] {data['ts']} | {user} | {data['store']} | {amount:,}원")
    time.sleep(0.5)