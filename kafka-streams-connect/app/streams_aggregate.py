import json
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = ['kafka:9092']
INPUT_TOPIC       = 'payments'
OUTPUT_TOPIC      = 'user-total-amount'

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id='streams-aggregate-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# State Store: 유저별 누적 금액 및 거래 횟수 저장
state_store = defaultdict(lambda: {"total_amount": 0, "count": 0})

print("[Streams Aggregate] 유저별 누적 결제 금액 집계 시작...")

for message in consumer:
    payment = message.value
    user    = payment['user_id']
    amount  = payment['amount']

    state_store[user]["total_amount"] += amount
    state_store[user]["count"]        += 1

    result = {
        "user_id":      user,
        "total_amount": state_store[user]["total_amount"],
        "count":        state_store[user]["count"],
        "last_payment": amount,
        "ts":           payment['ts']
    }
    producer.send(OUTPUT_TOPIC, key=user.encode('utf-8'), value=result)

    print(f"\n--- 💳 유저별 누적 결제 금액 ({payment['ts']}) ---")
    for u, s in sorted(state_store.items(), key=lambda x: -x[1]["total_amount"]):
        print(f"  {u}: {s['total_amount']:,}원 ({s['count']}회)")
    print("------------------------------------------")