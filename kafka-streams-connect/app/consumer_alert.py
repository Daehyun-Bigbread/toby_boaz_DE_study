import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['kafka:9092']

consumer = KafkaConsumer(
    'high-amount-alerts',
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id='alert-monitor-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("[Alert Monitor] 고액 거래 알림 수신 대기 중...")

for message in consumer:
    alert = message.value
    print(
        f"[🚨 ALERT] "
        f"{alert['ts']} | "
        f"{alert['user_id']} | "
        f"{alert['amount']:,}원 | "
        f"{alert['store']}"
    )