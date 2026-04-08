import json
from kafka import KafkaConsumer

# Group ID가 다르기 때문에 dps-monitor-group과 독립적으로 움직임
consumer = KafkaConsumer(
    'boss-attacks',
    bootstrap_servers=['kafka:9092'],
    group_id='log-storage-group', 
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("[INFO] 모든 공격 로그를 백업 저장합니다 (Group: log-storage-group)")

for message in consumer:
    log = message.value
    # 파일에 쓰는 대신 터미널 출력으로 대체
    print(f"[BACKUP] {log['ts']} | {log['user_id']} | {log['damage']} 대미지 기록 완료.")