import json
import sys
from kafka import KafkaConsumer
from kafka import TopicPartition

TOPIC = 'boss-attacks'

# 파티션 번호를 인자로 받음 (예: python consumer_logger.py 0)
if len(sys.argv) > 1:
    partition = int(sys.argv[1])
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka:9092'],
        group_id=f'log-storage-group-p{partition}',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.assign([TopicPartition(TOPIC, partition)])
    print(f"[INFO] PARTITION {partition} 전용 로거 시작")
else:
    # 인자 없으면 전체 파티션 구독 (기존 동작)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=['kafka:9092'],
        group_id='log-storage-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("[INFO] 모든 공격 로그를 백업 저장합니다 (Group: log-storage-group)")

for message in consumer:
    log = message.value
    print(f"[PARTITION {message.partition}] {log['ts']} | {log['user_id']} | {log['damage']} 대미지 기록 완료.")
