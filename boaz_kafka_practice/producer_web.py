# producer_web.py - 웹 이벤트 Producer

import json
import time
from kafka import KafkaProducer
import random

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'web-events'
event_id = 0

print(f"웹 이벤트 Producer 시작. '{topic_name}' 토픽으로 이벤트를 전송합니다.")
print("일부 이벤트는 의도적으로 필드가 누락됩니다.\n")

try:
    while True:
        # 5번에 1번은 필드가 누락된 불완전한 이벤트 전송
        if event_id % 5 == 0:
            # user_id 필드가 누락됨
            message = {
                'event_id': event_id,
                'event_type': 'page_view',
                'timestamp': time.time()
            }
            print(f"⚠️  불완전 이벤트 전송: {message}")
        else:
            # 정상 이벤트
            message = {
                'event_id': event_id,
                'event_type': random.choice(['page_view', 'click', 'scroll']),
                'user_id': f'user_{random.randint(1, 100)}',
                'timestamp': time.time()
            }
            print(f"✅ 정상 이벤트 전송: {message}")
        
        producer.send(topic_name, value=message)
        producer.flush()
        event_id += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\n웹 이벤트 Producer 종료.")
finally:
    producer.close()
