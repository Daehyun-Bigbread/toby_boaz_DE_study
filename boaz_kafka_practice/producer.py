# producer.py
import json
import time
from kafka import KafkaProducer
import random

# 카프카 프로듀서 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 메시지 값을 JSON 형식으로 직렬화합니다.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'live-stream'
print(f"프로듀서 시작. '{topic_name}' 토픽으로 1초마다 메시지를 전송합니다.")
msg_id = 0

try:
    while True:
        message = {
            'message_id': msg_id,
            'data': f'event_data_{random.randint(1, 100)}'
        }
        producer.send(topic_name, value=message)
        print(f"-> 전송: {message}")
        msg_id += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("\n프로듀서 종료.")
finally:
    producer.flush() # 아직 전송되지 않은 메시지가 있다면 모두 전송
    producer.close()