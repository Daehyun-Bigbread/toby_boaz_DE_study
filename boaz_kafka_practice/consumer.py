# consumer.py
import json
from kafka import KafkaConsumer
import os

# 각 컨슈머를 구분하기 위해 프로세스 ID를 사용합니다.
consumer_id = os.getpid()
print(f"컨슈머 [{consumer_id}] 시작.")

# 카프카 컨슈머 설정
consumer = KafkaConsumer(
    'live-stream', # 구독할 토픽 이름
    bootstrap_servers='localhost:9092',
    group_id='live-stream-group', # ⭐️ 모든 컨슈머가 동일한 그룹 ID를 가져야 합니다.
    auto_offset_reset='latest',   # 가장 최근 메시지부터 읽기 시작
    # 수신한 메시지 값을 JSON 형식으로 역직렬화합니다.
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

try:
    for message in consumer:
        # 어떤 컨슈머가, 어떤 파티션에서, 어떤 메시지를 받았는지 출력
        print(
            f"[컨슈머 {consumer_id}, "
            f"파티션 {message.partition}] "
            f"수신: {message.value}"
        )
except KeyboardInterrupt:
    print(f"\n컨슈머 [{consumer_id}] 종료.")
finally:
    consumer.close()