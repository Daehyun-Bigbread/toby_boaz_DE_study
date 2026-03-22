# producer_purchase.py - 구매 이벤트 Producer

import json
import time
from kafka import KafkaProducer
import random

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 일부러 잘못된 형식도 보낼 수 있도록 문자열로 직렬화
    value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else json.dumps(v).encode('utf-8')
)

topic_name = 'purchase-events'
purchase_id = 0

print(f"구매 이벤트 Producer 시작. '{topic_name}' 토픽으로 이벤트를 전송합니다.")
print("일부 이벤트는 의도적으로 JSON 형식이 깨져 있습니다.\n")

try:
    while True:
        # 4번에 1번은 깨진 JSON 형식으로 전송
        if purchase_id % 4 == 0:
            # 의도적으로 JSON 형식을 깨뜨림 (닫는 괄호 누락)
            message = '{"purchase_id": ' + str(purchase_id) + ', "amount": 5000, "product": "invalid_json"'
            print(f"💥 깨진 JSON 전송: {message}")
        else:
            # 정상 이벤트
            message = {
                'purchase_id': purchase_id,
                'user_id': f'user_{random.randint(1, 100)}',
                'product': random.choice(['laptop', 'mouse', 'keyboard', 'monitor']),
                'amount': random.randint(100, 5000) * 100,
                'timestamp': time.time()
            }
            print(f"✅ 정상 구매 이벤트: {message}")
        
        producer.send(topic_name, value=message)
        producer.flush()
        purchase_id += 1
        time.sleep(3)

except KeyboardInterrupt:
    print("\n구매 이벤트 Producer 종료.")
finally:
    producer.close()
