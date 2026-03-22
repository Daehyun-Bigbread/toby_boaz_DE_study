# consumer_analytics.py - 최종 분석 Consumer

import json
from kafka import KafkaConsumer

topic_name = 'analytics-events'

print("=" * 60)
print("Analytics Consumer 시작")
print(f"토픽: {topic_name}")
print("스트림 프로세서에서 처리된 이벤트를 수신합니다.")
print("=" * 60)
print()

# Kafka Consumer 설정
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='localhost:9092',
    group_id='analytics-consumer-group',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

try:
    for message in consumer:
        event = message.value
        print(f"📊 [Analytics Event] source={event.get('source')}, "
              f"user_id={event.get('user_id')}, "
              f"partition={message.partition}")
        
        # source에 따라 다른 형식으로 출력
        if event.get('source') == 'web':
            print(f"   🌐 웹 이벤트: {event.get('event_type')}")
        elif event.get('source') == 'purchase':
            print(f"   💰 구매 이벤트: {event.get('product')} - {event.get('amount')}원")
        
        print()

except KeyboardInterrupt:
    print("\nAnalytics Consumer 종료.")
finally:
    consumer.close()
