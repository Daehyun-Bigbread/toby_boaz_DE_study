# Kafka 실습: 여러 데이터 소스를 하나로 처리해보기

---

## 🎯 실습 목표

이번 실습의 목표는 Kafka를 이용해
**여러 Producer → 스트림 처리 → 하나의 Consumer** 로 이어지는
**기본적인 스트리밍 파이프라인 구조**를 직접 만들어보는 것이다.

이 실습을 통해 다음을 경험한다.

1. 여러 Producer가 동시에 Kafka로 데이터를 보낼 수 있다
2. Kafka는 중간에서 데이터를 가공하는 역할도 할 수 있다
3. 처리 중 오류가 발생해도 시스템이 멈추지 않게 만들 수 있다

---

## 🧩 실습 시나리오

다음과 같은 상황을 가정한다.

* 웹 서비스에서 사용자 행동 이벤트가 발생한다
* 결제 시스템에서 구매 이벤트가 발생한다
* 두 이벤트는 서로 다른 Topic으로 Kafka에 저장된다
* 중간 스트림 프로세서가 이벤트를 읽어 하나의 형태로 정리한다
* 최종 Consumer는 정리된 이벤트만 받아서 확인한다

---

## 🏗️ 전체 실습 구조

```
[웹 이벤트 Producer] ──▶ web-events ─────┐
                                       ├─▶ stream-processor ──▶ analytics-events ──▶ Consumer
[구매 이벤트 Producer] ─▶ purchase-events ┘
                                       └──────────────▶ analytics-dlq
```

---

## ✅ 1단계: 환경 구성 (Ubuntu 기준)

### 1. 실습 코드 다운로드

```bash
git clone https://github.com/denny221/BOAZ-25-kafka-practice.git
cd BOAZ-25-kafka-practice/
```

---

### 2. Kafka 서버 실행

```bash
docker-compose up -d
docker ps
```

`kafka`, `zookeeper` 컨테이너가 `Up` 상태인지 확인한다.

---

### 3. 파이썬 실행 환경 준비

```bash
python3 -m venv kafka-env

source kafka-env/bin/activate
pip install kafka-python
```

---

## 🚀 2단계: 실습 진행

총 **4개의 터미널**을 사용한다.
모든 터미널에서 아래 명령을 먼저 실행한다.

```bash
cd BOAZ-25-kafka-practice/
source kafka-env/bin/activate
```

---

### 1️⃣ 웹 이벤트 Producer 실행

```bash
python3 producer_web.py
```

* `web-events` 토픽으로 이벤트 전송
* 일부 이벤트는 필드가 누락된 상태로 전송됨

---

### 2️⃣ 구매 이벤트 Producer 실행

```bash
python3 producer_purchase.py
```

* `purchase-events` 토픽으로 이벤트 전송
* 일부 이벤트는 형식이 깨진 상태로 전송됨

---

### 3️⃣ 스트림 프로세서 실행

```bash
python3 stream_processor.py
```

스트림 프로세서의 역할:

* `web-events`, `purchase-events` 두 토픽을 읽는다
* 이벤트를 간단한 공통 포맷으로 변환한다
* 정상 이벤트 → `analytics-events`
* 처리 실패 이벤트 → `analytics-dlq`

📌 이 프로세서는 **Consumer이자 Producer 역할**을 동시에 한다.

---

### 4️⃣ 최종 Consumer 실행

```bash
python3 consumer_analytics.py
```

* `analytics-events` 토픽을 읽는다
* 스트림 프로세서에서 정리된 이벤트만 출력된다

---

## 📊 3단계: 결과 확인

### 1. 파이프라인 흐름 확인

* 두 Producer의 이벤트가 동시에 출력된다
* Consumer는 하나의 토픽만 읽는데도 모든 이벤트를 확인할 수 있다

👉 **Kafka가 중간 처리 파이프라인으로 사용될 수 있음**을 확인

---

### 2. DLQ 동작 확인

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics-dlq \
  --from-beginning
```

* 깨진 이벤트만 DLQ 토픽에 저장됨
* 정상 이벤트는 포함되지 않음

---

## 📝 4단계: 과제

## 🎯 과제 목표

스트림 프로세서 코드(`stream_processor.py`)를 **아주 조금 수정**하여
DLQ로 보내는 메시지를 **조금 더 알아보기 쉽게 만드는 것**이 목표이다.

---

## 📌 과제 요구 사항

### 1️⃣ DLQ 메시지에 한 줄 로그 추가

현재:

* 에러가 발생하면 DLQ로 메시지를 전송함

과제:

* DLQ로 메시지를 보낼 때
  **아래 문구를 출력**하도록 로그를 추가한다.

```text
[DLQ] 처리 실패 이벤트를 analytics-dlq 토픽으로 전송합니다.
```

(👉 `print()` 추가)

---

### 2️⃣ DLQ 메시지 내용 그대로 유지

* 메시지 구조를 바꾸지 않는다
* 추가 가공 ❌
* 새로운 토픽 생성 ❌

👉 **"에러를 만나도 멈추지 않고 DLQ로 보낸다"**는 개념 확인

---

## ✅ 성공 조건

* 스트림 프로세서가 에러 이벤트를 만나도 종료되지 않는다
* 위 DLQ 로그 문구가 출력된다
* `analytics-dlq` 토픽에서 실패 이벤트가 조회된다

---

## 📸 제출물

1. 스트림 프로세서 실행 화면 캡처
   (DLQ 로그가 보이도록)

2. DLQ 토픽 조회 화면 캡처

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics-dlq \
  --from-beginning
```
