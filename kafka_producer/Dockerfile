# 1. 파이썬 3.11 슬림 버전 (Java 같은 무거운 건 이제 필요 없어요!)
FROM python:3.11-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 루트 폴더에 있는 requirements.txt를 컨테이너로 복사
# (여기에 kafka-python과 rich가 적혀 있어야 합니다!)
COPY requirements.txt .

# 4. 이제 진짜 카프카 라이브러리 설치
RUN pip install --no-cache-dir -r requirements.txt

# 5. 로그가 실시간으로 잘 보이게 설정
ENV PYTHONUNBUFFERED=1



