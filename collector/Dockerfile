FROM python:3.11-slim

# 작업 디렉터리 설정
WORKDIR /app

# 현재 디렉토리의 모든 파일을 복사하되, Dockerfile 제외
COPY . .

# 필요 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 실행할 스크립트 지정 및 로그 리디렉션
CMD ["python", "main.py"]