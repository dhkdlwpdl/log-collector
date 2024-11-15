import configparser
import logging
import os

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('collector.conf')

log_directory = config['LOGGING']['log_directory']
log_file = config['LOGGING']['log_file']
log_level = config['LOGGING']['log_level']
os.makedirs(log_directory, exist_ok=True)  # 디렉토리가 없으면 생성
logging.basicConfig(
    level=getattr(logging, log_level),  # 로그 레벨 설정
    filename=os.path.join(log_directory, log_file),  # 로그 파일 경로
    filemode='a'  # 파일 모드
)

logger = logging.getLogger()