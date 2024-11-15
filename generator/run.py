import time
import random
from datetime import datetime
import os
import logging
import configparser

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('generator.conf')

data_directory = config['DATA']['data_directory']
items = config['DATA']['items'].split(', ')  # 아이템 목록
data_rate_per_second = int(config['DATA']['data_rate_per_second'])


# 로그 설정
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



def generate_data():
    try:
        # 랜덤 데이터 생성
        item_id = random.choice(items)  # A, B, C 중 무작위 선택
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 데이터 생성 시간
        return (item_id, created_at)
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        return None, None  # 예외 발생 시 None 반환

def save_data_to_file(data, file):
    # 데이터 파일에 생성한 데이터 적재
    file.write(data + "\n")

def main():
    logger.info('Start Generating Data ...')
    # 초당 10건의 데이터를 생성 -> ${data_dir}/${item_id}/{yyyymmdd}.dat 구조에 맞게 파일로 저장
    try:
        while True:
            for _ in range(data_rate_per_second):
                item_id, created_at = generate_data()

                # 데이터 생성에 실패했을 경우: 일단 건너뜀
                if item_id is None or created_at is None:
                    continue

                # 디렉터리와 파일 경로 생성
                try:
                    path = os.path.join(data_directory, item_id)
                    os.makedirs(path, exist_ok=True)
                except OSError as e:
                    logger.error(f"Failed to create directory {path}: {e}")
                    continue  # 디렉터리 생성 실패 시 다음 루프로 이동
            
                filename = os.path.join(path, datetime.now().strftime("%Y%m%d") + ".dat")

                try:
                    with open(filename, "a") as file:  # 파일을 자동으로 닫아줌
                        save_data_to_file(created_at, file)
                except IOError as e:
                    logger.error(f"Failed to write to file {filename}: {e}")
            time.sleep(1)  # 1초 대기
    except KeyboardInterrupt:
        print("\Generator Stopped")
    except Exception as e:
        logger.critical(f"Unexpected error happend in data generation loop: {e}")
    finally:
        logger.info('Stopped generating Data!')

if __name__ == "__main__":
    main()