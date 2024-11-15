import configparser
import os
import concurrent.futures
from database_handler import DatabaseHandler
from item_history_collector import ItemHistoryCollector

def main():
    # 설정 파일 로드
    config = configparser.ConfigParser()
    config.read('collector.conf')

    db_config = config['DATABASE']
    db_handler = DatabaseHandler(db_config)

    item_history_collector_config = config['ITEM_HISTORY_COLLECTOR']
    item_history_collector = ItemHistoryCollector(db_handler, item_history_collector_config)

    data_directory = item_history_collector_config['data_directory']
    items = os.listdir(data_directory)
    item_ids = [item for item in items if os.path.isdir(os.path.join(data_directory, item))] # 데이터 경로의 item 이름들 추출


    # item_history_collector.process(item_id=item_ids[0])
    try:
        # 각 파일 경로에 대해 데이터 수집 함수를 병렬로 실행
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(item_history_collector.process, item_ids)

    finally:
        db_handler.close()  # 데이터베이스 핸들러 정리 예시


if __name__ == "__main__":
    main()