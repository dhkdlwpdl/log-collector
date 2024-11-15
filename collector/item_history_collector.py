import json
import os
import datetime
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from database_handler import DatabaseHandler
from logging_config import logger

class ItemHistoryCollector:
    def __init__(self, db_handler: DatabaseHandler, config) -> None:
        self.db_handler = db_handler
        self.config = config

    def process(self, item_id):
        table_name = self.config['table_name']
        data_dir = self.config['data_directory'] + item_id

        self._initialize_database(table_name)
        
        self._process_batch(data_dir, table_name, item_id)
        
        self._start_observer(data_dir, table_name, item_id)

    def _initialize_database(self, table_name):
        try:
            # 테이블이 없으면 새로 생성
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    item_id TEXT,
                    created_at TIMESTAMPTZ
                );
            """
            create_hypertable_query = f"SELECT create_hypertable('{table_name}', 'created_at', if_not_exists => TRUE);"
            self.db_handler.execute_query(create_table_query)
            self.db_handler.execute_query(create_hypertable_query)

            os.makedirs('/app/offsets/', exist_ok=True)
        except Exception as e:
            logger.error(f'Failed to initialize database for table {table_name}: {e}')
            raise

    def _process_batch(self, data_dir, table_name, item_id):
        try:
            # 마지막으로 처리된 offset부터 현재 데이터까지 일단 배치 적재 (cdc가 되는 일자와 별개로 수집이 중단된 경우 진행)
            offset_json = Utils.load_offset(item_id)

            files = [filename for filename in os.listdir(data_dir)
                    if filename.endswith('.dat') and int(filename[:-4]) >= max(offset_json.keys())]

            for filename in files:
                date = int(filename[:-4])
                file_path = os.path.join(data_dir, filename)
                logger.debug(f"Processing existing file: {file_path}")

                current_offset = offset_json.get(date, 0)
                data_to_insert = Utils.extract_data_from_file(file_path, current_offset)
                if data_to_insert:
                    query = f"INSERT INTO {table_name} (item_id, created_at) VALUES %s"
                    rows_inserted = self.db_handler.execute_query_batch(query, [(item_id, x) for x in data_to_insert])
                    logger.debug(f'{rows_inserted} rows inserted')
                    current_offset += rows_inserted

                offset_json[date] = current_offset

            Utils.update_offset(item_id, offset_json) # 추가 적재한 데이터 건수에 맞춰서 offset 증가시키기
        except Exception as e:
            logger.error(f'Error occurred while processing batch for item {item_id}: {e}')

    def _start_observer(self, data_dir, table_name, item_id):
        # 파일 시스템 변화 모니터링
        observer = Observer()
        event_handler = FileHandler(self.db_handler, table_name, item_id)
        observer.schedule(event_handler, data_dir, recursive=False)
        observer.start()
        
        try:
            logger.info(f"Monitor for changes in .dat files for {item_id}...")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        except Exception as e:
            logger.error(f'Error occurred in observer: {e}')
            observer.stop()
            raise
        finally:
            self.db_handler.close()
            observer.join()



class FileHandler(FileSystemEventHandler):
    def __init__(self, db_handler, table_name, item_id):
        self.db_handler = db_handler
        self.table_name = table_name
        self.item_id = item_id
        self.tmp_offsets = 0

    def on_modified(self, event):
        offset_json = Utils.load_offset(self.item_id)
        if event.src_path.endswith('.dat'):
            logger.debug(f"Detected change in file: {event.src_path}")

            date = int(os.path.basename(event.src_path)[:-4])
            current_offset = offset_json.get(date, 0)
            data_to_insert = Utils.extract_data_from_file(event.src_path, current_offset)
            
            if data_to_insert:
                query = f"INSERT INTO {self.table_name} (item_id, created_at) VALUES %s"
                try:
                    rows_inserted = self.db_handler.execute_query_batch(query, [(self.item_id, x) for x in data_to_insert])
                    logger.debug(f'{rows_inserted} rows inserted')

                    current_offset += rows_inserted

                    offset_json[date] = current_offset
                except Exception as e:
                    logger.error(f"Error inserting data: {e}\tfile_name: {event.src_path}\tstart_offset: {current_offset}\tend_offset: {current_offset + len(data_to_insert)}")
            
            Utils.update_offset(self.item_id, offset_json)

class Utils:
    @staticmethod
    def _load_offset_json(offset_file):
        if os.path.exists(offset_file):
            try:
                with open(offset_file, 'r') as f:
                    offset_json = {int(key): value for key, value in json.load(f).items()}
                    # 키가 2개보다 많다면 가장 큰 키 기준으로 2개만 남기기
                    if len(offset_json) > 2:
                        sorted_keys = sorted(offset_json.keys(), reverse=True)
                        keys_to_remove = sorted_keys[2:]

                        for key in keys_to_remove:
                            del offset_json[key]

                    return offset_json
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Error loading offset JSON from {offset_file}: {e}")
                raise
        return {0: 0}


    @staticmethod
    def load_offset(item_id):
        offset_file = os.path.join('/app/offsets/', f'offset_{item_id}')
        try:
            return Utils._load_offset_json(offset_file)
        except Exception as e:
            logger.error(f"Failed to load offset for item {item_id}: {e}")
            raise
    
    @staticmethod
    def update_offset(item_id, offset_json):
        offset_file = os.path.join('/app/offsets/' ,f'offset_{item_id}') # offset 정보를 저장하는 파일 이름
        try:
            with open(offset_file, 'w') as f:
                json.dump(offset_json, f)  # 파일에 JSON 형식으로 저장  # 파일 경로와 새로운 위치를 기록 (기존 내용 덮어쓰기)
        except Exception as e:
            logger.error(f"Failed to write offset to {offset_file}: {e}")
            raise


    @staticmethod
    def extract_data_from_file(file_path, offset=0):
        # 파일에서 데이터 추출
        # offset 이후의 데이터만 추출
        data = []
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()  # 모든 라인을 읽어옴

                # offset 이후의 라인 읽기
                for line in lines[offset:]:
                    if not line.strip():  # 빈 줄 pass
                        continue
                    try:
                        created_at = datetime.datetime.strptime(line.strip(), '%Y-%m-%d %H:%M:%S')
                        data.append(created_at)
                    except ValueError:
                        logger.error(f"Skipping malformed line: {line.strip()}")  # 형식이 잘못된 줄 pass
                        
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except IOError as e:
            logger.error(f"Error reading file {file_path}: {e}")
        
        return data