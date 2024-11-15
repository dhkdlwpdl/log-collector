import psycopg2
from psycopg2 import extras
from logging_config import logger

class DatabaseHandler:
    def __init__(self, db_config):
        self.config = db_config
        self.conn = self.connect()
    
    def connect(self):
        try:
            conn = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['name'],
                    user=self.config['user'],
                    password=self.config['password']
                )
            logger.info('Database Connection established successfully!')
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f'Operational error during database connection: {e}')
            raise
        except psycopg2.DatabaseError as e:
            logger.error(f'Database error during connection: {e}')
            raise
        except Exception as e:
            logger.error(f'Failed to open database connection: {e}')
            raise

    def execute_query(self, query, params=None):
        if not self.conn:
            logger.error('No database connection.')
            raise RuntimeError("Database connection not established.")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                self.conn.commit()
                logger.info(f'Query executed successfully: {query}')
        except psycopg2.ProgrammingError as e:
            logger.error(f'Programming error during query execution: {e}')
            self.conn.rollback()
        except psycopg2.DatabaseError as e:
            logger.error(f'Database error during query execution: {e}')
            self.conn.rollback()
        except Exception as e:
            logger.error(f'Failed to execute query: {e}')
            self.conn.rollback()

    def execute_query_batch(self, query, data):
        if not self.conn:
            logger.error('No database connection.')
            raise RuntimeError("Database connection not established.")
        
        try:
            with self.conn.cursor() as cursor:
                extras.execute_values(cursor, query, data)
                self.conn.commit()
                logger.info(f'Batch query executed successfully: {query}')
                return cursor.rowcount
        except psycopg2.ProgrammingError as e:
            logger.error(f'Programming error during batch query execution: {e}')
            self.conn.rollback()
        except psycopg2.DatabaseError as e:
            logger.error(f'Database error during batch query execution: {e}')
            self.conn.rollback()
        except Exception as e:
            logger.error(f'Failed to execute batch query: {e}')
            self.conn.rollback()

    def close(self):
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                logger.info('Database connection closed successfully!')
        except Exception as e:
            logger.error(f'Failed to close database connection: {e}')

    