import os
import logging

# 설정 클래스
class Config:
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI', 'postgresql+psycopg2://yj:yj123!@timescaledb:5432/items')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ITEM_LOGS_TABLE = os.environ.get('ITEM_LOGS_TABLE', 'item_creation_logs')