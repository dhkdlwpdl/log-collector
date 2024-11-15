from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc
import datetime
from config import Config
import logging
from logging.handlers import RotatingFileHandler
import os

# Flask 애플리케이션 설정
app = Flask(__name__)
app.config.from_object(Config)  # Config 객체로부터 설정 로드
db = SQLAlchemy(app)

# 데이터베이스 모델 정의
class ItemLogs(db.Model):
    __tablename__ = Config.ITEM_LOGS_TABLE  # 테이블 이름을 설정 파일에서 가져옴
    item_id = db.Column(db.String, primary_key=True)
    created_at = db.Column(db.DateTime, nullable=False)

# API 1: 아이템들 건수를 기준으로 비율과 랭크를 반환
@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        # item_id로 그룹화하여 각 아이템의 로그 수 계산
        item_logs_count = (
            db.session.query(
                ItemLogs.item_id,
                func.count(ItemLogs.created_at).label('log_count'), # tmp
            )
            .group_by(ItemLogs.item_id)
            .order_by(func.count(ItemLogs.created_at).desc())  # 로그 건수를 기준으로 내림차순 정렬
            .all()  # 모든 결과 가져오기
        )

        # 전체 로그 수 계산
        total_count = sum(item.log_count for item in item_logs_count)

        # 결과를 리스트 형태로 변환하고 비율 추가
        results = [
            {
                'item_id': item.item_id,
                'ratio': item.log_count / total_count if total_count > 0 else 0,
                'log_count': item.log_count  # 로그 수는 임시로 추가
            }
            for item in item_logs_count
        ]

        # 비율 기준으로 순위를 매기기
        rank = 1
        prev_log_count = None
        for index, item in enumerate(results):
            if prev_log_count is None or item['log_count'] < prev_log_count:
                rank = index + 1  # 새로운 순위 부여
            item['rank'] = rank  # 순위 추가
            prev_log_count = item['log_count']  # 이전 로그 수 업데이트

        final_results = [
            {
                'item_id': item['item_id'],
                'ratio': item['ratio'],
                'rank': item['rank']
            }
            for item in results
        ]

        return jsonify(final_results)
    except Exception as e:
        # 데이터베이스 오류 또는 기타 예외에 대한 처리
        app.logger.error(f"Error in get_stats: {e}")
        return jsonify({'error': 'An error occurred while retrieving stats'}), 500


# API 2: 특정 기간의 아이템 건수를 반환
@app.route('/api/count', methods=['GET'])
def get_count():
    item_id = request.args.get('item_id')
    from_date = request.args.get('from')
    to_date = request.args.get('to')

    # 입력 파라미터 검증
    if not item_id or not from_date or not to_date:
        return jsonify({'error': 'Missing parameters'}), 400

    # 날짜 형식 변환
    try:
        from_date = datetime.datetime.fromisoformat(from_date)
        to_date = datetime.datetime.fromisoformat(to_date)
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400

    try:
        item_id = item_id.strip("'")
        # 특정 기간 내의 아이템 건수 계산
        count = (
            db.session.query(func.count(ItemLogs.item_id))  # 아이템 ID의 개수를 계산
            .filter(
                ItemLogs.item_id == item_id,
                ItemLogs.created_at >= from_date,
                ItemLogs.created_at <= to_date
            )
            .scalar() or 0  # None일 경우 0 반환
        )

        final_results = [
            {
                'item_id': item_id,
                'count': count
            }
        ]
        return jsonify(final_results)
    except Exception as e:
        app.logger.error(f"Error in get_count: {e}")
        return jsonify({'error': 'An error occurred while retrieving count'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)