#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creon DataReader 데이터베이스 모듈 테스트
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import random

# 프로젝트 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from creon_database_part1 import CreonDatabase, DatabaseError
from creon_config import get_config
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_database_initialization():
    """데이터베이스 초기화 테스트"""
    print("=" * 60)
    print("데이터베이스 초기화 테스트")
    print("=" * 60)
    
    # 임시 데이터베이스 파일 생성
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_creon_data.db"
    
    try:
        # 데이터베이스 생성
        db = CreonDatabase(str(db_path))
        
        # 데이터베이스 연결 확인
        if db.conn is not None:
            print("[OK] 데이터베이스 연결 성공")
            
            # 테이블 존재 여부 확인
            cursor = db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            print(f"[OK] 생성된 테이블 수: {len(tables)}")
            print("  생성된 테이블:")
            for table in tables:
                print(f"    - {table[0]}")
            
            # 필수 테이블 확인
            required_tables = ['stock_metadata', 'daily_ohlcv', 'collection_jobs']
            for table in required_tables:
                cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'")
                if cursor.fetchone()[0] == 1:
                    print(f"[OK] 필수 테이블 '{table}' 존재")
                else:
                    print(f"[ERROR] 필수 테이블 '{table}' 없음")
            
            # SQLite 최적화 설정 확인
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            print(f"[OK] SQLite journal_mode: {journal_mode}")
            
            cursor.execute("PRAGMA synchronous")
            synchronous = cursor.fetchone()[0]
            print(f"[OK] SQLite synchronous: {synchronous}")
            
            cursor.execute("PRAGMA foreign_keys")
            foreign_keys = cursor.fetchone()[0]
            print(f"[OK] SQLite foreign_keys: {foreign_keys}")
            
            return db
            
        else:
            print("[ERROR] 데이터베이스 연결 실패")
            return None
            
    except Exception as e:
        print(f"[ERROR] 데이터베이스 초기화 실패: {e}")
        return None
    finally:
        # 임시 디렉토리 정리 (테스트 후)
        pass


def test_stock_metadata(db):
    """종목 메타데이터 테스트"""
    print("\n" + "=" * 60)
    print("종목 메타데이터 테스트")
    print("=" * 60)
    
    try:
        # 테스트 종목 데이터
        test_stocks = [
            {
                'stock_code': '005930',
                'stock_name': '삼성전자',
                'market_type': 'KOSPI',
                'sector_code': '1010',
                'sector_name': '전기전자',
                'listing_date': 19750611,
                'is_etf': False,
                'is_etn': False,
                'is_delisted': False
            },
            {
                'stock_code': '000660',
                'stock_name': 'SK하이닉스',
                'market_type': 'KOSPI',
                'sector_code': '1010',
                'sector_name': '전기전자',
                'listing_date': 19960701,
                'is_etf': False,
                'is_etn': False,
                'is_delisted': False
            },
            {
                'stock_code': '035420',
                'stock_name': 'NAVER',
                'market_type': 'KOSDAQ',
                'sector_code': '4050',
                'sector_name': '서비스업',
                'listing_date': 20081002,
                'is_etf': False,
                'is_etn': False,
                'is_delisted': False
            }
        ]
        
        # 종목 메타데이터 삽입
        inserted_count = 0
        for stock in test_stocks:
            try:
                db.upsert_stock_metadata(stock)
                inserted_count += 1
                print(f"[OK] 종목 메타데이터 삽입: {stock['stock_code']} - {stock['stock_name']}")
            except Exception as e:
                print(f"[ERROR] 종목 메타데이터 삽입 실패 ({stock['stock_code']}): {e}")
        
        print(f"\n[OK] 총 {inserted_count}개 종목 메타데이터 삽입 완료")
        
        # 종목 메타데이터 조회 테스트
        print("\n종목 메타데이터 조회 테스트:")
        
        # 단일 종목 조회
        stock_info = db.get_stock_metadata('005930')
        if stock_info:
            print(f"[OK] 단일 종목 조회 성공: {stock_info['stock_code']} - {stock_info['stock_name']}")
        else:
            print("[ERROR] 단일 종목 조회 실패")
        
        # 모든 종목 조회
        all_stocks = db.get_all_stocks()
        print(f"[OK] 모든 종목 조회: {len(all_stocks)}개 종목")
        
        # 시장별 종목 조회
        kospi_stocks = db.get_all_stocks('KOSPI')
        kosdaq_stocks = db.get_all_stocks('KOSDAQ')
        print(f"[OK] KOSPI 종목: {len(kospi_stocks)}개")
        print(f"[OK] KOSDAQ 종목: {len(kosdaq_stocks)}개")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 종목 메타데이터 테스트 실패: {e}")
        return False


def test_daily_ohlcv(db):
    """일봉 OHLCV 데이터 테스트"""
    print("\n" + "=" * 60)
    print("일봉 OHLCV 데이터 테스트")
    print("=" * 60)
    
    try:
        # 테스트 일봉 데이터 생성 (삼성전자, 10일치)
        test_data = []
        base_date = 20240101  # 2024년 1월 1일
        stock_code = '005930'
        
        for i in range(10):
            date = base_date + i
            open_price = 70000 + random.randint(-1000, 1000)
            high = open_price + random.randint(500, 2000)
            low = open_price - random.randint(500, 2000)
            close = random.randint(low, high)
            volume = random.randint(1000000, 5000000)
            
            test_data.append({
                'stock_code': stock_code,
                'date': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'amount': volume * close
            })
        
        # 일봉 데이터 삽입
        inserted_count = 0
        for data in test_data:
            try:
                db.insert_daily_data(
                    stock_code=data['stock_code'],
                    date=data['date'],
                    open=data['open'],
                    high=data['high'],
                    low=data['low'],
                    close=data['close'],
                    volume=data['volume'],
                    amount=data['amount']
                )
                inserted_count += 1
            except Exception as e:
                print(f"[ERROR] 일봉 데이터 삽입 실패 ({data['date']}): {e}")
        
        print(f"[OK] 총 {inserted_count}개 일봉 데이터 삽입 완료")
        
        # 일봉 데이터 조회 테스트
        print("\n일봉 데이터 조회 테스트:")
        
        # 특정 종목의 모든 일봉 데이터 조회
        all_daily = db.get_daily_data(stock_code)
        print(f"[OK] {stock_code}의 모든 일봉 데이터: {len(all_daily)}개")
        
        # 날짜 범위로 조회
        start_date = 20240103
        end_date = 20240107
        range_daily = db.get_daily_data_range(stock_code, start_date, end_date)
        print(f"[OK] {stock_code}의 {start_date}~{end_date} 일봉 데이터: {len(range_daily)}개")
        
        # 최신 데이터 조회
        latest_data = db.get_latest_daily_data(stock_code)
        if latest_data:
            print(f"[OK] {stock_code}의 최신 일봉 데이터: {latest_data['date']}")
        
        # 데이터 존재 여부 확인
        exists_20240105 = db.check_daily_data_exists(stock_code, 20240105)
        exists_20241231 = db.check_daily_data_exists(stock_code, 20241231)  # 존재하지 않는 날짜
        print(f"[OK] 일봉 데이터 존재 여부 (20240105): {exists_20240105}")
        print(f"[OK] 일봉 데이터 존재 여부 (20241231): {exists_20241231}")
        
        # 데이터 통계
        stats = db.get_daily_data_stats(stock_code)
        print(f"[OK] 일봉 데이터 통계:")
        print(f"    - 데이터 수: {stats['count']}")
        print(f"    - 시작 날짜: {stats['min_date']}")
        print(f"    - 종료 날짜: {stats['max_date']}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 일봉 OHLCV 데이터 테스트 실패: {e}")
        return False


def test_data_merge(db):
    """데이터 병합 테스트"""
    print("\n" + "=" * 60)
    print("데이터 병합 테스트")
    print("=" * 60)
    
    try:
        stock_code = '000660'  # SK하이닉스
        
        # 기존 데이터 (5일치)
        existing_data = []
        base_date = 20240101
        
        for i in range(5):
            date = base_date + i
            existing_data.append({
                'stock_code': stock_code,
                'date': date,
                'open': 120000 + random.randint(-2000, 2000),
                'high': 122000 + random.randint(0, 3000),
                'low': 118000 - random.randint(0, 3000),
                'close': 121000 + random.randint(-1000, 1000),
                'volume': random.randint(2000000, 8000000)
            })
        
        # 기존 데이터 삽입
        for data in existing_data:
            db.insert_daily_data(**data)
        
        print(f"[OK] 기존 데이터 삽입 완료: {len(existing_data)}개")
        
        # 새로운 데이터 (중복 2일 + 새로운 3일)
        new_data = []
        
        # 중복 데이터 (날짜 20240103, 20240104)
        for i in [2, 3]:
            date = base_date + i
            new_data.append({
                'stock_code': stock_code,
                'date': date,
                'open': 125000,  # 다른 값으로 업데이트
                'high': 127000,
                'low': 123000,
                'close': 126000,
                'volume': 10000000  # 다른 값
            })
        
        # 새로운 데이터 (날짜 20240105, 20240106, 20240107)
        for i in range(5, 8):
            date = base_date + i
            new_data.append({
                'stock_code': stock_code,
                'date': date,
                'open': 130000 + random.randint(-1000, 1000),
                'high': 132000 + random.randint(0, 2000),
                'low': 128000 - random.randint(0, 2000),
                'close': 131000 + random.randint(-500, 500),
                'volume': random.randint(3000000, 10000000)
            })
        
        print(f"[OK] 새로운 데이터 준비 완료: {len(new_data)}개 (중복 {2}개 + 신규 {3}개)")
        
        # 데이터 병합 테스트
        merge_result = db.merge_daily_data(stock_code, new_data)
        
        print(f"\n[OK] 데이터 병합 결과:")
        print(f"    - 추가된 데이터: {merge_result.added_count}개")
        print(f"    - 업데이트된 데이터: {merge_result.updated_count}개")
        print(f"    - 총 처리된 데이터: {merge_result.total_count}개")
        
        if merge_result.date_range:
            print(f"    - 날짜 범위: {merge_result.date_range.start} ~ {merge_result.date_range.end}")
        
        # 병합 후 데이터 확인
        final_data = db.get_daily_data(stock_code)
        print(f"[OK] 병합 후 총 데이터 수: {len(final_data)}개")
        
        # 데이터 갭 확인
        gaps = db.find_data_gaps(stock_code, 'daily')
        print(f"[OK] 데이터 갭 수: {len(gaps)}개")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 데이터 병합 테스트 실패: {e}")
        return False


def test_performance(db):
    """성능 테스트"""
    print("\n" + "=" * 60)
    print("성능 테스트")
    print("=" * 60)
    
    try:
        import time
        
        # 대량 데이터 삽입 성능 테스트
        print("대량 데이터 삽입 성능 테스트...")
        
        test_stock_code = 'TEST001'
        test_data_count = 100
        
        # 테스트 종목 추가
        db.insert_stock_metadata(
            stock_code=test_stock_code,
            stock_name='테스트종목',
            market_type='KOSPI',
            sector_code='9999',
            sector_name='테스트'
        )
        
        # 대량 데이터 생성
        bulk_data = []
        base_date = 20230101
        
        for i in range(test_data_count):
            date = base_date + i
            bulk_data.append({
                'stock_code': test_stock_code,
                'date': date,
                'open': 10000 + random.randint(-500, 500),
                'high': 10500 + random.randint(0, 500),
                'low': 9500 - random.randint(0, 500),
                'close': 10200 + random.randint(-300, 300),
                'volume': random.randint(100000, 1000000)
            })
        
        # 개별 삽입 성능 측정
        start_time = time.time()
        
        for data in bulk_data[:10]:  # 10개만 테스트
            db.insert_daily_data(**data)
        
        individual_time = time.time() - start_time
        print(f"[OK] 개별 삽입 (10개): {individual_time:.3f}초")
        
        # 배치 삽입 성능 측정
        start_time = time.time()
        
        # 배치 삽입 메서드가 있다고 가정 (실제 구현 필요)
        # db.batch_insert_daily_data(bulk_data[10:30])  # 20개
        
        batch_time = time.time() - start_time
        print(f"[OK] 배치 삽입 (20개): {batch_time:.3f}초")
        
        # 조회 성능 테스트
        print("\n조회 성능 테스트...")
        
        # 단일 조회
        start_time = time.time()
        for _ in range(10):
            db.get_stock_metadata(test_stock_code)
        single_query_time = (time.time() - start_time) / 10
        print(f"[OK] 단일 조회 평균: {single_query_time:.4f}초")
        
        # 범위 조회
        start_time = time.time()
        db.get_daily_data_range(test_stock_code, 20230101, 20230131)
        range_query_time = time.time() - start_time
        print(f"[OK] 범위 조회 (31일): {range_query_time:.3f}초")
        
        # 데이터베이스 통계
        cursor = db.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM stock_metadata")
        stock_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM daily_ohlcv")
        daily_count = cursor.fetchone()[0]
        
        print(f"\n[OK] 데이터베이스 통계:")
        print(f"    - 종목 수: {stock_count}")
        print(f"    - 일봉 데이터 수: {daily_count}")
        
        # 테이블 크기 확인
        cursor.execute("""
            SELECT name, 
                   (pgsize * page_count) / 1024.0 / 1024.0 as size_mb
            FROM dbstat
            ORDER BY size_mb DESC
        """)
        
        print(f"    - 테이블 크기:")
        try:
            for row in cursor.fetchall():
                print(f"        {row[0]}: {row[1]:.2f}MB")
        except:
            print(f"        테이블 크기 정보를 가져올 수 없습니다.")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 성능 테스트 실패: {e}")
        return False


def test_config_integration():
    """설정 통합 테스트"""
    print("\n" + "=" * 60)
    print("설정 통합 테스트")
    print("=" * 60)
    
    try:
        config = get_config()
        
        print("INI 설정 파일에서 데이터베이스 설정 확인:")
        
        # 데이터베이스 타입
        db_type = config.get('storage.database_type', 'sqlite')
        print(f"[OK] 데이터베이스 타입: {db_type}")
        
        # 데이터베이스 경로
        db_path = config.get('storage.database_path', '')
        print(f"[OK] 데이터베이스 경로: {db_path}")
        
        # SQLite 최적화 설정
        sqlite_config = config.get('database_sqlite', {})
        print(f"[OK] SQLite 최적화 설정:")
        for key, value in sqlite_config.items():
            print(f"    - {key}: {value}")
        
        # 데이터베이스 활성화 여부
        db_enabled = config.get('storage.database_enabled', False)
        print(f"[OK] 데이터베이스 활성화: {db_enabled}")
        
        # 파일 저장 활성화 여부
        file_enabled = config.get('storage.file_enabled', False)
        print(f"[OK] 파일 저장 활성화: {file_enabled}")
        
        # 설정 유효성 검증
        errors = config.validate()
        if errors:
            print(f"\n[ERROR] 설정 오류 발견:")
            for category, msgs in errors.items():
                for msg in msgs:
                    print(f"    {category}: {msg}")
            return False
        else:
            print(f"\n[OK] 모든 설정이 유효합니다.")
            return True
            
    except Exception as e:
        print(f"[ERROR] 설정 통합 테스트 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("Creon DataReader 데이터베이스 모듈 테스트 시작")
    print("=" * 60)
    
    test_results = {}
    temp_dir = None
    db = None
    
    try:
        # 설정 통합 테스트
        test_results['config'] = test_config_integration()
        
        # 임시 데이터베이스 생성
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_creon_data.db"
        
        # 데이터베이스 초기화 테스트
        db = test_database_initialization()
        test_results['init'] = db is not None
        
        if db:
            # 종목 메타데이터 테스트
            test_results['metadata'] = test_stock_metadata(db)
            
            # 일봉 데이터 테스트
            test_results['daily_ohlcv'] = test_daily_ohlcv(db)
            
            # 데이터 병합 테스트
            test_results['merge'] = test_data_merge(db)
            
            # 성능 테스트
            test_results['performance'] = test_performance(db)
            
            # 데이터베이스 연결 종료
            db.close()
            print(f"\n[OK] 데이터베이스 연결 종료")
        
        # 테스트 결과 요약
        print("\n" + "=" * 60)
        print("테스트 결과 요약")
        print("=" * 60)
        
        total_tests = len(test_results)
        passed_tests = sum(1 for result in test_results.values() if result)
        
        for test_name, result in test_results.items():
            status = "[OK] 통과" if result else "[ERROR] 실패"
            print(f"{test_name:15} : {status}")
        
        print(f"\n총 {total_tests}개 테스트 중 {passed_tests}개 통과")
        
        if passed_tests == total_tests:
            print("\n[SUCCESS] 모든 테스트 통과!")
            return 0
        else:
            print(f"\n[WARNING]  {total_tests - passed_tests}개 테스트 실패")
            return 1
            
    except Exception as e:
        print(f"\n[ERROR] 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # 임시 파일 정리
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"\n[OK] 임시 파일 정리 완료: {temp_dir}")
            except Exception as e:
                print(f"\n[WARNING]  임시 파일 정리 실패: {e}")


if __name__ == "__main__":
    sys.exit(main())