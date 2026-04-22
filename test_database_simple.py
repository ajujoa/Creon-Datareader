#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creon DataReader 데이터베이스 모듈 간단 테스트
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

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


def test_database_basic():
    """데이터베이스 기본 기능 테스트"""
    print("=" * 60)
    print("데이터베이스 기본 기능 테스트")
    print("=" * 60)
    
    # 임시 데이터베이스 파일 생성
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_creon_data.db"
    
    try:
        # 데이터베이스 생성
        print(f"데이터베이스 생성: {db_path}")
        db = CreonDatabase(str(db_path))
        
        # 데이터베이스 연결 확인
        if db.conn is not None:
            print("[OK] 데이터베이스 연결 성공")
            
            # 테이블 존재 여부 확인
            cursor = db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            print(f"[OK] 생성된 테이블 수: {len(tables)}")
            
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
            
            # 기본 데이터 삽입 테스트
            print("\n기본 데이터 삽입 테스트:")
            
            # 종목 메타데이터 삽입
            test_stock = {
                'code': '005930',
                'name': '삼성전자',
                'market_type': 'KOSPI',
                'sector_code': '1010',
                'sector_name': '전기전자',
                'listing_date': 19750611,
                'is_etf': False,
                'is_etn': False,
                'is_delisted': False
            }
            
            try:
                result = db.upsert_stock_metadata(test_stock)
                if result:
                    print("[OK] 종목 메타데이터 삽입 성공")
                else:
                    print("[ERROR] 종목 메타데이터 삽입 실패")
            except Exception as e:
                print(f"[ERROR] 종목 메타데이터 삽입 실패: {e}")
            
            # 종목 메타데이터 조회
            stock_info = db.get_stock_metadata('005930')
            if stock_info:
                print(f"[OK] 종목 메타데이터 조회 성공: {stock_info['stock_name']}")
            else:
                print("[ERROR] 종목 메타데이터 조회 실패")
            
            # 모든 종목 조회
            all_stocks = db.get_all_stocks()
            print(f"[OK] 모든 종목 조회: {len(all_stocks)}개")
            
            # 데이터베이스 통계
            cursor.execute("SELECT COUNT(*) FROM stock_metadata")
            stock_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM daily_ohlcv")
            daily_count = cursor.fetchone()[0]
            
            print(f"\n[OK] 데이터베이스 통계:")
            print(f"    - 종목 수: {stock_count}")
            print(f"    - 일봉 데이터 수: {daily_count}")
            
            # 데이터베이스 연결 종료
            db.close()
            print("\n[OK] 데이터베이스 연결 종료")
            
            return True
            
        else:
            print("[ERROR] 데이터베이스 연결 실패")
            return False
            
    except Exception as e:
        print(f"[ERROR] 데이터베이스 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # 임시 파일 정리
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"\n[OK] 임시 파일 정리 완료: {temp_dir}")
            except Exception as e:
                print(f"\n[WARNING] 임시 파일 정리 실패: {e}")


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
        if isinstance(sqlite_config, dict):
            for key in ['journal_mode', 'synchronous', 'cache_size', 'temp_store']:
                if key in sqlite_config:
                    print(f"    - {key}: {sqlite_config[key]}")
        else:
            print(f"    - 설정 형식 오류: {type(sqlite_config)}")
        
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
    
    try:
        # 설정 통합 테스트
        test_results['config'] = test_config_integration()
        
        # 데이터베이스 기본 기능 테스트
        test_results['database'] = test_database_basic()
        
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
            print(f"\n[WARNING] {total_tests - passed_tests}개 테스트 실패")
            return 1
            
    except Exception as e:
        print(f"\n[ERROR] 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())