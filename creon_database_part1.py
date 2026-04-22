# coding=utf-8
"""
Creon DataReader v2.0 - 데이터베이스 관리 모듈 (파트 1)
"""

import sqlite3
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np

from creon_config import get_config

logger = logging.getLogger(__name__)


@dataclass
class DataRange:
    """데이터 범위 정보"""
    start: Optional[int] = None  # YYYYMMDD or YYYYMMDDHHMM
    end: Optional[int] = None
    count: int = 0


@dataclass
class DataGap:
    """데이터 갭 정보"""
    start: int
    end: int
    size: int
    priority: int = 1  # 1: 높음, 2: 중간, 3: 낮음


@dataclass
class MergeResult:
    """병합 결과 정보"""
    added_count: int = 0
    updated_count: int = 0
    removed_count: int = 0
    total_count: int = 0
    date_range: Optional[DataRange] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class DatabaseError(Exception):
    """데이터베이스 관련 에러"""
    pass


class CreonDatabase:
    """Creon 데이터베이스 관리 클래스"""
    
    def __init__(self, db_path: Optional[str] = None):
        """초기화"""
        self.config = get_config()
        
        if db_path:
            self.db_path = db_path
        else:
            self.db_path = self.config.get('storage.database_path', './data/database/creon_data.db')
        
        # 데이터베이스 파일 경로 생성
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.conn: Optional[sqlite3.Connection] = None
        self._initialize_database()
        logger.info(f"데이터베이스 초기화 완료: {self.db_path}")
    
    def _initialize_database(self):
        """데이터베이스 초기화 (테이블 생성)"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            
            # INI 설정에서 SQLite 최적화 설정 적용
            self._apply_sqlite_optimizations()
            
            # 외래키 제약 조건 활성화
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            # 테이블 생성
            self._create_tables()
            
            # 인덱스 생성
            self._create_indexes()
            
            # 기본 데이터 설정
            self._initialize_default_data()
            
            logger.debug("데이터베이스 테이블 생성 완료")
            
        except Exception as e:
            logger.error(f"데이터베이스 초기화 실패: {e}")
            raise DatabaseError(f"데이터베이스 초기화 실패: {e}")
    
    def _apply_sqlite_optimizations(self):
        """SQLite 최적화 설정 적용"""
        try:
            # INI 설정에서 SQLite 최적화 설정 가져오기
            sqlite_config = self.config.get('database_sqlite', {})
            
            # 설정이 딕셔너리인지 확인
            if not isinstance(sqlite_config, dict):
                logger.warning(f"SQLite 설정 형식 오류: {type(sqlite_config)}")
                return
            
            # WAL 모드 설정 (Write-Ahead Logging)
            journal_mode = sqlite_config.get('journal_mode', 'WAL')
            self.conn.execute(f"PRAGMA journal_mode = {journal_mode}")
            logger.debug(f"SQLite journal_mode 설정: {journal_mode}")
            
            # 동기화 모드 설정
            synchronous = sqlite_config.get('synchronous', 'NORMAL')
            self.conn.execute(f"PRAGMA synchronous = {synchronous}")
            logger.debug(f"SQLite synchronous 설정: {synchronous}")
            
            # 캐시 크기 설정 (페이지 단위, 음수는 KiB 단위)
            cache_size = sqlite_config.get('cache_size', -2000)
            self.conn.execute(f"PRAGMA cache_size = {cache_size}")
            logger.debug(f"SQLite cache_size 설정: {cache_size}")
            
            # 임시 저장소 설정
            temp_store = sqlite_config.get('temp_store', 'MEMORY')
            self.conn.execute(f"PRAGMA temp_store = {temp_store}")
            logger.debug(f"SQLite temp_store 설정: {temp_store}")
            
            # 메모리 매핑 크기 설정
            mmap_size = sqlite_config.get('mmap_size', 268435456)  # 256MB
            self.conn.execute(f"PRAGMA mmap_size = {mmap_size}")
            logger.debug(f"SQLite mmap_size 설정: {mmap_size}")
            
            # Busy 타임아웃 설정
            busy_timeout = sqlite_config.get('busy_timeout', 5000)
            self.conn.execute(f"PRAGMA busy_timeout = {busy_timeout}")
            logger.debug(f"SQLite busy_timeout 설정: {busy_timeout}")
            
            # 자동 VACUUM 설정
            auto_vacuum = sqlite_config.get('auto_vacuum', 'INCREMENTAL')
            self.conn.execute(f"PRAGMA auto_vacuum = {auto_vacuum}")
            logger.debug(f"SQLite auto_vacuum 설정: {auto_vacuum}")
            
            # 페이지 크기 설정 (기본값 4096)
            self.conn.execute("PRAGMA page_size = 4096")
            
            # 성능 향상을 위한 추가 설정
            self.conn.execute("PRAGMA optimize")
            
            logger.info("SQLite 최적화 설정 적용 완료")
            
        except Exception as e:
            logger.warning(f"SQLite 최적화 설정 적용 실패: {e}")
            # 최적화 실패해도 기본 동작은 유지
    
    def _create_tables(self):
        """테이블 생성"""
        cursor = self.conn.cursor()
        
        # 시스템 설정 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                config_key VARCHAR(50) PRIMARY KEY,
                config_value TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 종목 메타정보 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_metadata (
                stock_code VARCHAR(10) PRIMARY KEY,
                stock_name VARCHAR(100) NOT NULL,
                market_type VARCHAR(10) NOT NULL,
                sector_code VARCHAR(10),
                sector_name VARCHAR(50),
                listing_date INTEGER,
                delisting_date INTEGER DEFAULT NULL,
                is_etf BOOLEAN DEFAULT FALSE,
                is_etn BOOLEAN DEFAULT FALSE,
                is_delisted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (market_type IN ('KOSPI', 'KOSDAQ', 'KONEX'))
            )
        """)
        
        # 데이터 수집 설정 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collection_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_name VARCHAR(50) UNIQUE NOT NULL,
                chart_type VARCHAR(20) NOT NULL,
                interval_minutes INTEGER DEFAULT NULL,
                data_count INTEGER DEFAULT 1000,
                ohlcv_only BOOLEAN DEFAULT TRUE,
                enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 수집 작업 이력 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collection_jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER,
                job_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP DEFAULT NULL,
                total_stocks INTEGER DEFAULT 0,
                processed_stocks INTEGER DEFAULT 0,
                failed_stocks INTEGER DEFAULT 0,
                error_message TEXT DEFAULT NULL,
                FOREIGN KEY (config_id) REFERENCES collection_config(id)
            )
        """)
        
        # 일봉 OHLCV 데이터 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_ohlcv (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                date INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume BIGINT NOT NULL,
                amount BIGINT,
                change REAL,
                change_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date),
                FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code),
                CHECK (date BETWEEN 19000101 AND 21000101),
                CHECK (high >= low),
                CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0)
            )
        """)
        
        # 일봉 추가 지표 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                date INTEGER NOT NULL,
                market_cap BIGINT,
                listed_shares BIGINT,
                foreign_limit BIGINT,
                foreign_holdings BIGINT,
                foreign_ratio REAL,
                institution_net BIGINT,
                institution_cumulative BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date),
                FOREIGN KEY (stock_code, date) REFERENCES daily_ohlcv(stock_code, date)
            )
        """)
        
        # 분봉 데이터 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minute_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                datetime INTEGER NOT NULL,
                date INTEGER NOT NULL,
                interval_minutes INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume BIGINT NOT NULL,
                amount BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, datetime, interval_minutes),
                FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code),
                CHECK (interval_minutes IN (1, 5, 30, 60)),
                CHECK (datetime BETWEEN 190001010000 AND 210001010000),
                CHECK (high >= low),
                CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0)
            )
        """)
        
        # 데이터 검증 결과 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_validation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                chart_type VARCHAR(20) NOT NULL,
                interval_minutes INTEGER DEFAULT NULL,
                validation_date INTEGER NOT NULL,
                check_type VARCHAR(30) NOT NULL,
                status VARCHAR(20) NOT NULL,
                issues_found INTEGER DEFAULT 0,
                details TEXT,
                validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
            )
        """)
        
        # 데이터 갭 정보 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_gaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                chart_type VARCHAR(20) NOT NULL,
                interval_minutes INTEGER DEFAULT NULL,
                gap_start INTEGER NOT NULL,
                gap_end INTEGER NOT NULL,
                gap_size INTEGER NOT NULL,
                priority INTEGER DEFAULT 1,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                filled_at TIMESTAMP DEFAULT NULL,
                FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
            )
        """)
        
        # 병합 이력 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS merge_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code VARCHAR(10) NOT NULL,
                chart_type VARCHAR(20) NOT NULL,
                interval_minutes INTEGER DEFAULT NULL,
                merge_type VARCHAR(20) NOT NULL,
                before_merge_start INTEGER,
                before_merge_end INTEGER,
                after_merge_start INTEGER,
                after_merge_end INTEGER,
                added_count INTEGER DEFAULT 0,
                updated_count INTEGER DEFAULT 0,
                removed_count INTEGER DEFAULT 0,
                merge_date INTEGER NOT NULL,
                merged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
            )
        """)
        
        self.conn.commit()
    
    def _create_indexes(self):
        """인덱스 생성"""
        cursor = self.conn.cursor()
        
        # 일봉 데이터 인덱스
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_stock_date ON daily_ohlcv(stock_code, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_ohlcv(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_stock ON daily_ohlcv(stock_code)")
        
        # 분봉 데이터 인덱스
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_stock_datetime ON minute_data(stock_code, datetime)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_stock_interval ON minute_data(stock_code, interval_minutes)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_datetime ON minute_data(datetime)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_date_interval ON minute_data(date, interval_minutes)")
        
        # 메타데이터 인덱스
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_market ON stock_metadata(market_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_sector ON stock_metadata(sector_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_listing ON stock_metadata(listing_date)")
        
        # 데이터 갭 인덱스
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gaps_stock_type ON data_gaps(stock_code, chart_type, interval_minutes)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gaps_priority ON data_gaps(priority, detected_at)")
        
        self.conn.commit()
    
    def _initialize_default_data(self):
        """기본 데이터 설정"""
        cursor = self.conn.cursor()
        
        # 시스템 기본 설정
        default_configs = [
            ('data_retention_days', '3650', '데이터 보관 기간 (일)'),
            ('merge_lookback_days', '3', '이어받기 시 확인할 일수'),
            ('default_collection_count', '1000', '기본 수집 데이터 개수'),
            ('request_delay_seconds', '0.25', 'API 요청 간 딜레이'),
            ('max_retry_count', '3', '최대 재시도 횟수'),
            ('auto_merge_enabled', 'true', '자동 병합 기능 사용 여부'),
            ('data_validation_enabled', 'true', '데이터 검증 기능 사용 여부')
        ]
        
        for key, value, desc in default_configs:
            cursor.execute("""
                INSERT OR IGNORE INTO system_config (config_key, config_value, description)
                VALUES (?, ?, ?)
            """, (key, value, desc))
        
        # 기본 수집 설정
        default_collections = [
            ('daily_full', 'daily', None, 10000, True),
            ('1min_full', 'minute', 1, 5000, True),
            ('5min_full', 'minute', 5, 5000, True),
            ('30min_full', 'minute', 30, 5000, True),
            ('60min_full', 'minute', 60, 5000, True)
        ]
        
        for name, chart_type, interval, count, ohlcv_only in default_collections:
            cursor.execute("""
                INSERT OR IGNORE INTO collection_config 
                (config_name, chart_type, interval_minutes, data_count, ohlcv_only)
                VALUES (?, ?, ?, ?, ?)
            """, (name, chart_type, interval, count, ohlcv_only))
        
        self.conn.commit()
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.debug("데이터베이스 연결 종료")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # ===== 종목 메타데이터 관리 =====
    
    def upsert_stock_metadata(self, stock_info: Dict[str, Any]) -> bool:
        """종목 메타데이터 저장 또는 업데이트"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO stock_metadata 
                (stock_code, stock_name, market_type, sector_code, sector_name,
                 listing_date, delisting_date, is_etf, is_etn, is_delisted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                stock_info['code'],
                stock_info.get('name', ''),
                stock_info.get('market_type', 'KOSPI'),
                stock_info.get('sector_code', ''),
                stock_info.get('sector_name', ''),
                stock_info.get('listing_date'),
                stock_info.get('delisting_date'),
                1 if stock_info.get('is_etf', False) else 0,
                1 if stock_info.get('is_etn', False) else 0,
                1 if stock_info.get('is_delisted', False) else 0
            ))
            
            self.conn.commit()
            logger.debug(f"종목 메타데이터 저장: {stock_info['code']}")
            return True
            
        except Exception as e:
            logger.error(f"종목 메타데이터 저장 실패: {stock_info['code']}, {e}")
            return False
    
    def get_stock_metadata(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """종목 메타데이터 조회"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM stock_metadata WHERE stock_code = ?", (stock_code,))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
            
        except Exception as e:
            logger.error(f"종목 메타데이터 조회 실패: {stock_code}, {e}")
            return None
    
    def get_all_stocks(self, market: Optional[str] = None) -> List[Dict[str, Any]]:
        """모든 종목 메타데이터 조회"""
        try:
            cursor = self.conn.cursor()
            
            if market:
                cursor.execute("SELECT * FROM stock_metadata WHERE market_type = ?", (market,))
            else:
                cursor.execute("SELECT * FROM stock_metadata")
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"종목 목록 조회 실패: {e}")
            return []