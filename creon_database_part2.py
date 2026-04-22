# coding=utf-8
"""
Creon DataReader v2.0 - 데이터베이스 관리 모듈 (파트 2)
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
from creon_database_part1 import DataRange, DataGap, MergeResult, DatabaseError, CreonDatabase

logger = logging.getLogger(__name__)


class CreonDatabaseExtended(CreonDatabase):
    """Creon 데이터베이스 관리 클래스 (확장)"""
    
    # ===== 일봉 데이터 관리 =====
    
    def insert_daily_data(self, stock_code: str, data: Dict[str, List[Any]]) -> int:
        """
        일봉 데이터 저장
        Returns: 저장된 데이터 개수
        """
        try:
            cursor = self.conn.cursor()
            inserted_count = 0
            
            # 데이터 준비
            dates = data.get('date', [])
            opens = data.get('open', [])
            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])
            volumes = data.get('volume', [])
            
            if not dates:
                logger.warning(f"저장할 일봉 데이터 없음: {stock_code}")
                return 0
            
            # 배치 삽입
            for i in range(len(dates)):
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO daily_ohlcv 
                        (stock_code, date, open, high, low, close, volume, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        stock_code,
                        dates[i],
                        opens[i] if i < len(opens) else 0,
                        highs[i] if i < len(highs) else 0,
                        lows[i] if i < len(lows) else 0,
                        closes[i] if i < len(closes) else 0,
                        volumes[i] if i < len(volumes) else 0
                    ))
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"일봉 데이터 삽입 실패: {stock_code}, 날짜: {dates[i]}, {e}")
                    continue
            
            self.conn.commit()
            logger.info(f"일봉 데이터 저장 완료: {stock_code}, {inserted_count}개")
            return inserted_count
            
        except Exception as e:
            logger.error(f"일봉 데이터 저장 실패: {stock_code}, {e}")
            self.conn.rollback()
            return 0
    
    def get_daily_data(self, stock_code: str, 
                      start_date: Optional[int] = None,
                      end_date: Optional[int] = None,
                      limit: Optional[int] = None) -> pd.DataFrame:
        """일봉 데이터 조회"""
        try:
            query = """
                SELECT date, open, high, low, close, volume, amount, change, change_rate
                FROM daily_ohlcv
                WHERE stock_code = ?
            """
            params = [stock_code]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            query += " ORDER BY date"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            df = pd.read_sql_query(query, self.conn, params=params)
            
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
                df.set_index('date', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"일봉 데이터 조회 실패: {stock_code}, {e}")
            return pd.DataFrame()
    
    def get_daily_data_range(self, stock_code: str) -> DataRange:
        """일봉 데이터 범위 조회"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT MIN(date), MAX(date), COUNT(*) 
                FROM daily_ohlcv 
                WHERE stock_code = ?
            """, (stock_code,))
            
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                return DataRange(
                    start=result[0],
                    end=result[1],
                    count=result[2]
                )
            else:
                return DataRange()
            
        except Exception as e:
            logger.error(f"일봉 데이터 범위 조회 실패: {stock_code}, {e}")
            return DataRange()
    
    # ===== 분봉 데이터 관리 =====
    
    def insert_minute_data(self, stock_code: str, interval: int, data: Dict[str, List[Any]]) -> int:
        """
        분봉 데이터 저장
        Returns: 저장된 데이터 개수
        """
        try:
            cursor = self.conn.cursor()
            inserted_count = 0
            
            # 데이터 준비
            datetimes = data.get('date', [])  # 실제로는 datetime (YYYYMMDDHHMM)
            opens = data.get('open', [])
            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])
            volumes = data.get('volume', [])
            
            if not datetimes:
                logger.warning(f"저장할 분봉 데이터 없음: {stock_code}, 간격: {interval}")
                return 0
            
            # 배치 삽입
            for i in range(len(datetimes)):
                try:
                    datetime_val = datetimes[i]
                    date_val = datetime_val // 10000  # YYYYMMDD 추출
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO minute_data 
                        (stock_code, datetime, date, interval_minutes, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        stock_code,
                        datetime_val,
                        date_val,
                        interval,
                        opens[i] if i < len(opens) else 0,
                        highs[i] if i < len(highs) else 0,
                        lows[i] if i < len(lows) else 0,
                        closes[i] if i < len(closes) else 0,
                        volumes[i] if i < len(volumes) else 0
                    ))
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"분봉 데이터 삽입 실패: {stock_code}, 시간: {datetimes[i]}, {e}")
                    continue
            
            self.conn.commit()
            logger.info(f"분봉 데이터 저장 완료: {stock_code}, 간격: {interval}, {inserted_count}개")
            return inserted_count
            
        except Exception as e:
            logger.error(f"분봉 데이터 저장 실패: {stock_code}, 간격: {interval}, {e}")
            self.conn.rollback()
            return 0
    
    def get_minute_data(self, stock_code: str, interval: int,
                       start_datetime: Optional[int] = None,
                       end_datetime: Optional[int] = None,
                       limit: Optional[int] = None) -> pd.DataFrame:
        """분봉 데이터 조회"""
        try:
            query = """
                SELECT datetime, open, high, low, close, volume, amount
                FROM minute_data
                WHERE stock_code = ? AND interval_minutes = ?
            """
            params = [stock_code, interval]
            
            if start_datetime:
                query += " AND datetime >= ?"
                params.append(start_datetime)
            
            if end_datetime:
                query += " AND datetime <= ?"
                params.append(end_datetime)
            
            query += " ORDER BY datetime"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            df = pd.read_sql_query(query, self.conn, params=params)
            
            if not df.empty:
                # datetime을 datetime 형식으로 변환
                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d%H%M')
                df.set_index('datetime', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"분봉 데이터 조회 실패: {stock_code}, 간격: {interval}, {e}")
            return pd.DataFrame()
    
    def get_minute_data_range(self, stock_code: str, interval: int) -> DataRange:
        """분봉 데이터 범위 조회"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT MIN(datetime), MAX(datetime), COUNT(*) 
                FROM minute_data 
                WHERE stock_code = ? AND interval_minutes = ?
            """, (stock_code, interval))
            
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                return DataRange(
                    start=result[0],
                    end=result[1],
                    count=result[2]
                )
            else:
                return DataRange()
            
        except Exception as e:
            logger.error(f"분봉 데이터 범위 조회 실패: {stock_code}, 간격: {interval}, {e}")
            return DataRange()
    
    # ===== 데이터 갭 분석 =====
    
    def find_data_gaps(self, stock_code: str, chart_type: str, 
                      interval: Optional[int] = None) -> List[DataGap]:
        """
        데이터 갭 찾기
        """
        gaps = []
        
        try:
            if chart_type == 'daily':
                data_range = self.get_daily_data_range(stock_code)
                if data_range.count == 0:
                    return gaps
                
                # 모든 날짜 조회
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT date FROM daily_ohlcv 
                    WHERE stock_code = ? 
                    ORDER BY date
                """, (stock_code,))
                
                dates = [row[0] for row in cursor.fetchall()]
                
                # 갭 찾기
                for i in range(1, len(dates)):
                    gap_days = dates[i] - dates[i-1]
                    if gap_days > 100:  # 1일 이상 차이 (YYYYMMDD 형식)
                        gap_size = gap_days // 100  # 대략적인 일수
                        gaps.append(DataGap(
                            start=dates[i-1] + 1,
                            end=dates[i] - 1,
                            size=gap_size,
                            priority=1 if gap_size > 30 else 2
                        ))
            
            elif chart_type == 'minute' and interval:
                data_range = self.get_minute_data_range(stock_code, interval)
                if data_range.count == 0:
                    return gaps
                
                # 모든 datetime 조회
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT datetime FROM minute_data 
                    WHERE stock_code = ? AND interval_minutes = ?
                    ORDER BY datetime
                """, (stock_code, interval))
                
                datetimes = [row[0] for row in cursor.fetchall()]
                
                # 갭 찾기 (간격에 따른 예상 차이 계산)
                expected_gap = interval
                
                for i in range(1, len(datetimes)):
                    # datetime 차이 계산 (분 단위)
                    dt1 = datetimes[i-1]
                    dt2 = datetimes[i]
                    
                    # YYYYMMDDHHMM 형식 파싱
                    year1, month1, day1, hour1, minute1 = self._parse_datetime(dt1)
                    year2, month2, day2, hour2, minute2 = self._parse_datetime(dt2)
                    
                    # 시간 차이 계산 (분)
                    time_diff = ((year2 - year1) * 525600 +  # 1년 = 365*24*60 분
                                (month2 - month1) * 43800 +   # 1월 = 30*24*60 분
                                (day2 - day1) * 1440 +        # 1일 = 24*60 분
                                (hour2 - hour1) * 60 +
                                (minute2 - minute1))
                    
                    if time_diff > expected_gap * 2:  # 예상 간격의 2배 이상 차이
                        gap_size = time_diff // expected_gap - 1
                        gaps.append(DataGap(
                            start=dt1,
                            end=dt2,
                            size=gap_size,
                            priority=1 if gap_size > 100 else 2
                        ))
            
            logger.debug(f"데이터 갭 발견: {stock_code}, {chart_type}, {len(gaps)}개")
            return gaps
            
        except Exception as e:
            logger.error(f"데이터 갭 분석 실패: {stock_code}, {chart_type}, {e}")
            return []
    
    def _parse_datetime(self, dt: int) -> Tuple[int, int, int, int, int]:
        """YYYYMMDDHHMM 형식의 datetime 파싱"""
        dt_str = str(dt).zfill(12)
        year = int(dt_str[0:4])
        month = int(dt_str[4:6])
        day = int(dt_str[6:8])
        hour = int(dt_str[8:10])
        minute = int(dt_str[10:12])
        return year, month, day, hour, minute
    
    def save_data_gap(self, gap: DataGap, stock_code: str, chart_type: str, 
                     interval: Optional[int] = None) -> bool:
        """데이터 갭 저장"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO data_gaps 
                (stock_code, chart_type, interval_minutes, gap_start, gap_end, gap_size, priority, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                stock_code,
                chart_type,
                interval,
                gap.start,
                gap.end,
                gap.size,
                gap.priority
            ))
            
            self.conn.commit()
            logger.debug(f"데이터 갭 저장: {stock_code}, {chart_type}, {gap.start}-{gap.end}")
            return True
            
        except Exception as e:
            logger.error(f"데이터 갭 저장 실패: {stock_code}, {e}")
            return False
    
    # ===== 병합 이력 관리 =====
    
    def save_merge_history(self, stock_code: str, chart_type: str, 
                          interval: Optional[int], merge_type: str,
                          before_range: DataRange, after_range: DataRange,
                          result: MergeResult) -> bool:
        """병합 이력 저장"""
        try:
            cursor = self.conn.cursor()
            
            # 현재 날짜 (YYYYMMDD)
            merge_date = int(datetime.now().strftime('%Y%m%d'))
            
            cursor.execute("""
                INSERT INTO merge_history 
                (stock_code, chart_type, interval_minutes, merge_type,
                 before_merge_start, before_merge_end,
                 after_merge_start, after_merge_end,
                 added_count, updated_count, removed_count, merge_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stock_code,
                chart_type,
                interval,
                merge_type,
                before_range.start,
                before_range.end,
                after_range.start,
                after_range.end,
                result.added_count,
                result.updated_count,
                result.removed_count,
                merge_date
            ))
            
            self.conn.commit()
            logger.info(f"병합 이력 저장: {stock_code}, {chart_type}, 추가: {result.added_count}")
            return True
            
        except Exception as e:
            logger.error(f"병합 이력 저장 실패: {stock_code}, {e}")
            return False
    
    # ===== 유틸리티 메소드 =====
    
    def get_data_info(self, stock_code: str, chart_type: str, 
                     interval: Optional[int] = None) -> Dict[str, Any]:
        """데이터 정보 조회"""
        info = {
            'exists': False,
            'range': None,
            'count': 0,
            'latest_date': None,
            'earliest_date': None,
            'gaps': []
        }
        
        try:
            if chart_type == 'daily':
                data_range = self.get_daily_data_range(stock_code)
                if data_range.count > 0:
                    info['exists'] = True
                    info['range'] = {'start': data_range.start, 'end': data_range.end}
                    info['count'] = data_range.count
                    info['latest_date'] = data_range.end
                    info['earliest_date'] = data_range.start
                    info['gaps'] = self.find_data_gaps(stock_code, chart_type)
            
            elif chart_type == 'minute' and interval:
                data_range = self.get_minute_data_range(stock_code, interval)
                if data_range.count > 0:
                    info['exists'] = True
                    info['range'] = {'start': data_range.start, 'end': data_range.end}
                    info['count'] = data_range.count
                    info['latest_date'] = data_range.end
                    info['earliest_date'] = data_range.start
                    info['gaps'] = self.find_data_gaps(stock_code, chart_type, interval)
        
        except Exception as e:
            logger.error(f"데이터 정보 조회 실패: {stock_code}, {chart_type}, {e}")
        
        return info
    
    def cleanup_old_data(self, retention_days: int = 3650) -> int:
        """오래된 데이터 정리"""
        try:
            cursor = self.conn.cursor()
            
            # 기준 날짜 계산
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y%m%d')
            
            # 일봉 데이터 정리
            cursor.execute("""
                DELETE FROM daily_ohlcv 
                WHERE date < ?
            """, (int(cutoff_date),))
            
            daily_deleted = cursor.rowcount
            
            # 분봉 데이터 정리
            cursor.execute("""
                DELETE FROM minute_data 
                WHERE date < ?
            """, (int(cutoff_date),))
            
            minute_deleted = cursor.rowcount
            
            self.conn.commit()
            
            total_deleted = daily_deleted + minute_deleted
            logger.info(f"오래된 데이터 정리 완료: 일봉 {daily_deleted}개, 분봉 {minute_deleted}개")
            
            return total_deleted
            
        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")
            return 0
    
    def get_database_stats(self) -> Dict[str, Any]:
        """데이터베이스 통계 조회"""
        try:
            cursor = self.conn.cursor()
            
            stats = {}
            
            # 테이블별 통계
            tables = ['stock_metadata', 'daily_ohlcv', 'minute_data', 
                     'data_gaps', 'merge_history', 'collection_jobs']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[f'{table}_count'] = count
            
            # 일봉 데이터 통계
            cursor.execute("""
                SELECT COUNT(DISTINCT stock_code), MIN(date), MAX(date)
                FROM daily_ohlcv
            """)
            daily_stats = cursor.fetchone()
            stats['daily_stocks'] = daily_stats[0]
            stats['daily_earliest'] = daily_stats[1]
            stats['daily_latest'] = daily_stats[2]
            
            # 분봉 데이터 통계
            cursor.execute("""
                SELECT COUNT(DISTINCT stock_code), MIN(datetime), MAX(datetime)
                FROM minute_data
            """)
            minute_stats = cursor.fetchone()
            stats['minute_stocks'] = minute_stats[0]
            stats['minute_earliest'] = minute_stats[1]
            stats['minute_latest'] = minute_stats[2]
            
            # 데이터베이스 파일 크기
            db_file = Path(self.db_path)
            if db_file.exists():
                stats['file_size_mb'] = db_file.stat().st_size / (1024 * 1024)
            
            return stats
            
        except Exception as e:
            logger.error(f"데이터베이스 통계 조회 실패: {e}")
            return {}


# 통합 클래스
class CreonDatabaseManager(CreonDatabaseExtended):
    """통합 데이터베이스 관리 클래스"""
    pass


if __name__ == "__main__":
    # 데이터베이스 테스트
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 데이터베이스 생성
        db = CreonDatabaseManager()
        
        # 통계 출력
        stats = db.get_database_stats()
        print("데이터베이스 통계:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 종목 메타데이터 테스트
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
        
        db.upsert_stock_metadata(test_stock)
        print(f"\n종목 메타데이터 저장: {test_stock['code']}")
        
        # 저장된 데이터 조회
        saved_stock = db.get_stock_metadata('005930')
        if saved_stock:
            print(f"저장된 종목 정보: {saved_stock['stock_name']}")
        
        # 모든 종목 조회
        all_stocks = db.get_all_stocks('KOSPI')
        print(f"\nKOSPI 종목 수: {len(all_stocks)}")
        
        # 데이터베이스 종료
        db.close()
        print("\n데이터베이스 테스트 완료")
        
    except Exception as e:
        print(f"테스트 실패: {e}")
        import traceback
        traceback.print_exc()