# coding=utf-8
"""
Creon DataReader v2.0 - 메인 데이터 수집 클래스
"""

import logging
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from creon_config import get_config
from creon_api import get_api_manager, ChartType, ChartRequest, ChartData
from creon_filemanager import get_file_manager
from creon_database_part1 import CreonDatabase, DataRange, DataGap, MergeResult
from creon_database_part2 import CreonDatabaseExtended

logger = logging.getLogger(__name__)


@dataclass
class CollectionResult:
    """수집 결과 정보"""
    stock_code: str
    chart_type: str
    interval: Optional[int]
    success: bool
    data_count: int = 0
    error_message: Optional[str] = None
    duration: float = 0.0


@dataclass
class MergeStrategy:
    """병합 전략 정보"""
    type: str  # 'full', 'incremental', 'backfill', 'none'
    collect_from: Optional[int] = None
    collect_to: Optional[int] = None
    priority: str = 'medium'
    description: str = ''


class CreonDataReader:
    """Creon 데이터 수집 클래스"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.api = get_api_manager()
        self.file_mgr = get_file_manager()
        self.db = CreonDatabaseExtended()
        
        # 상태 변수
        self.is_running = False
        self.current_job_id = None
        self.progress_callback = None
        
        logger.info("CreonDataReader 초기화 완료")
    
    def set_progress_callback(self, callback):
        """진행 상황 콜백 설정"""
        self.progress_callback = callback
    
    def _update_progress(self, message: str, progress: float = None):
        """진행 상황 업데이트"""
        if self.progress_callback:
            self.progress_callback(message, progress)
        else:
            logger.info(message)
    
    def collect_stock_data(self, stock_code: str, chart_type: str,
                          interval: Optional[int] = None,
                          count: Optional[int] = None,
                          merge: bool = True) -> CollectionResult:
        """
        단일 종목 데이터 수집
        """
        start_time = time.time()
        result = CollectionResult(
            stock_code=stock_code,
            chart_type=chart_type,
            interval=interval,
            success=False
        )
        
        try:
            self._update_progress(f"종목 데이터 수집 시작: {stock_code}, {chart_type}")
            
            # 병합 전략 결정
            merge_strategy = None
            if merge:
                merge_strategy = self._determine_merge_strategy(stock_code, chart_type, interval)
                self._update_progress(f"병합 전략: {merge_strategy.type}")
            
            # 데이터 수집
            chart_data = self._collect_chart_data(stock_code, chart_type, interval, count, merge_strategy)
            
            if chart_data and chart_data.received_count > 0:
                # 데이터 저장
                save_success = self._save_data(stock_code, chart_type, interval, chart_data, merge_strategy)
                
                if save_success:
                    result.success = True
                    result.data_count = chart_data.received_count
                    
                    # 병합 이력 기록
                    if merge_strategy and merge_strategy.type != 'none':
                        self._log_merge_history(stock_code, chart_type, interval, merge_strategy, result)
                
                self._update_progress(f"데이터 저장 완료: {stock_code}, {result.data_count}개")
            else:
                result.error_message = "수집된 데이터가 없습니다."
                self._update_progress(f"데이터 없음: {stock_code}")
        
        except Exception as e:
            result.error_message = str(e)
            logger.error(f"종목 데이터 수집 실패: {stock_code}, {e}")
        
        result.duration = time.time() - start_time
        return result
    
    def _determine_merge_strategy(self, stock_code: str, chart_type: str,
                                 interval: Optional[int]) -> MergeStrategy:
        """
        병합 전략 결정
        """
        # 데이터 존재 여부 확인
        db_info = self.db.get_data_info(stock_code, chart_type, interval)
        file_info = self.file_mgr.get_data_info(stock_code, chart_type, interval)
        
        has_data = db_info['exists'] or file_info['exists']
        
        if not has_data:
            # 데이터 없음 → 전체 수집
            return MergeStrategy(
                type='full',
                description='데이터 없음, 전체 수집'
            )
        
        # 최신 데이터 날짜 확인
        latest_date = None
        if db_info['latest_date']:
            latest_date = db_info['latest_date']
        elif file_info['data_range']:
            latest_date = file_info['data_range']['end']
        
        if not latest_date:
            return MergeStrategy(
                type='full',
                description='데이터 날짜 정보 없음, 전체 수집'
            )
        
        # 현재 날짜 계산
        current_date = self._get_current_date(chart_type, interval)
        
        # 3일 이내인지 확인
        if self._is_within_3_days(latest_date, current_date, chart_type):
            return MergeStrategy(
                type='none',
                description='데이터 최신 상태, 수집 불필요'
            )
        
        # 데이터 갭 확인
        gaps = db_info['gaps']
        if gaps:
            # 가장 큰 갭 처리
            largest_gap = max(gaps, key=lambda x: x.size)
            return MergeStrategy(
                type='backfill',
                collect_from=largest_gap.start,
                collect_to=largest_gap.end,
                priority='high',
                description=f'데이터 갭 채우기: {largest_gap.size}개 누락'
            )
        
        # 증분 업데이트
        collect_from = latest_date
        collect_to = self._calculate_3_days_before(current_date, chart_type, interval)
        
        return MergeStrategy(
            type='incremental',
            collect_from=collect_from,
            collect_to=collect_to,
            description=f'증분 업데이트: {collect_from} ~ {collect_to}'
        )
    
    def _collect_chart_data(self, stock_code: str, chart_type: str,
                           interval: Optional[int], count: Optional[int],
                           merge_strategy: Optional[MergeStrategy]) -> Optional[ChartData]:
        """
        차트 데이터 수집
        """
        try:
            # 요청 파라미터 설정
            if count is None:
                count = self.config.get('collection.default_count', 1000)
            
            # 차트 타입 설정
            if chart_type == 'daily':
                chart_type_enum = ChartType.DAILY
                interval_param = None
            elif chart_type == 'minute':
                chart_type_enum = ChartType.MINUTE
                interval_param = interval
            else:
                raise ValueError(f"지원하지 않는 차트 타입: {chart_type}")
            
            # 요청 생성
            request = ChartRequest(
                code=stock_code,
                chart_type=chart_type_enum,
                count=count,
                interval=interval_param,
                ohlcv_only=self.config.get('collection.ohlcv_only', True)
            )
            
            # 병합 전략에 따른 추가 파라미터
            if merge_strategy and merge_strategy.type in ['incremental', 'backfill']:
                if merge_strategy.collect_from:
                    request.from_date = merge_strategy.collect_from
                if merge_strategy.collect_to:
                    # 날짜 기준 요청으로 변경
                    request.request_type = '1'  # 날짜 기준
                    request.count = None  # 개수 기준 사용 안함
            
            # 데이터 요청
            chart_data = self.api.stock_chart.request_chart(request)
            
            # 요청 제한 딜레이
            time.sleep(self.config.get('collection.request_delay', 0.25))
            
            return chart_data
            
        except Exception as e:
            logger.error(f"차트 데이터 수집 실패: {stock_code}, {chart_type}, {e}")
            return None
    
    def _save_data(self, stock_code: str, chart_type: str, interval: Optional[int],
                  chart_data: ChartData, merge_strategy: Optional[MergeStrategy]) -> bool:
        """
        데이터 저장 (파일 + 데이터베이스)
        """
        try:
            data_dict = chart_data.data
            
            # 파일 저장
            if self.config.get('storage.file_enabled', True):
                file_format = self.config.get('storage.file_format', 'json')
                file_success = self.file_mgr.save_data(
                    stock_code, chart_type, data_dict, interval, file_format
                )
                if not file_success:
                    logger.warning(f"파일 저장 실패: {stock_code}")
            
            # 데이터베이스 저장
            if self.config.get('storage.database_enabled', True):
                if chart_type == 'daily':
                    db_success = self.db.insert_daily_data(stock_code, data_dict) > 0
                elif chart_type == 'minute' and interval:
                    db_success = self.db.insert_minute_data(stock_code, interval, data_dict) > 0
                else:
                    db_success = False
                
                if not db_success:
                    logger.warning(f"데이터베이스 저장 실패: {stock_code}")
            
            return True
            
        except Exception as e:
            logger.error(f"데이터 저장 실패: {stock_code}, {e}")
            return False
    
    def _log_merge_history(self, stock_code: str, chart_type: str,
                          interval: Optional[int], merge_strategy: MergeStrategy,
                          result: CollectionResult):
        """병합 이력 기록"""
        try:
            # 데이터 범위 조회
            data_range = self.db.get_data_info(stock_code, chart_type, interval)['range']
            
            if data_range:
                merge_result = MergeResult(
                    added_count=result.data_count,
                    total_count=result.data_count
                )
                
                # 병합 이력 저장 (간단한 구현)
                logger.info(f"병합 이력: {stock_code}, {chart_type}, 추가: {result.data_count}개")
                
        except Exception as e:
            logger.error(f"병합 이력 기록 실패: {stock_code}, {e}")
    
    def collect_market_data(self, market: str = 'KOSPI', chart_type: str = 'daily',
                           interval: Optional[int] = None, max_stocks: Optional[int] = None,
                           merge: bool = True, count: Optional[int] = None,
                           filters: Optional[Dict[str, Any]] = None) -> List[CollectionResult]:
        """
        마켓 전체 데이터 수집
        filters 파라미터가 없으면 설정 파일에서 읽음 (MH_creon_datareader_down_20260106.py 필터링 로직 적용)
        """
        results = []
        
        try:
            # 종목 목록 가져오기
            if filters is None:
                filters = {
                    'exclude_etf': self.config.get('filters.exclude_etf', True),
                    'exclude_etn': self.config.get('filters.exclude_etn', True),
                    'exclude_delisted': self.config.get('filters.exclude_delisted', True),
                    'exclude_keywords': self.config.get('filters.exclude_keywords', []),
                    'price_min': self.config.get('filters.price_min', 0),
                    'price_max': self.config.get('filters.price_max', 0),
                    'amount_min': self.config.get('filters.amount_min', 0),
                    'amount_max': self.config.get('filters.amount_max', 0)
                }
            
            stocks = self.api.get_market_codes(market, filters)
            
            if max_stocks and max_stocks > 0:
                stocks = stocks[:max_stocks]
            
            total_stocks = len(stocks)
            print(f"\n[종목] 전체: {total_stocks}개")
            
            # 병렬 처리 설정
            max_workers = self.config.get('performance.max_workers', 4)
            use_multiprocessing = self.config.get('performance.use_multiprocessing', False)
            
            success_count = 0
            total_data = 0
            
            if use_multiprocessing and max_workers > 1:
                results = self._collect_parallel(stocks, chart_type, interval, merge, max_workers)
                success_count = sum(1 for r in results if r.success)
                total_data = sum(r.data_count for r in results if r.success)
            else:
                for i, stock_info in enumerate(stocks):
                    stock_code = stock_info['code']
                    stock_name = stock_info.get('name', stock_code)
                    
                    result = self.collect_stock_data(stock_code, chart_type, interval, count=count, merge=merge)
                    results.append(result)
                    
                    if result.success:
                        success_count += 1
                        total_data += result.data_count
                    
                    # 한 줄 진행률: 전체/완료/비율 + 현재 종목명
                    pct = (i + 1) / total_stocks * 100
                    print(f"\r[진행] {i+1}/{total_stocks} ({pct:.1f}%) | 성공 {success_count} | 현재: {stock_name}    ", end='', flush=True)
            
            print()
            
        except Exception as e:
            logger.error(f"마켓 데이터 수집 실패: {market}, {e}")
        
        return results
    
    def _collect_parallel(self, stocks: List[Dict[str, Any]], chart_type: str,
                         interval: Optional[int], merge: bool, max_workers: int) -> List[CollectionResult]:
        """병렬 데이터 수집"""
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 작업 제출
            future_to_stock = {}
            for stock_info in stocks:
                stock_code = stock_info['code']
                future = executor.submit(
                    self.collect_stock_data,
                    stock_code, chart_type, interval, None, merge
                )
                # count는 merge_strategy에 포함되므로 생략
                future_to_stock[future] = stock_code
            
            # 결과 수집
            completed = 0
            total = len(stocks)
            
            for future in as_completed(future_to_stock):
                completed += 1
                stock_code = future_to_stock[future]
                
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    result = CollectionResult(
                        stock_code=stock_code,
                        chart_type=chart_type,
                        interval=interval,
                        success=False,
                        error_message=str(e)
                    )
                    results.append(result)
                
                # 진행 상황 업데이트
                progress = completed / total * 100
                self._update_progress(
                    f"병렬 처리 중: {completed}/{total} ({progress:.1f}%)",
                    progress
                )
        
        return results
    
    def _get_current_date(self, chart_type: str, interval: Optional[int]) -> int:
        """현재 날짜/시간 계산"""
        now = datetime.now()
        
        if chart_type == 'daily':
            return int(now.strftime('%Y%m%d'))
        elif chart_type == 'minute':
            # 가장 가까운 이전 분봉 시간 계산
            if interval:
                minute = (now.minute // interval) * interval
                return int(now.strftime('%Y%m%d%H') + f"{minute:02d}")
        
        return int(now.strftime('%Y%m%d%H%M'))
    
    def _is_within_3_days(self, latest_date: int, current_date: int, chart_type: str) -> bool:
        """3일 이내인지 확인"""
        if chart_type == 'daily':
            # 일봉: 날짜 차이 계산
            days_diff = (current_date - latest_date) // 10000  # YYYYMMDD 형식
            return days_diff <= 3
        elif chart_type == 'minute':
            # 분봉: 시간 차이 계산 (대략 3일 = 4320분)
            # 간단한 구현: 날짜 부분만 비교
            latest_date_str = str(latest_date)
            current_date_str = str(current_date)
            
            if len(latest_date_str) >= 8 and len(current_date_str) >= 8:
                latest_day = int(latest_date_str[:8])
                current_day = int(current_date_str[:8])
                days_diff = current_day - latest_day
                return days_diff <= 3
        
        return False
    
    def _calculate_3_days_before(self, current_date: int, chart_type: str,
                                interval: Optional[int]) -> int:
        """3일 전 날짜/시간 계산"""
        if chart_type == 'daily':
            # 일봉: 3일 전 날짜
            date_str = str(current_date)
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            target_date = datetime(year, month, day) - timedelta(days=3)
            return int(target_date.strftime('%Y%m%d'))
        
        elif chart_type == 'minute':
            # 분봉: 3일 전 시간 (간단한 구현)
            date_str = str(current_date).zfill(12)
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            hour = int(date_str[8:10])
            minute = int(date_str[10:12])
            
            target_datetime = datetime(year, month, day, hour, minute) - timedelta(days=3)
            return int(target_datetime.strftime('%Y%m%d%H%M'))
        
        return current_date
    
    def get_data_summary(self, market: Optional[str] = None) -> Dict[str, Any]:
        """데이터 요약 정보 조회"""
        summary = {
            'total_stocks': 0,
            'daily_data': {'count': 0, 'stocks': 0},
            'minute_data': {'1min': 0, '5min': 0, '30min': 0, '60min': 0},
            'file_stats': {'total_size_mb': 0, 'file_count': 0},
            'db_stats': {}
        }
        
        try:
            # 데이터베이스 통계
            db_stats = self.db.get_database_stats()
            summary['db_stats'] = db_stats
            
            # 파일 통계
            file_mgr = self.file_mgr
            
            # 일봉 파일 통계
            daily_files = file_mgr.get_all_files('daily')
            summary['daily_data']['stocks'] = len(daily_files)
            summary['daily_data']['count'] = sum(f.get('data_count', 0) for f in daily_files)
            
            # 분봉 파일 통계
            for interval in [1, 5, 30, 60]:
                minute_files = file_mgr.get_all_files('minute', interval)
                summary['minute_data'][f'{interval}min'] = len(minute_files)
            
            # 총 종목 수
            if market:
                stocks = self.api.get_market_codes(market)
                summary['total_stocks'] = len(stocks)
            else:
                # 모든 마켓 종목 수
                total = 0
                for market_type in ['KOSPI', 'KOSDAQ', 'KONEX']:
                    try:
                        stocks = self.api.get_market_codes(market_type)
                        total += len(stocks)
                    except:
                        pass
                summary['total_stocks'] = total
        
        except Exception as e:
            logger.error(f"데이터 요약 조회 실패: {e}")
        
        return summary
    
    def cleanup(self):
        """정리 작업"""
        try:
            # 오래된 데이터 정리
            retention_days = self.config.get('data_retention_days', 3650)
            deleted_count = self.db.cleanup_old_data(retention_days)
            
            # 오래된 백업 파일 정리
            backup_retention = self.config.get('merge.backup_retention_days', 30)
            backup_deleted = self.file_mgr.cleanup_old_backups(backup_retention)
            
            logger.info(f"정리 작업 완료: 데이터 {deleted_count}개, 백업 {backup_deleted}개 삭제")
            
        except Exception as e:
            logger.error(f"정리 작업 실패: {e}")


# 전역 데이터 리더 인스턴스
_data_reader: Optional[CreonDataReader] = None


def get_data_reader() -> CreonDataReader:
    """전역 데이터 리더 인스턴스 반환"""
    global _data_reader
    
    if _data_reader is None:
        _data_reader = CreonDataReader()
    
    return _data_reader


if __name__ == "__main__":
    # 데이터 리더 테스트
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    def progress_callback(message: str, progress: float = None):
        """진행 상황 콜백"""
        if progress is not None:
            print(f"[{progress:.1f}%] {message}")
        else:
            print(message)
    
    try:
        # 데이터 리더 생성
        reader = get_data_reader()
        reader.set_progress_callback(progress_callback)
        
        # 연결 테스트
        print("Creon 연결 테스트...")
        if reader.api.check_connection():
            print("Creon 연결 성공")
            
            # 데이터 요약 조회
            print("\n데이터 요약 조회...")
            summary = reader.get_data_summary('KOSPI')
            print(f"KOSPI 총 종목: {summary['total_stocks']}")
            print(f"일봉 데이터: {summary['daily_data']['stocks']}종목")
            
            # 단일 종목 테스트 (옵션)
            test_collection = False  # 테스트 시 True로 변경
            
            if test_collection:
                print("\n단일 종목 데이터 수집 테스트...")
                
                # 테스트 종목 선택 (삼성전자)
                test_stock = '005930'
                
                result = reader.collect_stock_data(test_stock, 'daily', merge=True)
                print(f"수집 결과: {'성공' if result.success else '실패'}")
                print(f"데이터 개수: {result.data_count}")
                print(f"소요 시간: {result.duration:.2f}초")
                
                if result.error_message:
                    print(f"에러: {result.error_message}")
            
            # 정리 작업 테스트
            print("\n정리 작업 테스트...")
            reader.cleanup()
            
            print("\n데이터 리더 테스트 완료")
        
        else:
            print("Creon 연결 실패")
            
    except Exception as e:
        print(f"테스트 실패: {e}")
        import traceback
        traceback.print_exc()