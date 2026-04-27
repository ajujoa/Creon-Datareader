# coding=utf-8
"""
Creon DataReader v2.0 - Creon API 래퍼 모듈
"""

import win32com.client
import time
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ChartType(Enum):
    """차트 타입 열거형"""
    DAILY = 'D'
    WEEKLY = 'W'
    MONTHLY = 'M'
    MINUTE = 'm'
    TICK = 'T'


class RequestType(Enum):
    """요청 타입 열거형"""
    BY_COUNT = '2'  # 개수 기준
    BY_DATE = '1'   # 날짜 기준


@dataclass
class ChartRequest:
    """차트 데이터 요청 설정"""
    code: str
    chart_type: ChartType
    request_type: RequestType = RequestType.BY_COUNT
    count: int = 1000
    from_date: Optional[int] = None  # YYYYMMDD or YYYYMMDDHHMM
    to_date: Optional[int] = None    # YYYYMMDD or YYYYMMDDHHMM
    interval: Optional[int] = None   # 분봉 간격 (1, 3, 5, 10, 15, 30, 60)
    ohlcv_only: bool = True
    adjusted_price: bool = True


@dataclass
class ChartData:
    """차트 데이터 결과"""
    code: str
    chart_type: ChartType
    interval: Optional[int]
    columns: List[str]
    data: Dict[str, List[Any]]
    request_time: datetime
    received_count: int


class CreonAPIError(Exception):
    """Creon API 관련 에러"""
    pass


class ConnectionError(CreonAPIError):
    """Creon 연결 에러"""
    pass


class RequestError(CreonAPIError):
    """데이터 요청 에러"""
    pass


class CpStockChart:
    """주식 차트 데이터 클래스"""
    
    # OHLCV 기본 컬럼
    OHLCV_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    # 분봉 추가 컬럼
    MINUTE_COLUMNS = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']
    
    # 확장 컬럼 (OHLCV 외 추가 데이터)
    EXTENDED_COLUMNS = [
        'date', 'open', 'high', 'low', 'close', 'volume',
        '상장주식수', '외국인주문한도수량', '외국인현보유수량',
        '외국인현보유비율', '기관순매수', '기관누적순매수'
    ]
    
    # 분봉 확장 컬럼
    MINUTE_EXTENDED_COLUMNS = [
        'date', 'time', 'open', 'high', 'low', 'close', 'volume',
        '상장주식수', '외국인주문한도수량', '외국인현보유수량',
        '외국인현보유비율', '기관순매수', '기관누적순매수'
    ]
    
    def __init__(self):
        """초기화"""
        self.objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
        self._check_connection()
        logger.info("CpStockChart 초기화 완료")
    
    def _check_connection(self):
        """Creon PLUS 연결 상태 확인"""
        try:
            objCpStatus = win32com.client.Dispatch('CpUtil.CpCybos')
            bConnect = objCpStatus.IsConnect
            
            if bConnect == 0:
                raise ConnectionError("PLUS가 정상적으로 연결되지 않았습니다.")
            
            logger.debug("Creon PLUS 연결 상태: 정상")
            
        except Exception as e:
            raise ConnectionError(f"Creon 연결 확인 실패: {e}")
    
    def _check_request_status(self):
        """
        요청 상태 확인
        Returns: (status_code, status_message)
        """
        try:
            rqStatus = self.objStockChart.GetDibStatus()
            rqRet = self.objStockChart.GetDibMsg1()
            
            if rqStatus != 0:
                error_msg = f"통신상태 오류[{rqStatus}]{rqRet}"
                logger.error(error_msg)
                raise RequestError(error_msg)
            
            return rqStatus, rqRet
            
        except Exception as e:
            raise RequestError(f"요청 상태 확인 실패: {e}")
    
    def _get_column_indices(self, ohlcv_only: bool, is_minute: bool = False) -> Tuple[List[int], List[str]]:
        """컬럼 인덱스와 이름 반환"""
        if ohlcv_only:
            if is_minute:
                indices = [0, 1, 2, 3, 4, 5, 8]  # 날짜, 시간, 시가, 고가, 저가, 종가, 거래량
                columns = self.MINUTE_COLUMNS
            else:
                indices = [0, 2, 3, 4, 5, 8]  # 날짜, 시가, 고가, 저가, 종가, 거래량
                columns = self.OHLCV_COLUMNS
        else:
            if is_minute:
                indices = [0, 1, 2, 3, 4, 5, 8, 12, 14, 16, 17, 20, 21]
                columns = self.MINUTE_EXTENDED_COLUMNS
            else:
                indices = [0, 2, 3, 4, 5, 8, 12, 14, 16, 17, 20, 21]
                columns = self.EXTENDED_COLUMNS
        
        return indices, columns
    
    def request_chart(self, request: ChartRequest) -> ChartData:
        """
        차트 데이터 요청
        """
        logger.info(f"차트 데이터 요청: {request.code}, 타입: {request.chart_type.value}")
        
        # 요청 시작 시간 기록
        request_time = datetime.now()
        
        # 요청 파라미터 설정
        self._set_request_parameters(request)
        
        # 데이터 수집
        data = self._collect_data(request)
        
        # 분봉 데이터 처리
        if request.chart_type == ChartType.MINUTE:
            data = self._process_minute_data(data)
        
        # 결과 생성
        result = ChartData(
            code=request.code,
            chart_type=request.chart_type,
            interval=request.interval,
            columns=data['columns'],
            data=data['values'],
            request_time=request_time,
            received_count=len(data['values']['date']) if data['values']['date'] else 0
        )
        
        logger.info(f"데이터 수집 완료: {result.received_count}개")
        return result
    
    def _set_request_parameters(self, request: ChartRequest):
        """요청 파라미터 설정"""
        # 기본 파라미터
        self.objStockChart.SetInputValue(0, request.code)  # 종목코드
        self.objStockChart.SetInputValue(1, ord(request.request_type.value))  # 요청 타입
        
        # 요청 타입에 따른 파라미터 설정
        if request.request_type == RequestType.BY_COUNT:
            self.objStockChart.SetInputValue(4, request.count)  # 요청 개수
        elif request.request_type == RequestType.BY_DATE:
            if request.from_date:
                self.objStockChart.SetInputValue(3, request.from_date)  # 시작일
            if request.to_date:
                self.objStockChart.SetInputValue(2, request.to_date)    # 종료일
        
        # 컬럼 설정
        is_minute = request.chart_type == ChartType.MINUTE
        indices, columns = self._get_column_indices(request.ohlcv_only, is_minute)
        self.objStockChart.SetInputValue(5, indices)
        
        # 차트 타입 설정
        self.objStockChart.SetInputValue(6, ord(request.chart_type.value))
        
        # 분봉 간격 설정
        if request.chart_type == ChartType.MINUTE and request.interval:
            self.objStockChart.SetInputValue(7, request.interval)
        
        # 수정주가 사용 여부
        self.objStockChart.SetInputValue(9, ord('1') if request.adjusted_price else ord('0'))
    
    def _collect_data(self, request: ChartRequest) -> Dict[str, Any]:
        """데이터 수집"""
        is_minute = request.chart_type == ChartType.MINUTE
        _, columns = self._get_column_indices(request.ohlcv_only, is_minute)
        
        # 데이터 저장용 딕셔너리 초기화
        data_values = {col: [] for col in columns}
        
        total_received = 0
        max_count = request.count if request.request_type == RequestType.BY_COUNT else 10000
        
        while total_received < max_count:
            # 요청 실행
            self.objStockChart.BlockRequest()
            
            # 요청 상태 확인
            self._check_request_status()
            
            # 요청 제한으로 인한 딜레이
            time.sleep(0.25)
            
            # 수신된 데이터 개수
            batch_size = self.objStockChart.GetHeaderValue(3)
            if batch_size == 0:
                logger.warning(f"수신된 데이터가 없음: {request.code}")
                break
            
            # 배치 크기 조정 (요청 개수 초과 방지)
            remaining = max_count - total_received
            batch_size = min(batch_size, remaining)
            
            # 데이터 추출
            for i in range(batch_size):
                for col_idx, col_name in enumerate(columns):
                    value = self.objStockChart.GetDataValue(col_idx, i)
                    data_values[col_name].append(value)
            
            total_received += batch_size
            
            # 모든 데이터 수신 완료 여부 확인
            if not self.objStockChart.Continue:
                logger.debug(f"모든 데이터 수신 완료: {request.code}")
                break
            
            # 날짜 기준 중단 조건 (날짜 기준 요청 시)
            if request.request_type == RequestType.BY_DATE and request.from_date:
                if is_minute:
                    # 분봉: 날짜+시간 비교
                    last_date = data_values['date'][-1]
                    last_time = data_values['time'][-1]
                    last_datetime = int(f"{last_date}{last_time:04}")
                    if last_datetime < request.from_date:
                        break
                else:
                    # 일봉: 날짜 비교
                    last_date = data_values['date'][-1]
                    if last_date < request.from_date:
                        break
        
        return {
            'columns': columns,
            'values': data_values
        }
    
    def _process_minute_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """분봉 데이터 처리 (날짜+시간 결합)"""
        values = data['values']
        columns = data['columns']
        
        # 'time' 컬럼이 있는지 확인
        if 'time' not in values:
            return data
        
        # 날짜와 시간을 결합하여 새로운 'datetime' 컬럼 생성
        datetimes = []
        for date_val, time_val in zip(values['date'], values['time']):
            datetime_val = int(f"{date_val}{time_val:04}")
            datetimes.append(datetime_val)
        
        # 기존 'date' 컬럼을 'datetime'으로 대체
        values['date'] = datetimes
        
        # 'time' 컬럼 제거
        del values['time']
        
        # 컬럼 목록 업데이트
        if 'time' in columns:
            time_index = columns.index('time')
            columns[time_index] = 'datetime'
        
        return {
            'columns': columns,
            'values': values
        }
    
    def request_daily(self, code: str, count: int = 1000, 
                     ohlcv_only: bool = True, from_date: Optional[int] = None) -> ChartData:
        """
        일봉 데이터 요청 (간편 메소드)
        """
        request = ChartRequest(
            code=code,
            chart_type=ChartType.DAILY,
            count=count,
            ohlcv_only=ohlcv_only,
            from_date=from_date
        )
        return self.request_chart(request)
    
    def request_minute(self, code: str, interval: int, count: int = 1000,
                      ohlcv_only: bool = True, from_date: Optional[int] = None) -> ChartData:
        """
        분봉 데이터 요청 (간편 메소드)
        """
        request = ChartRequest(
            code=code,
            chart_type=ChartType.MINUTE,
            interval=interval,
            count=count,
            ohlcv_only=ohlcv_only,
            from_date=from_date
        )
        return self.request_chart(request)


class CpCodeMgr:
    """종목 코드 관리 클래스"""
    
    # 마켓 코드 매핑
    MARKET_CODES = {
        'KOSPI': 1,
        'KOSDAQ': 2,
        'KONEX': 3
    }
    
    # 부구분 코드 매핑
    SECTION_CODES = {
        1: '주권',
        2: 'ETF',
        3: 'ETN',
        4: 'ELW',
        5: '신주인수권',
        6: '수익증권',
        10: '기타'
    }
    
    def __init__(self):
        """초기화"""
        self.objCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
        logger.info("CpCodeMgr 초기화 완료")
    
    def get_code_list(self, market: str = 'KOSPI') -> List[str]:
        """
        마켓별 종목코드 리스트 반환
        """
        market_code = self.MARKET_CODES.get(market.upper())
        if market_code is None:
            raise ValueError(f"지원하지 않는 마켓: {market}")
        
        try:
            code_list = self.objCodeMgr.GetStockListByMarket(market_code)
            logger.info(f"{market} 종목코드 수: {len(code_list)}")
            return list(code_list)
            
        except Exception as e:
            logger.error(f"종목코드 리스트 조회 실패: {e}")
            raise
    
    def get_code_name(self, code: str) -> str:
        """종목코드로 종목명 조회"""
        try:
            name = self.objCodeMgr.CodeToName(code)
            return name if name else ""
            
        except Exception as e:
            logger.error(f"종목명 조회 실패: {code}, {e}")
            return ""
    
    def get_section_code(self, code: str) -> int:
        """종목의 부구분코드 조회"""
        try:
            section = self.objCodeMgr.GetStockSectionKind(code)
            return section
            
        except Exception as e:
            logger.error(f"부구분코드 조회 실패: {code}, {e}")
            return 0
    
    def get_section_name(self, code: str) -> str:
        """종목의 부구분명 조회"""
        section_code = self.get_section_code(code)
        return self.SECTION_CODES.get(section_code, '알수없음')
    
    def is_etf(self, code: str) -> bool:
        """ETF 여부 확인"""
        section_code = self.get_section_code(code)
        return section_code == 2
    
    def is_etn(self, code: str) -> bool:
        """ETN 여부 확인"""
        section_code = self.get_section_code(code)
        return section_code == 3
    
    def is_delisted(self, code: str) -> bool:
        """상장폐지 여부 확인"""
        try:
            # 종목명이 없으면 상장폐지로 간주
            name = self.get_code_name(code)
            return not bool(name)
            
        except Exception:
            return True
    
    def get_market_type(self, code: str) -> Optional[str]:
        """종목의 마켓 타입 조회"""
        try:
            # 모든 마켓에서 코드 검색
            for market_name, market_code in self.MARKET_CODES.items():
                code_list = self.objCodeMgr.GetStockListByMarket(market_code)
                if code in code_list:
                    return market_name
            return None
            
        except Exception as e:
            logger.error(f"마켓 타입 조회 실패: {code}, {e}")
            return None
    
    def get_stock_info(self, code: str) -> Dict[str, Any]:
        """종목 상세 정보 조회"""
        try:
            info = {
                'code': code,
                'name': self.get_code_name(code),
                'market_type': self.get_market_type(code),
                'section_code': self.get_section_code(code),
                'section_name': self.get_section_name(code),
                'is_etf': self.is_etf(code),
                'is_etn': self.is_etn(code),
                'is_delisted': self.is_delisted(code)
            }
            return info
            
        except Exception as e:
            logger.error(f"종목 정보 조회 실패: {code}, {e}")
            return {
                'code': code,
                'name': '',
                'market_type': None,
                'section_code': 0,
                'section_name': '알수없음',
                'is_etf': False,
                'is_etn': False,
                'is_delisted': True
            }


class CreonAPIManager:
    """Creon API 통합 관리 클래스"""
    
    def __init__(self):
        """초기화"""
        self.stock_chart = CpStockChart()
        self.code_mgr = CpCodeMgr()
        self._connection_checked = False
        logger.info("CreonAPIManager 초기화 완료")
    
    def check_connection(self) -> bool:
        """Creon 연결 상태 확인"""
        try:
            objCpStatus = win32com.client.Dispatch('CpUtil.CpCybos')
            bConnect = objCpStatus.IsConnect
            
            if bConnect == 0:
                logger.error("Creon PLUS 연결되지 않음")
                return False
            
            self._connection_checked = True
            logger.info("Creon PLUS 연결 상태: 정상")
            return True
            
        except Exception as e:
            logger.error(f"Creon 연결 확인 실패: {e}")
            return False
    
    def get_market_codes(self, market: str = 'KOSPI',
                        filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        마켓별 종목코드 리스트 반환 (필터 적용)
        MH_creon_datareader_down_20260106.py 필터링 로직 적용
        """
        if not self._connection_checked:
            if not self.check_connection():
                raise ConnectionError("Creon 연결 실패")

        if filters is None:
            filters = {}

        code_list = self.code_mgr.get_code_list(market)

        # 'A' 접두어 제거 후 'K' 포함 종목 제거 (MH 원본 로직)
        filtered_codes = []
        for code in code_list:
            clean_code = code.replace('A', '')
            if 'K' not in clean_code:
                filtered_codes.append(clean_code)

        # 종목 정보 수집
        stocks = []
        for code in filtered_codes:
            try:
                # 'A' 접두어 붙여서 조회 (MH 원본 로직)
                full_code = 'A' + code
                name = self.code_mgr.get_code_name(full_code)

                # 필터 적용
                if self._apply_stock_filter(code, name, filters):
                    stocks.append({
                        'code': full_code,
                        'clean_code': code,
                        'name': name,
                        'market_type': market
                    })

            except Exception as e:
                logger.warning(f"종목 정보 처리 실패: {code}, {e}")
                continue

        logger.info(f"필터 적용 후 종목 수: {len(stocks)}/{len(filtered_codes)}")
        return stocks

    def _apply_stock_filter(self, code: str, name: str, filters: Dict[str, Any]) -> bool:
        """
        종목 필터 적용 (MH_creon_datareader_down_20260106.py 로직 기반)
        """
        if not name:
            return False

        # ETF/ETN 제외
        if filters.get('exclude_etf', False) and 'ETF' in name.upper():
            return False
        if filters.get('exclude_etn', False) and 'ETN' in name.upper():
            return False

        # 상장폐지 제외
        if filters.get('exclude_delisted', False) and not name:
            return False

        # 키워드 필터 (MH 원본: 정규식 str.contains('|'.join(exclude_keywords)))
        exclude_keywords = filters.get('exclude_keywords', [])
        if exclude_keywords:
            import re
            pattern = '|'.join(re.escape(kw) for kw in exclude_keywords if kw)
            if pattern and re.search(pattern, name, re.IGNORECASE):
                return False

        # 가격/거래대금 필터 (단일 COM 호출로 통합)
        price_min = filters.get('price_min', 0)
        price_max = filters.get('price_max', 0)
        amount_min = filters.get('amount_min', 0)
        amount_max = filters.get('amount_max', 0)
        if price_min > 0 or price_max > 0 or amount_min > 0 or amount_max > 0:
            try:
                price, amount = self._get_stock_market_data('A' + code)
                if price == 0 and amount == 0:
                    return False
                if price_min > 0 and price < price_min:
                    return False
                if price_max > 0 and price > price_max:
                    return False
                if amount_min > 0 and amount < amount_min:
                    return False
                if amount_max > 0 and amount > amount_max:
                    return False
            except Exception:
                pass

        return True

    def _get_stock_market_data(self, code: str):
        """현재가와 거래대금을 한번의 COM 호출로 조회"""
        try:
            objStockMst = win32com.client.Dispatch("DsCbo1.StockMst")
            objStockMst.SetInputValue(0, code)
            objStockMst.BlockRequest()
            price = objStockMst.GetHeaderValue(11)
            volume = objStockMst.GetHeaderValue(18)
            amount = (volume * price) // 1000000
            return price, amount
        except Exception as e:
            logger.warning(f"시세 데이터 조회 실패: {code}, {e}")
            return 0, 0
    
    def get_daily_data(self, code: str, **kwargs) -> ChartData:
        """일봉 데이터 가져오기"""
        return self.stock_chart.request_daily(code, **kwargs)
    
    def get_minute_data(self, code: str, interval: int, **kwargs) -> ChartData:
        """분봉 데이터 가져오기"""
        return self.stock_chart.request_minute(code, interval, **kwargs)


# 전역 API 관리자 인스턴스
_api_manager: Optional[CreonAPIManager] = None


def get_api_manager() -> CreonAPIManager:
    """전역 API 관리자 인스턴스 반환"""
    global _api_manager
    
    if _api_manager is None:
        _api_manager = CreonAPIManager()
    
    return _api_manager


if __name__ == "__main__":
    # 모듈 테스트
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # API 관리자 생성
        api = get_api_manager()
        
        # 연결 테스트
        if api.check_connection():
            print("Creon 연결 성공")
            
            # 종목코드 테스트
            stocks = api.get_market_codes('KOSPI', {
                'exclude_etf': True,
                'exclude_etn': True,
                'exclude_delisted': True
            })
            
            print(f"KOSPI 종목 수: {len(stocks)}")
            
            if stocks:
                # 첫 번째 종목 정보 출력
                sample = stocks[0]
                print(f"\n샘플 종목:")
                for key, value in sample.items():
                    print(f"  {key}: {value}")
                
                # 일봉 데이터 테스트 (옵션)
                test_code = sample['code']
                print(f"\n{test_code} 일봉 데이터 테스트...")
                
                try:
                    daily_data = api.get_daily_data(test_code, count=10)
                    print(f"수신된 데이터: {daily_data.received_count}개")
                    print(f"컬럼: {daily_data.columns}")
                    
                    if daily_data.data and 'date' in daily_data.data:
                        dates = daily_data.data['date'][:5]
                        closes = daily_data.data['close'][:5]
                        print(f"최근 5일 종가: {list(zip(dates, closes))}")
                        
                except Exception as e:
                    print(f"데이터 요청 실패: {e}")
        
        else:
            print("Creon 연결 실패")
            
    except Exception as e:
        print(f"테스트 실패: {e}")
        import traceback
        traceback.print_exc()