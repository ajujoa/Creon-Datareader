# coding=utf-8
# 표준 라이브러리
import sys
import os
import gc
import time
import logging
import traceback
import warnings
from datetime import datetime
from pathlib import Path
import pywintypes

# 서드파티 라이브러리
import numpy as np
import pandas as pd
import tqdm
import matplotlib.pyplot as plt

# pandas 엑셀 관련 경고 무시
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# 금융 데이터 관련
from pykrx import stock
from exchange_calendars import get_calendar
try:
    import FinanceDataReader as fdr  # type: ignore
    FDR_AVAILABLE = True
except ImportError:
    FDR_AVAILABLE = False
    fdr = None  # type: ignore

# 로컬 모듈
import creonAPI
import decorators
import creon_launcher
from utils import is_market_open, available_latest_date, preformat_cjk

# 경고 메시지 설정
warnings.simplefilter(action='ignore', category=FutureWarning)

# KRX 캘린더 초기화
krx_calendar = get_calendar('XKRX')

# 로거 설정
logger = logging.getLogger('creon_chart')

def setup_logging():
    """로그 설정: 콘솔(INFO) + 파일(DEBUG)"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"creon_chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # 루트 로거 설정
    logger.setLevel(logging.DEBUG)

    # 파일 핸들러 - 모든 레벨
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)-7s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # 콘솔 핸들러 - INFO 이상만
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('[%(levelname)-7s] %(message)s'))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return log_file

class CreonDataReader:
    def __init__(self):
        # Creon API 객체 초기화
        self.objStockChart = creonAPI.CpStockChart()
        self.objCodeMgr = creonAPI.CpCodeMgr()

        # 데이터 저장용 변수 초기화
        self.rcv_data = dict()
        self.update_status_msg = ''
        self.return_status_msg = ''

        # 종목코드 관련 데이터프레임 초기화
        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()

        # 설정값 초기화
        self.settings = {
            'tick_unit': '분봉',       # '분봉' 또는 '일봉'
            'tick_range': 3,           # 분봉 간격 (1, 3, 5, 10, 15, 30, 60)
            'count': 65000,            # 데이터 개수 (3분봉 최대 ~65,000)
            'ohlcv_only': True,        # OHLCV 데이터만 가져오기 여부
            'market_types': ['KOSPI', 'KOSDAQ'],  # 수집 대상 시장
            'max_stocks': 0            # 최대 종목 수
        }

        # DB 연결 (2pc MariaDB 직접)
        self._db_conn = None
        self._load_db_config()

        # 필터 설정
        self._load_filter_config()

        # 데이터 저장 경로 설정
        self.setup_data_directory()

    def setup_data_directory(self):
        """데이터 저장 디렉토리 설정 (분봉별 폴더)"""
        base_dir = Path('json_data')
        tick_unit = self.settings['tick_unit']
        if tick_unit == '분봉':
            folder_name = f"{self.settings['tick_range']}min"
        else:
            folder_name = tick_unit  # '일봉'
        self.data_dir = base_dir / folder_name
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"데이터 저장 경로: {self.data_dir}")

    def _load_db_config(self):
        """2pc MariaDB 연결 설정 로드"""
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read('creon_config.ini', encoding='utf-8')
        db = cfg['database']
        self.db_config = {
            'host': db.get('host', '64.176.226.37'),
            'port': db.getint('port', fallback=3306),
            'user': db.get('user', 'MH_TR_DB'),
            'password': db.get('password', ''),
            'database': db.get('database', 'MH_TR_DB'),
            'table': db.get('table', 'minute_data_3min'),
            'charset': db.get('charset', 'utf8mb4'),
        }

    def _load_filter_config(self):
        """종목명 필터 키워드를 INI에서 로드"""
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read('creon_config.ini', encoding='utf-8')
        raw = cfg.get('filter', 'exclude_keywords', fallback='')
        self.exclude_keywords = [kw.strip() for kw in raw.split(',') if kw.strip()]
        logger.info(f"필터 키워드 (INI): {self.exclude_keywords}")

    def _get_db_conn(self):
        """Lazy DB 연결 (첫 사용 시 연결, 이후 재사용)"""
        if self._db_conn is None:
            import pymysql
            self._db_conn = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset=self.db_config['charset'],
                connect_timeout=10,
                read_timeout=60,
                write_timeout=60,
            )
        else:
            # ponytail: 커넥션 데드 체크 — ping 실패 시 재연결
            try:
                self._db_conn.ping()
            except Exception:
                try:
                    self._db_conn.close()
                except Exception:
                    pass
                self._db_conn = None
                return self._get_db_conn()
        return self._db_conn

    def _upload_to_db(self, code, df, from_date=0):
        """새 데이터를 2pc MariaDB로 직접 업로드 (증분)

        datetime: Creon raw serial (원본 호환)
        time: HHMMSS (거래일 내 연속 candle index)
        """
        if df.empty:
            return 0
        try:
            conn = self._get_db_conn()
            table = self.db_config['table']

            # ponytail: from_date를 max_dt로 사용 (SELECT 생략)
            max_dt = from_date

            # 새 데이터만 필터
            new_rows = df[df['datetime'] > max_dt]
            if new_rows.empty:
                return 0

            # day_block 기준 그룹화 → 연속 candle index
            from collections import defaultdict
            day_groups = defaultdict(list)
            for _, r in new_rows.iterrows():
                dt = int(r['datetime'])
                day_block = dt // 10000  # Creon day_block (원본 포맷 그대로)
                day_groups[day_block].append((dt, r))

            batch = []
            for day_block in sorted(day_groups):
                candles = sorted(day_groups[day_block], key=lambda x: x[0])
                for ci, (dt, r) in enumerate(candles):
                    total_min = 9 * 60 + ci * 3
                    if total_min > 15 * 60 + 30:  # cap at 15:30
                        total_min = 15 * 60 + 30
                    h, m = divmod(total_min, 60)
                    time_val = h * 10000 + m * 100

                    batch.append((
                        code, dt, time_val,
                        int(r['open']), int(r['high']), int(r['low']),
                        int(r['close']), int(r['volume']),
                    ))

            with conn.cursor() as cur:
                BATCH_SIZE = 500
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    ph = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(chunk))
                    flat = [v for row in chunk for v in row]
                    cur.execute(
                        f"INSERT IGNORE INTO {table} "
                        "(stock_code, datetime, time, open, high, low, close, volume) "
                        f"VALUES {ph}", flat)
                    conn.commit()

            return len(batch)
        except Exception as e:
            logger.warning(f"[{code}] DB 업로드 실패: {e}")
            return 0

    def _close_db(self):
        """DB 연결 종료"""
        if self._db_conn:
            try:
                self._db_conn.close()
            except Exception:
                pass
            self._db_conn = None

    def connect_code_list_view_mh(self):
        """종목 코드 리스트를 가져와서 처리하는 메서드"""
        onday = self.date_delta()
        logger.info(f"최근 개장일인 {onday} 기준")

        # KOSPI 종목 리스트 가져오기 (FinanceDataReader 사용)
        code_list = []
        name_list = []
        fdr_available = FDR_AVAILABLE  # 전역 변수를 로컬 변수로 복사

        # FinanceDataReader로 시도
        if fdr_available:
            try:
                # 전체 종목 리스트 가져오기
                logger.debug("FinanceDataReader로 KRX 종목 리스트 요청 중...")
                df_stocks = fdr.StockListing('KRX')

                # KOSPI + KOSDAQ 필터링
                market_types = self.settings['market_types']
                if 'Market' in df_stocks.columns:
                    market_df = df_stocks[df_stocks['Market'].isin(market_types)].copy()

                    if len(market_df) > 0:
                        logger.info(f"FinanceDataReader로 종목 리스트 가져옴: {len(market_df)}개")
                        logger.info(f"대상 시장: {market_types}")

                        # 종목 코드와 종목명 추출
                        if 'Symbol' in market_df.columns and 'Name' in market_df.columns:
                            # 'K' 포함 종목 제거
                            filtered_df = market_df[~market_df['Symbol'].str.contains('K', na=False)]
                            code_list = filtered_df['Symbol'].tolist()
                            name_list = filtered_df['Name'].tolist()
                            logger.info(f"K 제거후 남은 리스트 수 : {len(code_list)}")
                        else:
                            logger.warning("필요한 컬럼(Symbol, Name)을 찾을 수 없습니다.")
                            raise ValueError("Missing columns")
                    else:
                        logger.warning("FinanceDataReader가 빈 리스트를 반환했습니다.")
                        raise ValueError("Empty list")
                else:
                    logger.warning("Market 컬럼을 찾을 수 없습니다.")
                    raise ValueError("Missing Market column")

            except Exception as e:
                logger.warning(f"FinanceDataReader 데이터 가져오기 오류: {e}")
                logger.info("Creon API를 사용하여 종목 리스트를 가져옵니다.")
                fdr_available = False  # 다음 시도 방지

        # FinanceDataReader 실패 시 Creon API 사용
        if not fdr_available or not code_list:
            try:
                # 시장 코드 매핑
                market_map = {'KOSPI': 1, 'KOSDAQ': 2}
                target_markets = self.settings['market_types']
                logger.debug(f"Creon API로 {target_markets} 종목 리스트 요청 중...")

                creon_code_list = []
                for market_name in target_markets:
                    market_code = market_map.get(market_name, 1)
                    codes = self.objCodeMgr.get_code_list(market_code)
                    creon_code_list.extend(codes)
                    logger.debug(f"  {market_name}({market_code}): {len(codes)}종목")

                if not creon_code_list:
                    logger.error("Creon API로도 종목 리스트를 가져올 수 없습니다.")
                    return

                # 'A' 접두어 제거 및 'K' 포함 종목 제거
                code_list = [code.replace('A', '') for code in creon_code_list if 'K' not in code]
                logger.info(f"Creon API로 종목 리스트 가져옴: {len(code_list)}개")

                # 종목명 가져오기
                name_list = []
                for code in code_list:
                    try:
                        name = self.objCodeMgr.get_code_name('A' + code)
                        name_list.append(name)
                    except:
                        name_list.append('')

            except Exception as creon_error:
                logger.error(f"Creon API 오류: {creon_error}")
                traceback.print_exc()
                return

        # 데이터프레임 생성
        sv_code_df = pd.DataFrame({
            '종목코드': code_list,
            '종목명': name_list
        }, columns=('종목코드', '종목명'))

        # 특정 문자열 포함 종목 제거
        exclude_keywords = self.exclude_keywords

        logger.info(f"필터링 시작 - 전체: {len(sv_code_df)}종목")
        logger.info(f"제외 키워드: {exclude_keywords}")
        logger.info(f"ETF/ETN/상장폐지 제외: 활성화")

        # 1) 키워드 필터: 종목명에 키워드가 포함된 행 제거
        before_filter = len(sv_code_df)
        mask = sv_code_df['종목명'].str.contains('|'.join(exclude_keywords), na=False, case=False)
        sv_code_df = sv_code_df[~mask]
        logger.info(f"  키워드 필터: {before_filter} → {len(sv_code_df)}종목 ({before_filter - len(sv_code_df)}종목 제외)")

        # 2) ETF/ETN 명시적 제외: 종목명에 'ETF' 또는 'ETN' 포함 여부
        before_etf = len(sv_code_df)
        etf_etn_mask = sv_code_df['종목명'].str.contains('ETF|ETN', na=False, case=False)
        sv_code_df = sv_code_df[~etf_etn_mask]
        logger.info(f"  ETF/ETN 제외: {before_etf} → {len(sv_code_df)}종목 ({before_etf - len(sv_code_df)}종목 제외)")

        # 3) 상장폐지 제외: 종목명이 비어있거나 '상장폐지' 포함
        before_delisted = len(sv_code_df)
        delisted_mask = sv_code_df['종목명'].isna() | (sv_code_df['종목명'] == '') | sv_code_df['종목명'].str.contains('상장폐지', na=False)
        sv_code_df = sv_code_df[~delisted_mask]
        logger.info(f"  상장폐지 제외: {before_delisted} → {len(sv_code_df)}종목 ({before_delisted - len(sv_code_df)}종목 제외)")

        # 코드에 'A' 접두어 추가
        sv_code_list = ['A' + item for item in sv_code_df['종목코드'].tolist()]

        # max_stocks가 0이면 모든 종목을 처리하고, 그렇지 않으면 설정된 수만큼만 처리
        if self.settings['max_stocks'] > 0:
            sv_code_list = sv_code_list[:self.settings['max_stocks']]
            logger.info(f"최대 종목 제한 적용: {self.settings['max_stocks']}종목")

        # 최종 데이터프레임 생성
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({
            '종목코드': sv_code_list,
            '종목명': sv_name_list
        }, columns=('종목코드', '종목명'))
        logger.info(f"최종 수집 대상: {len(self.sv_code_df)}종목")

    def _get_db_latest_datetime(self, code):
        """DB에서 특정 종목의 최신 datetime 조회"""
        try:
            conn = self._get_db_conn()
            table = self.db_config['table']
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX(datetime) FROM {table} WHERE stock_code = %s",
                    (code,))
                row = cur.fetchone()
                if row and row[0]:
                    return int(row[0])
        except Exception as e:
            logger.debug(f"[{code}] DB 최신 datetime 조회 실패: {e}")
        return 0

    def update_price_db(self, filtered=False):
        """가격 데이터 업데이트"""
        fetch_code_df = self.sv_code_df
        db_code_df = self.db_code_df

        tick_unit = self.settings['tick_unit']
        tick_range = self.settings['tick_range'] if tick_unit == '분봉' else None

        logger.info(f"데이터 수집 시작: {tick_unit}" + (f" {tick_range}분" if tick_range else ""))
        logger.info(f"데이터 저장 경로: {self.data_dir}")
        logger.info(f"처리할 종목 수: {len(fetch_code_df)}")
        logger.info(f"설정: count={self.settings['count']}, ohlcv_only={self.settings['ohlcv_only']}, max_stocks={self.settings['max_stocks']}")

        # 시작 시간 기록
        start_time = datetime.now()
        logger.debug(f"처리 시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 시장이 닫혀있을 때만 데이터 업데이트
        if not is_market_open():
            latest_date = available_latest_date()
            if self.settings['tick_unit'] == '일봉':
                latest_date = latest_date // 10000

            total_stocks = len(fetch_code_df)
            success_count = 0
            update_count = 0     # 증분 업데이트 (기존 데이터 있음)
            new_count = 0        # 신규 다운로드 (기존 데이터 없음)
            skip_count = 0
            error_count = 0

            for i in range(total_stocks):
                code = fetch_code_df.iloc[i]
                stock_code = code[0]
                stock_name = code[1]
                self.update_status_msg = f'[{stock_code}] {stock_name}'

                # 현재까지 걸린 시간 계산
                current_time = datetime.now()
                elapsed_time = current_time - start_time
                elapsed_minutes = elapsed_time.total_seconds() / 60

                # 진행 상황 표시 (시간 포함)
                progress_msg = f"\r{i+1}/{total_stocks} ({((i+1)/total_stocks*100):.1f}%) {self.update_status_msg} - 경과시간: {elapsed_minutes:.1f}분\033[K"
                print(progress_msg, end='')

                try:
                    # 증분 업데이트: DB에서 최신 datetime 조회 (PK 인덱스, 즉시 응답)
                    is_minute = self.settings['tick_unit'] == '분봉'
                    from_date = self._get_db_latest_datetime(stock_code)

                    if self.settings['tick_unit'] == '일봉':
                        if not self.objStockChart.RequestDWM(stock_code, ord('D'), 
                                                           self.settings['count'], 
                                                           self, from_date, 
                                                           self.settings['ohlcv_only']):
                            reason = "신규 (Creon에도 없음)" if from_date == 0 else "최신 (변경 없음)"
                            logger.debug(f"[{stock_code}] {stock_name} {reason}")
                            skip_count += 1
                            continue
                    else:  # 분봉
                        # ponytail: 원본처럼 BY_COUNT만 사용, RequestMT 내부에서 from_date 비교로 중단
                        if not self.objStockChart.RequestMT(stock_code, ord('m'), 
                                                          self.settings['tick_range'], 
                                                          self.settings['count'], 
                                                          self, from_date,
                                                          ohlcv_only=self.settings['ohlcv_only']):
                            reason = "신규 (Creon에도 없음)" if from_date == 0 else "최신 (변경 없음)"
                            logger.debug(f"[{stock_code}] {stock_name} {reason}")
                            skip_count += 1
                            continue

                    # 데이터프레임 생성 및 처리
                    if is_minute:
                        # 분봉: RequestMT에서 date+time을 이미 결합했으므로 date를 인덱스로 사용
                        columns = ['open', 'high', 'low', 'close', 'volume']
                        df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
                    else:
                        # 일봉: date를 인덱스로 사용
                        columns = ['open', 'high', 'low', 'close', 'volume']
                        df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])

                    # ponytail: 교집합 — from_date 포함해 Creon 응답에서 잘라내고
                    # _upload_to_db에서 datetime > from_date 필터링으로 중복 제거
                    if from_date != 0:
                        df = df.loc[:from_date].iloc[:-1]

                    df = df.iloc[::-1]  # 날짜 기준 오름차순 정렬

                    # 인덱스를 컬럼으로 변환
                    df = df.reset_index()
                    if is_minute:
                        df = df.rename(columns={'index': 'datetime'})
                        # datetime: Creon raw serial
                        df['datetime'] = df['datetime'].astype(int)
                    else:
                        df = df.rename(columns={'index': 'date'})

                    # DB에 업로드만 (JSON 저장 안함)
                    if not df.empty:
                        uploaded = self._upload_to_db(stock_code, df, from_date)
                    else:
                        uploaded = 0

                    success_count += 1
                    if from_date == 0:
                        new_count += 1
                    else:
                        update_count += 1

                    # 메모리 정리
                    del df
                    gc.collect()

                    # ponytail: 0.3s는 RequestMT 내부에서 이미 처리됨
                except pywintypes.com_error as e:
                    logger.warning(f"[{stock_code}] {stock_name} com_error: {e}")
                    error_count += 1
                    continue
                except Exception as e:
                    logger.error(f"[{stock_code}] {stock_name} 예상치 못한 오류: {e}")
                    traceback.print_exc()
                    error_count += 1
                    continue

            print()  # 줄바꿈

            # 전체 처리 시간 계산 및 표시
            end_time = datetime.now()
            total_time = end_time - start_time
            total_minutes = total_time.total_seconds() / 60
            total_seconds = total_time.total_seconds()

            logger.info(f"처리 완료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"총 소요 시간: {total_minutes:.1f}분 ({total_seconds:.0f}초)")
            logger.info(f"결과: 성공={success_count} (신규:{new_count}, 업데이트:{update_count}), 데이터없음={skip_count}, 오류={error_count}, 전체={total_stocks}")

        else:
            logger.warning("장 중이므로 데이터 수집을 건너뜁니다.")

        self.update_status_msg = ''
        self.connect_code_list_view_mh()
        return fetch_code_df

    def update_price_db_filtered(self):
        """필터링된 가격 데이터 업데이트"""
        self.update_price_db(filtered=True)

    def in_time(self):
        """현재 시간이 개장 시간인지 확인"""
        current_time = datetime.now().time()
        start_time = datetime.time(9, 0)
        end_time = datetime.time(15, 30)
        
        if start_time <= current_time <= end_time:
            return "in_time"
        return "Before" if start_time > current_time else "after"

    def is_open(self):
        """오늘이 개장일인지 확인"""
        today = datetime.now().date()
        return krx_calendar.is_session(today)

    def date_delta(self):
        """거래일 기준 1일 전 날짜 반환"""
        today = datetime.now().date()
        date_delta = today - 1 * krx_calendar.day
        return date_delta.strftime("%Y%m%d")


def main():
    """메인 실행 함수"""

    log_file = setup_logging()
    logger.info(f"Creon Chart Download 시작")
    logger.info(f"로그 파일: {log_file}")

    # Creon Plus 자동 실행 및 로그인
    if not creon_launcher.ensure_connected():
        logger.error("Creon Plus 연결 실패. 프로그램을 종료합니다.")
        return
    print()
    print("========================================")
    print("  Creon DataReader MH")
    print("========================================")
    print()
    print("  3분봉 데이터를 수집하여 MariaDB에 업로드합니다.")
    print()

    reader = None
    try:
        reader = CreonDataReader()
        logger.info(f"설정: {reader.settings}")
        reader.connect_code_list_view_mh()
        reader.update_price_db()
        logger.info("프로그램 정상 종료")
    except Exception as e:
        logger.error(f"프로그램 비정상 종료: {e}")
        traceback.print_exc()
        raise
    finally:
        if reader:
            reader._close_db()


if __name__ == "__main__":
    main()