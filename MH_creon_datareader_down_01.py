# coding=utf-8
# 표준 라이브러리
import sys
import os
import gc
import warnings
import json
from datetime import datetime, time
from pathlib import Path

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

# 로컬 모듈
import creonAPI
import decorators
from utils import is_market_open, available_latest_date, preformat_cjk

# 경고 메시지 설정
warnings.simplefilter(action='ignore', category=FutureWarning)

# KRX 캘린더 초기화
krx_calendar = get_calendar('XKRX')

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
            'tick_unit': '분봉',  # '분봉' 또는 '일봉'
            'tick_range': 30,      # 분봉의 경우 5분봉
            'count': 1000,      # 데이터 개수
            'ohlcv_only': True,   # OHLCV 데이터만 가져오기 여부
            'price_min': 10000,     # 최소 가격
            'price_max': 30000,  # 최대 가격
            'max_stocks': 0      # 최대 종목 수
        }

        # 데이터 저장 경로 설정
        self.setup_data_directory()

    def setup_data_directory(self):
        """데이터 저장 디렉토리 설정"""
        # 기본 JSON 폴더 생성
        self.base_dir = Path('json_data')
        self.base_dir.mkdir(exist_ok=True)

        # 현재 날짜와 시간으로 하위 폴더 생성
        current_time = datetime.now()
        self.data_dir = self.base_dir / current_time.strftime('%Y%m%d_%H%M%S')
        self.data_dir.mkdir(exist_ok=True)

        print(f"데이터 저장 경로: {self.data_dir}")

    def connect_code_list_view_mh(self):
        """종목 코드 리스트를 가져와서 처리하는 메서드"""
        onday = self.date_delta()
        print(f"최근 개장일인 {onday}기준")
        
        # KOSPI 종목 데이터 가져오기
        df_KOSPI = stock.get_market_ohlcv(onday, market="KOSPI")
        
        # 가격 필터링
        df_KOSPI_filtered = df_KOSPI[
            (df_KOSPI['종가'] <= self.settings['price_max']) & 
            (df_KOSPI['종가'] >= self.settings['price_min'])
        ]
        ticker_list = df_KOSPI_filtered.index.tolist()
        
        # 'K' 포함 종목 제거
        code_list = [item for item in ticker_list if 'K' not in item]
        print(f"K 제거후 남은 리스트 수 : {len(code_list)}")
        
        # 종목명 가져오기
        name_list = [stock.get_market_ticker_name(code) for code in code_list]
        
        # 데이터프레임 생성
        sv_code_df = pd.DataFrame({
            '종목코드': code_list,
            '종목명': name_list
        }, columns=('종목코드', '종목명'))

        # 특정 문자열 포함 종목 제거
        exclude_keywords = ['KODEX', 'TIGER', 'ACE', '액티브', 'KOSEF', 'ARIRANG', 
                          '블룸버그', '합성', 'SOL', '스팩', 'HANARO']
        
        for keyword in exclude_keywords:
            sv_code_df['종목명'] = sv_code_df['종목명'].replace(keyword, '')

        # 코드에 'A' 접두어 추가
        sv_code_list = ['A' + item for item in sv_code_df['종목코드'].tolist()]
        
        # max_stocks가 0이면 모든 종목을 처리하고, 그렇지 않으면 설정된 수만큼만 처리
        if self.settings['max_stocks'] > 0:
            sv_code_list = sv_code_list[:self.settings['max_stocks']]
        
        # 최종 데이터프레임 생성
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({
            '종목코드': sv_code_list,
            '종목명': sv_name_list
        }, columns=('종목코드', '종목명'))

    def save_to_json(self, code, df):
        """데이터프레임을 JSON 파일로 저장"""
        try:
            # 파일명 생성
            if self.settings['tick_unit'] == '분봉':
                filename = f"{code}_{self.settings['tick_range']}분봉.json"
            else:
                filename = f"{code}_{self.settings['tick_unit']}.json"

            # 데이터프레임을 JSON 형식으로 변환
            df_json = df.to_dict(orient='records')
            
            # 메타데이터 추가
            data = {
                'meta': {
                    'code': code,
                    'tick_unit': self.settings['tick_unit'],
                    'tick_range': self.settings['tick_range'],
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'columns': df.columns.tolist()
                },
                'data': df_json
            }
            
            # JSON 파일로 저장
            filepath = self.data_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            print(f"데이터 저장 중 오류 발생: {e}")
            print(f"데이터프레임 정보:")
            print(f"컬럼: {df.columns.tolist()}")
            print(f"인덱스: {df.index.name}")
            print(f"데이터프레임 내용:")
            print(df.head())

    def update_price_db(self, filtered=False):
        """가격 데이터 업데이트"""
        fetch_code_df = self.sv_code_df
        db_code_df = self.db_code_df

        print(f"데이터 저장 경로: {self.data_dir}")
        print(f"처리할 종목 수: {len(fetch_code_df)}")
        print("처리 시작 시간: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # 시작 시간 기록
        start_time = datetime.now()

        # 컬럼 설정
        columns = ['open', 'high', 'low', 'close', 'volume']
        if not self.settings['ohlcv_only']:
            columns.extend(['상장주식수', '외국인주문한도수량', '외국인현보유수량', 
                          '외국인현보유비율', '기관순매수', '기관누적순매수'])

        # 시장이 닫혀있을 때만 데이터 업데이트
        if not is_market_open():
            latest_date = available_latest_date()
            if self.settings['tick_unit'] == '일봉':
                latest_date = latest_date // 10000

            total_stocks = len(fetch_code_df)
            for i in range(total_stocks):
                code = fetch_code_df.iloc[i]
                self.update_status_msg = f'[{code[0]}] {code[1]}'
                
                # 현재까지 걸린 시간 계산
                current_time = datetime.now()
                elapsed_time = current_time - start_time
                elapsed_minutes = elapsed_time.total_seconds() / 60
                
                # 진행 상황 표시 (시간 포함)
                print(f"\r{i+1}/{total_stocks} ({((i+1)/total_stocks*100):.1f}%) {self.update_status_msg} - 경과시간: {elapsed_minutes:.1f}분", end='')

                from_date = 0
                if self.settings['tick_unit'] == '일봉':
                    if not self.objStockChart.RequestDWM(code[0], ord('D'), 
                                                       self.settings['count'], 
                                                       self, from_date, 
                                                       self.settings['ohlcv_only']):
                        continue
                else:  # 분봉
                    if not self.objStockChart.RequestMT(code[0], ord('m'), 
                                                      self.settings['tick_range'], 
                                                      self.settings['count'], 
                                                      self, from_date, 
                                                      self.settings['ohlcv_only']):
                        continue

                # 데이터프레임 생성 및 처리
                df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])

                if from_date != 0:
                    df = df.loc[:from_date].iloc[:-1]

                df = df.iloc[::-1]  # 날짜 기준 오름차순 정렬

                # 인덱스를 컬럼으로 변환
                df = df.reset_index()
                df = df.rename(columns={'index': 'date'})

                # JSON 파일로 저장
                self.save_to_json(code[0], df)

                # 메모리 정리
                del df
                gc.collect()

            print()  # 줄바꿈
            
            # 전체 처리 시간 계산 및 표시
            end_time = datetime.now()
            total_time = end_time - start_time
            total_minutes = total_time.total_seconds() / 60
            print(f"\n처리 완료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"총 소요 시간: {total_minutes:.1f}분")

        self.update_status_msg = ''
        self.connect_code_list_view_mh()
        return fetch_code_df

    def update_price_db_filtered(self):
        """필터링된 가격 데이터 업데이트"""
        self.update_price_db(filtered=True)

    def in_time(self):
        """현재 시간이 개장 시간인지 확인"""
        current_time = datetime.now().time()
        start_time = time(9, 0)
        end_time = time(15, 30)
        
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
    reader = CreonDataReader()
    reader.connect_code_list_view_mh()
    reader.update_price_db()

if __name__ == "__main__":
    main() 