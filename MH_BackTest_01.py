import os
import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import time
from pathlib import Path
import json
### TA 패키지를 이용한 볼린져 밴드 계산 함수
import ta
#from ta.volatility import BollingerBands
#from ta.Trend import EMAIndicator
#from ta import add_all_ta_features
#from ta.Trend import ADXIndicator
from pykrx import stock
from exchange_calendars import get_calendar
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm

import matplotlib.pyplot as plt


# 현재 날짜와 시간을 가져옵니다.
current_datetime = datetime.now()
formatted_date = current_datetime.strftime("%m%d-%H%M")
excel_filename = formatted_date + ".xlsx"


total_result_df_all = pd.DataFrame()
total_result_df_all.name = 'total_result_df_all'
total_result_df = pd.DataFrame()
total_result_df.name = 'total_result_df'

#일봉 등 데이터 불러올 기준 날짜.
krx_calendar = get_calendar("XKRX")

############################################################


# 초기 설정
Total_result2 = pd.DataFrame(columns=['Stock Code', 'Plus Count', 'Minus Count', 'Total Rows', 'Total Profit'])
Total_result3 = pd.DataFrame(columns=['date', 'table_name', 'buy_price', 'sell_price', 'result', 'profit'])
tables_count = 0

# 전역 변수로 프로세스 수 설정
MAX_WORKERS = 10  # 기본값 설정
CHUNK_SIZE = 20  # 한 번에 처리할 파일 수

def save_results(results, folder_path, condition_set=1):
    """
    결과를 JSON 파일로 저장하는 함수 (유효한 결과만 저장)
    
    :param results: 저장할 결과 데이터
    :param folder_path: 결과를 저장할 폴더 경로
    :param condition_set: 사용된 조건 세트 번호
    :return: 저장된 파일의 경로 또는 None (저장할 데이터가 없는 경우)
    """
    # valid_dates가 있는 결과만 필터링
    valid_results = [result for result in results if result.get('valid_dates')]
    
    # 저장할 유효한 결과가 없으면 파일을 저장하지 않음
    if not valid_results:
        print("조건을 만족하는 날짜가 발견된 종목이 없어 파일을 저장하지 않습니다.")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = folder_path.name
    
    # 결과를 저장할 results 폴더 생성
    result_dir = folder_path / 'results'
    result_dir.mkdir(exist_ok=True)
    
    # 조건 세트 이름 매핑
    condition_names = {
        1: "기존조건",
        2: "추가1조건", 
        3: "추가2조건"
    }
    condition_name = condition_names.get(condition_set, "알수없음")
    
    # 더 효율적인 JSON 구조로 변환
    efficient_results = {
        "analysis_info": {
            "timestamp": timestamp,
            "folder": folder_name,
            "condition_set": condition_set,
            "condition_name": condition_name,
            "total_stocks_found": len(valid_results),
            "total_valid_dates": sum(len(result.get('valid_dates', [])) for result in valid_results)
        },
        "stocks": {}
    }
    
    # 종목별로 그룹화하여 저장
    for result in valid_results:
        code = result.get('code', '')
        if code not in efficient_results["stocks"]:
            efficient_results["stocks"][code] = {
                "dates": result.get('valid_dates', []),
                "file_source": result.get('file_name', ''),
                "processed_time": result.get('processed_time', '')
            }
        else:
            # 같은 종목이 여러 번 나타날 경우 날짜 병합 (중복 제거)
            existing_dates = set(efficient_results["stocks"][code]["dates"])
            new_dates = set(result.get('valid_dates', []))
            efficient_results["stocks"][code]["dates"] = sorted(list(existing_dates | new_dates))
    
    result_file = result_dir / f"analysis_result_{folder_name}_{condition_name}_{timestamp}.json"
    
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(efficient_results, f, ensure_ascii=False, indent=2)
    
    print(f"유효한 결과 {len(valid_results)}개 종목을 {result_file.name}에 저장했습니다.")
    return result_file

def process_table(code_num, df):
    print(code_num)
    table_name = code_num
    
    # 일봉 데이터로 변환
    daily_df = convert_to_daily(df)
    
    if not daily_df.empty:
        # 조건 검색
        valid_dates = check_conditions(daily_df)
        
        if valid_dates:
            print(f"\n{code_num} 종목의 조건 만족 날짜:")
            for date in valid_dates:
                print(date)
    
    return

#개장일 기준 -X일 구하는거. 테스트
def delta_days(days):
    
    xkrx_calendar = get_calendar('XKRX')
    
    start_date = datetime.datetime.now()
    end_date = start_date - days * xkrx_calendar.day  # 주식 개장일 기준 250일 이전
    end_date = end_date.strftime("%Y%m%d")
    
    return end_date

### 분봉 데이터를 더 큰 시간 단위로 변환
def resample_candles(df, multiplier):
    """
    분봉 데이터를 더 큰 시간 단위로 변환합니다.
    불완전한 봉(데이터가 부족한 첫/마지막 봉)은 제거합니다.
    
    :param df: pandas DataFrame, 'date' 열이 datetime 형식이어야 합니다.
    :param multiplier: 변환할 시간 단위의 배수 (예: 1분봉에서 3분봉으로 변환할 때는 3)
    :return: 변환된 DataFrame
    """
    # 원본 데이터프레임 복사
    df_copy = df.copy()
    
    # 'date' 열을 datetime 형식으로 변환 (이미 변환되어 있을 수도 있음)
    if not pd.api.types.is_datetime64_any_dtype(df_copy['date']):
        df_copy['date'] = pd.to_datetime(df_copy['date'], format='%Y%m%d%H%M')

    # datetime을 인덱스로 설정
    df_copy.set_index('date', inplace=True)
    
    # 데이터 정렬 (시간순)
    df_copy = df_copy.sort_index()
    
    # 지정된 시간 단위로 데이터 재구성
    resampled_df = df_copy.resample(f'{multiplier}T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    
    # NaN 값이 있는 행 제거 (불완전한 봉 제거)
    # open, high, low, close 중 하나라도 NaN이면 해당 봉은 불완전함
    resampled_df = resampled_df.dropna(subset=['open', 'high', 'low', 'close'])
    
    # 거래량이 0인 봉도 제거
    resampled_df = resampled_df[resampled_df['volume'] > 0]
    
    # 인덱스를 다시 컬럼으로 변환
    resampled_df.reset_index(inplace=True)
    
    print(f"리샘플링 완료: {len(df)}봉 -> {len(resampled_df)}봉 (배수: {multiplier})")
    
    return resampled_df

### RSI 계산 함수
def calculate_rsi(data, period=12):
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = round(100 - (100 / (1 + rs)), 1)
    return rsi

def TA_bol_band(data, Period=20, num_of_std=2):
    '''
    TA 패키지를 이용하여 볼린져 밴드를 계산합니다.
    :param data: pandas DataFrame, 'close' 열이 포함되어야 합니다.
    :param Period: 이동 평균을 계산할 기간.
    :param num_of_std: 표준편차의 배수.
    :return: 볼린져 밴드가 추가된 DataFrame.
    '''
    middle_band_label = 'BB_M_' + str(Period)
    upper_band_label = 'BB_H_' + str(Period)
    lower_band_label = 'BB_L_' + str(Period)
    Sum_BB_label = 'Sum_BB_' + str(Period)

    # TA 패키지의 BollingerBands 기능을 이용
    indicator_bb = ta.volatility.BollingerBands(close=data['close'], window=Period, window_dev=num_of_std)

    # 볼린져 밴드 계산
    BB_M = indicator_bb.bollinger_mavg()
    BB_H = indicator_bb.bollinger_hband()
    BB_L = indicator_bb.bollinger_lband()
    #data[middle_band_label] = round(BB_M, 2)
    data[upper_band_label] = round(BB_H, 2)
    data[lower_band_label] = round(BB_L, 2)

    # 볼린저 밴드와 종가의 백분율 차이 계산
    high_diff_percent = abs((data['close'] - BB_H) / data['close']) * 100
    low_diff_percent = abs((data['close'] - BB_L) / data['close']) * 100
    Sum_BB = high_diff_percent + low_diff_percent
    data[Sum_BB_label] = round(Sum_BB, 2)


    return data

# ADX (Average Directional movement index)
def TA_ADX(data, Period=12):

    ADX_label = 'ADX_' + str(Period)
    

    # TA 패키지의 ADX 기능을 이용
    Indicator_ADX = ta.trend.ADXIndicator(high=data['high'], low=data['low'], close=data['close'], window=Period)

    # ADX 계산
    ADX = Indicator_ADX.adx()
    data[ADX_label] = round(ADX, 2)

    return data

def calculate_stochslow(data, k_period=18, d_period=5, k_period2=6):
    # 기존 %K 계산을 위한 low_min, high_max
    low_min = data['low'].rolling(window=k_period).min()
    high_max = data['high'].rolling(window=k_period).max()

    # k_period2를 사용한 새로운 low_min, high_max 계산
    low_min_sum = low_min.rolling(window=k_period2).sum()
    high_max_sum = high_max.rolling(window=k_period2).sum()

    # 수정된 %K 계산
    data['%K'] = ((data['close'] - low_min_sum) / (high_max_sum - low_min_sum)) * 100

    # %D 계산 (%K의 이동 평균)
    data['%D'] = data['%K'].rolling(window=d_period).mean()

    return data

def tax(price):
    result = round(price*0.0023, 0)
    return result


def calculate_max_quantity(close_price, total_amount=100000):
    """
    주어진 종가(close price)와 총 금액을 기준으로 최대 매수 수량을 계산합니다.

    :param close_price: 매수할 주식의 종가
    :param total_amount: 매수에 사용할 총 금액
    :return: 매수할 수 있는 최대 주식 수량
    """
    if close_price <= 0:
        return 0

    max_quantity = total_amount // close_price  # 소수점 이하를 버리고 정수 부분만 취함
    return max_quantity

def export_to_excel(df, file_name):
    """
    주어진 데이터프레임을 Excel 파일로 내보냅니다.

    :param df: 내보낼 데이터프레임
    :param file_name: 생성될 Excel 파일의 이름 (확장자 '.xlsx' 포함)
    """
    try:
        # 데이터프레임을 Excel 파일로 저장
        df.to_excel(file_name, index=False)
        print(f"{file_name}으로 데이터프레임이 저장되었습니다.")
    except Exception as e:
        print(f"Excel 파일로 저장하는데 실패했습니다: {e}")

def bol_dolpa(df):
    df['BB_H_12_per'] = ((df['BB_H_12']/df['close'])-1)*100
    df['BB_H_50_per'] = ((df['BB_H_50']/df['close'])-1)*100
    df['BB_H_100_per'] = ((df['BB_H_100']/df['close'])-1)*100 
    

def detect_time_interval(df):
    """
    데이터의 시간 간격을 감지하는 함수
    
    :param df: pandas DataFrame, 'date' 열이 datetime 형식이어야 합니다.
    :return: (시간 간격(분), 시작 시간)
    """
    # 09시~10시 사이의 데이터만 필터링
    morning_data = df[df['date'].dt.hour == 9].copy()
    if morning_data.empty:
        return None, None
    
    # 시간 정렬
    morning_data = morning_data.sort_values('date')
    
    # 연속된 행 간의 시간 차이 계산
    time_diffs = morning_data['date'].diff().dt.total_seconds() / 60
    
    # 가장 빈번한 시간 간격 찾기
    most_common_interval = time_diffs.mode().iloc[0]
    
    # 시작 시간 찾기
    start_time = morning_data['date'].min().time()
    
    return most_common_interval, start_time

def convert_to_daily(df):
    """
    분봉 데이터를 일봉 데이터로 변환합니다.
    기존 일봉(09:00~15:30)과 매매용 일봉(09:00~15:10) 컬럼을 모두 생성합니다.
    
    :param df: pandas DataFrame, 'date' 열이 datetime 형식이어야 합니다.
    :return: 일봉 데이터로 변환된 DataFrame (15:10 컬럼 포함)
    """
    # 시간 간격과 시작 시간 감지
    interval, detected_start_time = detect_time_interval(df)
    
    if interval is None:
        print("09시~10시 사이의 데이터를 찾을 수 없습니다.")
        return pd.DataFrame()
    
    # 거래 시간대 설정
    market_start = pd.Timestamp('09:00:00').time()
    market_end_full = pd.Timestamp('15:30:00').time()  # 완전한 일봉용
    market_end_1510 = pd.Timestamp('15:10:00').time()  # 매매용 일봉
    
    # 전체 거래 시간대 데이터 (09:00~15:30)
    df_full = df[
        (df['date'].dt.time >= market_start) & 
        (df['date'].dt.time <= market_end_full)
    ].copy()
    
    # 15:10까지 데이터 (09:00~15:10)  
    df_1510 = df[
        (df['date'].dt.time >= market_start) & 
        (df['date'].dt.time <= market_end_1510)
    ].copy()
    
    # 데이터가 있는지만 간단히 확인
    if df_full.empty or df_1510.empty:
        print("거래 시간대 데이터가 없습니다.")
        return pd.DataFrame()
    
    print(f"전체 데이터: {len(df_full)}건, 15:10까지 데이터: {len(df_1510)}건")
    
    # 완전한 일봉 데이터 생성 (09:00~15:30)
    daily_df_full = df_full.groupby(df_full['date'].dt.date).agg({
        'open': 'first',    # 시가: 해당 날짜의 첫 가격
        'high': 'max',      # 고가: 해당 날짜의 최고 가격
        'low': 'min',       # 저가: 해당 날짜의 최저 가격
        'close': 'last',    # 종가: 해당 날짜의 마지막 가격
        'volume': 'sum'     # 거래량: 해당 날짜의 총 거래량
    }).reset_index()
    
    # 15:10까지 일봉 데이터 생성 (09:00~15:10)
    daily_df_1510 = df_1510.groupby(df_1510['date'].dt.date).agg({
        'open': 'first',    
        'high': 'max',      
        'low': 'min',       
        'close': 'last',    
        'volume': 'sum'     
    }).reset_index()
    
    # 15:10 컬럼명 변경
    daily_df_1510 = daily_df_1510.rename(columns={
        'open': 'open_1510',
        'high': 'high_1510',
        'low': 'low_1510',
        'close': 'close_1510',
        'volume': 'volume_1510'
    })
    
    # 두 데이터프레임 병합
    daily_df = pd.merge(daily_df_full, daily_df_1510, on='date', how='inner')
    
    # 날짜 컬럼을 datetime으로 변환
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    
    # 거래량이 0인 날짜 제외 (전체 거래량 기준)
    daily_df = daily_df[daily_df['volume'] > 0]
    
    # 날짜 순으로 정렬
    daily_df = daily_df.sort_values('date')
    
    print(f"일봉 데이터 생성 완료: {len(daily_df)}일")
    
    return daily_df

def calculate_ema(data, period):
    """
    지수이동평균(EMA)을 계산하는 함수
    
    :param data: 가격 데이터 Series
    :param period: EMA 기간
    :return: EMA Series
    """
    return data.ewm(span=period, adjust=False).mean()

def check_conditions(daily_df, condition_set=1):
    """
    수정된 조건으로 검색하는 함수 (당일은 15:10 데이터 사용)
    
    :param daily_df: 일봉 데이터 DataFrame (15:10 컬럼 포함)
    :param condition_set: 조건 세트 선택 (1: 기존, 2: 추가1, 3: 추가2)
    :return: 조건을 만족하는 날짜 리스트
    """
    # EMA 계산 (전날까지는 일반 종가, 당일은 15:10 종가 사용)
    daily_df['EMA22'] = calculate_ema(daily_df['close'], 22)
    daily_df['EMA60'] = calculate_ema(daily_df['close'], 60)
    daily_df['EMA120'] = calculate_ema(daily_df['close'], 120)
    daily_df['MA22'] = daily_df['close'].rolling(window=22).mean()
    daily_df['MA50'] = daily_df['close'].rolling(window=50).mean()  # 50일선 추가
    
    # 15:10 기준 MA 계산 (당일 조건 확인용)
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    
    # 최근 N봉 최고가 계산 (당일은 15:10 기준)
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 조건을 만족하는 날짜 찾기
    valid_dates = []
    
    print(f"\n=== 조건 세트 {condition_set} 적용 ===")
    if condition_set == 1:
        print("기존 조건: 8~2봉전 상승세, 2봉전/1봉전 교차 패턴")
    elif condition_set == 2:
        print("추가1 조건: 9~3봉전 상승세, 3봉전/2-1봉전 교차 패턴")
    elif condition_set == 3:
        print("추가2 조건: 10~4봉전 상승세, 4봉전/3-1봉전 교차 패턴")
    
    for idx in range(120, len(daily_df)):  # 최소 120일 데이터 필요
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        try:
            # 조건 세트에 따른 조건 1: 과거 상승세 확인
            if condition_set == 1:
                # 기존: 8봉전부터 2봉전까지 종가 > EMA22
                condition_1 = all(
                    daily_df.iloc[idx-i]['close'] > daily_df.iloc[idx-i]['EMA22']
                    for i in range(2, 9)  # 2봉전부터 8봉전까지
                )
            elif condition_set == 2:
                # 추가1: 9봉전부터 3봉전까지 종가 > EMA22
                condition_1 = all(
                    daily_df.iloc[idx-i]['close'] > daily_df.iloc[idx-i]['EMA22']
                    for i in range(3, 10)  # 3봉전부터 9봉전까지
                )
            elif condition_set == 3:
                # 추가2: 10봉전부터 4봉전까지 종가 > EMA22
                condition_1 = all(
                    daily_df.iloc[idx-i]['close'] > daily_df.iloc[idx-i]['EMA22']
                    for i in range(4, 11)  # 4봉전부터 10봉전까지
                )
            
            # 조건 2: 1봉전 EMA22 > EMA60 > EMA120 (모든 조건 세트 공통)
            condition_2 = (
                prev_row['EMA22'] > prev_row['EMA60'] > prev_row['EMA120']
            )
            
            # 조건 3: 교차 패턴 (조건 세트별로 다름)
            if condition_set == 1:
                # 기존: 2봉전(종가>EMA22>EMA120) & 1봉전(EMA22>종가>EMA120)
                prev2_row = daily_df.iloc[idx-2]
                condition_3a = (
                    prev2_row['close'] > prev2_row['EMA22'] > prev2_row['EMA120']
                )
                condition_3b = (
                    prev_row['EMA22'] > prev_row['close'] > prev_row['EMA120']
                )
                condition_3 = condition_3a and condition_3b
                
            elif condition_set == 2:
                # 추가1: 3봉전(종가>EMA22>EMA120) & 2-1봉전(EMA22>종가>EMA120)
                prev3_row = daily_df.iloc[idx-3]
                prev2_row = daily_df.iloc[idx-2]
                condition_3a = (
                    prev3_row['close'] > prev3_row['EMA22'] > prev3_row['EMA120']
                )
                condition_3b = (
                    prev2_row['EMA22'] > prev2_row['close'] > prev2_row['EMA120']
                )
                condition_3c = (
                    prev_row['EMA22'] > prev_row['close'] > prev_row['EMA120']
                )
                condition_3 = condition_3a and condition_3b and condition_3c
                
            elif condition_set == 3:
                # 추가2: 4봉전(종가>EMA22>EMA120) & 3-1봉전(EMA22>종가>EMA120)
                prev4_row = daily_df.iloc[idx-4]
                prev3_row = daily_df.iloc[idx-3]
                prev2_row = daily_df.iloc[idx-2]
                condition_3a = (
                    prev4_row['close'] > prev4_row['EMA22'] > prev4_row['EMA120']
                )
                condition_3b = (
                    prev3_row['EMA22'] > prev3_row['close'] > prev3_row['EMA120']
                )
                condition_3c = (
                    prev2_row['EMA22'] > prev2_row['close'] > prev2_row['EMA120']
                )
                condition_3d = (
                    prev_row['EMA22'] > prev_row['close'] > prev_row['EMA120']
                )
                condition_3 = condition_3a and condition_3b and condition_3c and condition_3d
            
            # 조건 4: 기준일 close_1510 > MA22_1510 (돌파) - 모든 조건 세트 공통
            condition_4 = (
                current_row['close_1510'] > current_row['MA22_1510']
            )
            
            # 조건 5: 기준일 최근10봉최고가 < (close_1510 * 1.3) - 모든 조건 세트 공통
            condition_5 = (
                current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3)
            )
            
            # 조건 6: 기준일 상승률 5%-15% (당일 15:10 vs 전날 15:30) - 모든 조건 세트 공통
            price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
            condition_6 = 5 <= price_increase <= 15
            
            # 조건 7: 익절가가 매수가보다 8% 이상 높음 - 모든 조건 세트 공통
            # 익절가 계산 (calculate_exit_prices 로직과 동일)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2
            buy_price = current_row['close_1510']
            stop_loss = current_row['MA50']
            
            # 50일선 데이터가 없으면 스킵
            if pd.isna(stop_loss):
                continue
                
            # 익절가 계산
            if candle_center > stop_loss:
                take_profit = candle_center + ((candle_center - stop_loss) * 1.5)
            else:
                take_profit = buy_price * 1.05
                
            # 익절가가 매수가보다 낮으면 5% 익절로 조정
            if take_profit <= buy_price:
                take_profit = buy_price * 1.05
                
            # 익절가가 매수가보다 8% 이상 높은지 확인
            condition_7 = take_profit >= buy_price * 1.08
            
            # 모든 조건 만족 시 해당 날짜 추가
            if (condition_1 and condition_2 and condition_3 and 
                condition_4 and condition_5 and condition_6 and condition_7):
                valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
                
        except Exception as e:
            print(f"날짜 {current_row['date']} 처리 중 오류 발생: {str(e)}")
            continue
    
    return valid_dates

def process_single_file(file, selected_folder, condition_set=1):
    """
    단일 파일을 처리하는 함수 (조건 검색 + 백테스팅)
    """
    try:
        # JSON 파일 읽기
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # JSON 구조 확인
        if 'meta' not in data or 'data' not in data:
            return {
                'code': file.name,
                'error': 'JSON 파일 구조 오류: meta 또는 data 섹션이 없습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None
            }
        
        # 종목 코드 추출 (meta 섹션에서)
        code = data['meta'].get('code', file.name)
        
        # 차트 데이터만 데이터프레임으로 변환
        chart_data = data['data']
        if not chart_data:
            return {
                'code': code,
                'error': '차트 데이터가 비어있습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None
            }
        
        df = pd.DataFrame(chart_data)
        
        # 필수 컬럼 확인
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                'code': code,
                'error': f'필수 컬럼 누락: {missing_columns}',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None
            }
        
        # date 컬럼을 datetime 형식으로 변환
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
        except Exception as date_error:
            # 다른 날짜 형식 시도
            try:
                df['date'] = pd.to_datetime(df['date'])
            except Exception:
                return {
                    'code': code,
                    'error': f'날짜 형식 변환 오류: {str(date_error)}. 첫 번째 date 값: {df["date"].iloc[0] if len(df) > 0 else "없음"}',
                    'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'backtest_result': None
                }
        
        # 필요한 컬럼만 선택
        df = df[required_columns]
        
        # 일봉 데이터로 변환
        daily_df = convert_to_daily(df)
        
        if daily_df.empty:
            return {
                'code': code,
                'valid_dates': [],
                'file_name': file.name,
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None
            }
        
        # 조건 검색 실행 (조건 세트 전달)
        valid_dates = check_conditions(daily_df, condition_set)
        
        # 백테스팅 실행 (조건 만족 날짜가 있을 때만)
        backtest_result = None
        if valid_dates:
            # 종목 코드를 daily_df에 추가 (백테스팅에서 사용)
            daily_df['code'] = code
            
            # 백테스팅 실행
            backtest_df = backtest_strategy(daily_df, df, valid_dates)
            
            if not backtest_df.empty:
                # 종목번호 컬럼 업데이트
                backtest_df['종목번호'] = code
                
                # 백테스팅 결과 저장
                backtest_file = save_backtest_results(backtest_df, selected_folder, code, condition_set)
                
                backtest_result = {
                    'total_trades': len(backtest_df),
                    'win_rate': len(backtest_df[backtest_df['수익률'] > 0]) / len(backtest_df) * 100 if len(backtest_df) > 0 else 0,
                    'avg_return': backtest_df['수익률'].mean() if len(backtest_df) > 0 else 0,
                    'file_saved': backtest_file.name if backtest_file else None
                }
        
        result = {
            'code': code,
            'valid_dates': valid_dates if valid_dates else [],
            'file_name': file.name,
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'backtest_result': backtest_result
        }
        return result
        
    except Exception as e:
        return {
            'code': file.name,
            'error': f'파일 처리 중 예상치 못한 오류: {str(e)}',
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'backtest_result': None
        }

def process_chunk(chunk_files, selected_folder, condition_set=1):
    """
    파일 청크를 처리하는 함수
    """
    chunk_results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_single_file, file, selected_folder, condition_set): file 
            for file in chunk_files
        }
        
        for future in future_to_file:
            result = future.result()
            if result:
                chunk_results.append(result)
    
    return chunk_results

def test_resample_candles(file, target_interval):
    """
    단일 파일의 분봉 데이터를 지정된 시간 간격으로 리샘플링하여 테스트하는 함수
    
    :param file: 테스트할 JSON 파일 경로
    :param target_interval: 목표 시간 간격 (분)
    :return: 테스트 결과 딕셔너리
    """
    try:
        # JSON 파일 읽기
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # JSON 구조 확인
        if 'meta' not in data or 'data' not in data:
            return {
                'code': file.name,
                'status': 'error',
                'message': 'JSON 파일 구조 오류: meta 또는 data 섹션이 없습니다'
            }
        
        # 종목 코드 추출
        code = data['meta'].get('code', file.name)
        
        # 차트 데이터를 데이터프레임으로 변환
        chart_data = data['data']
        if not chart_data:
            return {
                'code': code,
                'status': 'error',
                'message': '차트 데이터가 비어있습니다'
            }
        
        df = pd.DataFrame(chart_data)
        
        # 필수 컬럼 확인
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                'code': code,
                'status': 'error',
                'message': f'필수 컬럼 누락: {missing_columns}'
            }
        
        # date 컬럼을 datetime 형식으로 변환
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
        except Exception as date_error:
            # 다른 날짜 형식 시도
            try:
                df['date'] = pd.to_datetime(df['date'])
            except Exception:
                return {
                    'code': code,
                    'status': 'error',
                    'message': f'날짜 형식 변환 오류: {str(date_error)}. 첫 번째 date 값: {df["date"].iloc[0] if len(df) > 0 else "없음"}'
                }
        
        df = df[required_columns]
        
        # 현재 데이터의 시간 간격 감지
        current_interval, start_time = detect_time_interval(df)
        
        if current_interval is None:
            return {
                'code': code,
                'status': 'error',
                'message': '시간 간격을 감지할 수 없습니다.'
            }
        
        # 목표 간격이 현재 간격의 배수인지 확인
        if target_interval % current_interval != 0:
            return {
                'code': code,
                'status': 'invalid',
                'current_interval': int(current_interval),
                'target_interval': target_interval,
                'message': f'현재 검색된 데이터가 {int(current_interval)}분 차트 데이터입니다. {int(current_interval)}분 기준 배수로만 데이터 수정이 가능합니다.'
            }
        
        # 리샘플링 실행 전 원본 데이터 정보 수집
        original_count = len(df)
        original_first_time = df['date'].min()
        original_last_time = df['date'].max()
        
        # 리샘플링 실행
        multiplier = int(target_interval / current_interval)
        resampled_df = resample_candles(df.copy(), multiplier)
        
        # 결과 통계
        resampled_count = len(resampled_df)
        removed_count = original_count - (resampled_count * multiplier)
        
        # 리샘플링 후 데이터 정보
        resampled_first_time = resampled_df['date'].min() if not resampled_df.empty else None
        resampled_last_time = resampled_df['date'].max() if not resampled_df.empty else None
        
        # 불완전한 봉 감지 정보
        incomplete_info = ""
        if removed_count > 0:
            incomplete_info = f" (불완전한 봉 {removed_count}개 제거됨)"
        
        return {
            'code': code,
            'status': 'success',
            'current_interval': int(current_interval),
            'target_interval': target_interval,
            'multiplier': multiplier,
            'original_count': original_count,
            'resampled_count': resampled_count,
            'removed_count': removed_count,
            'original_time_range': f"{original_first_time.strftime('%Y-%m-%d %H:%M')} ~ {original_last_time.strftime('%Y-%m-%d %H:%M')}",
            'resampled_time_range': f"{resampled_first_time.strftime('%Y-%m-%d %H:%M')} ~ {resampled_last_time.strftime('%Y-%m-%d %H:%M')}" if resampled_first_time else "없음",
            'incomplete_info': incomplete_info,
            'data_sample': resampled_df.head(3).to_dict('records') if not resampled_df.empty else []
        }
        
    except Exception as e:
        return {
            'code': file.name,
            'status': 'error',
            'message': f'처리 중 예상치 못한 오류: {str(e)}'
        }

def test_resample_with_analysis(selected_folder, target_interval, condition_set=1):
    """
    리샘플링된 데이터로 조건 검색과 백테스팅을 수행하는 함수
    
    :param selected_folder: 선택된 폴더 경로
    :param target_interval: 목표 시간 간격 (분)
    :param condition_set: 조건 세트 번호
    :return: 분석 결과
    """
    # 폴더 내의 JSON 파일 목록 가져오기
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return []
    
    print(f"\n=== {target_interval}분봉 리샘플링 분석 시작 ===")
    print(f"처리할 파일 수: {len(json_files)}")
    
    results = []
    valid_files = []
    invalid_files = []
    
    # 1단계: 리샘플링 가능성 확인
    print("\n1단계: 리샘플링 가능성 확인 중...")
    
    for file in tqdm(json_files, desc="파일 검증"):
        test_result = test_resample_candles(file, target_interval)
        
        if test_result['status'] == 'success':
            valid_files.append(file)
            # 불완전한 봉이 제거된 경우 정보 출력
            if test_result.get('removed_count', 0) > 0:
                print(f"\n📊 {test_result['code']}: {test_result['incomplete_info']}")
                print(f"   원본: {test_result['original_time_range']}")
                print(f"   변환: {test_result['resampled_time_range']}")
        elif test_result['status'] == 'invalid':
            invalid_files.append(test_result)
            if len(invalid_files) == 1:  # 첫 번째 오류만 출력
                print(f"\n⚠️ {test_result['message']}")
        else:
            print(f"\n❌ {test_result['code']}: {test_result['message']}")
    
    print(f"\n검증 완료: 처리 가능 파일 {len(valid_files)}개, 불가능 파일 {len(invalid_files)}개")
    
    if not valid_files:
        print("처리 가능한 파일이 없습니다.")
        return []
    
    if invalid_files:
        print(f"처리 불가능한 파일이 {len(invalid_files)}개 있습니다.")
        print("계속 진행하시겠습니까? (y/n): ", end="")
        user_input = input().lower()
        if user_input != 'y':
            print("분석을 중단합니다.")
            return []
    
    # 2단계: 실제 분석 수행
    print(f"\n2단계: {len(valid_files)}개 파일로 조건 검색 및 백테스팅 수행...")
    
    # 파일을 청크로 나누기
    file_chunks = [valid_files[i:i + CHUNK_SIZE] for i in range(0, len(valid_files), CHUNK_SIZE)]
    
    # 리샘플링 버전의 프로세스 함수 사용
    with tqdm(total=len(valid_files), desc="분석 진행") as pbar:
        for chunk in file_chunks:
            chunk_results = process_chunk_with_resampling(chunk, selected_folder, target_interval, condition_set)
            results.extend(chunk_results)
            pbar.update(len(chunk))
    
    return results

def process_single_file_with_resampling(file, selected_folder, target_interval, condition_set=1):
    """
    리샘플링을 적용한 단일 파일 처리 함수
    """
    try:
        # JSON 파일 읽기
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # JSON 구조 확인
        if 'meta' not in data or 'data' not in data:
            return {
                'code': file.name,
                'error': 'JSON 파일 구조 오류: meta 또는 data 섹션이 없습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None,
                'resample_info': ''
            }
        
        # 종목 코드 추출
        code = data['meta'].get('code', file.name)
        
        # 차트 데이터를 데이터프레임으로 변환
        chart_data = data['data']
        if not chart_data:
            return {
                'code': code,
                'error': '차트 데이터가 비어있습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None,
                'resample_info': ''
            }
        
        df = pd.DataFrame(chart_data)
        
        # 필수 컬럼 확인
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                'code': code,
                'error': f'필수 컬럼 누락: {missing_columns}',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None,
                'resample_info': ''
            }
        
        # date 컬럼을 datetime 형식으로 변환
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
        except Exception as date_error:
            # 다른 날짜 형식 시도
            try:
                df['date'] = pd.to_datetime(df['date'])
            except Exception:
                return {
                    'code': code,
                    'error': f'날짜 형식 변환 오류: {str(date_error)}. 첫 번째 date 값: {df["date"].iloc[0] if len(df) > 0 else "없음"}',
                    'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'backtest_result': None,
                    'resample_info': ''
                }
        
        df = df[required_columns]
        
        # 현재 데이터의 시간 간격 감지
        current_interval, _ = detect_time_interval(df)
        
        # 리샘플링 (목표 간격이 현재 간격과 다른 경우에만)
        if target_interval != current_interval:
            multiplier = int(target_interval / current_interval)
            df = resample_candles(df, multiplier)
        
        # 일봉 데이터로 변환 (15:10 컬럼 포함)
        daily_df = convert_to_daily(df)
        
        if daily_df.empty:
            return {
                'code': code,
                'valid_dates': [],
                'file_name': file.name,
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'backtest_result': None,
                'resample_info': f"{int(current_interval)}분 -> {target_interval}분"
            }
        
        # 조건 검색 실행
        valid_dates = check_conditions(daily_df, condition_set)
        
        # 백테스팅 실행 (조건 만족 날짜가 있을 때만)
        backtest_result = None
        if valid_dates:
            daily_df['code'] = code
            backtest_df = backtest_strategy(daily_df, df, valid_dates)
            
            if not backtest_df.empty:
                backtest_df['종목번호'] = code
                
                # 리샘플링 정보를 포함한 파일명으로 저장
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backtest_dir = selected_folder / 'backtest_resampled'
                backtest_dir.mkdir(exist_ok=True)
                
                condition_names = {1: "기존조건", 2: "추가1조건", 3: "추가2조건"}
                condition_name = condition_names.get(condition_set, "알수없음")
                
                excel_file = backtest_dir / f"backtest_{code}_{target_interval}분_{condition_name}_{timestamp}.xlsx"
                
                # 통계 정보 생성
                total_trades = len(backtest_df)
                win_trades = len(backtest_df[backtest_df['수익률'] > 0])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                avg_return = backtest_df['수익률'].mean() if total_trades > 0 else 0
                
                stats_df = pd.DataFrame({
                    '항목': ['총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)', '조건세트', '리샘플링'],
                    '값': [total_trades, win_trades, total_trades - win_trades, round(win_rate, 2), 
                          round(avg_return, 2), condition_name, f"{int(current_interval)}분 -> {target_interval}분"]
                })
                
                # 엑셀 파일 저장
                with pd.ExcelWriter(excel_file) as writer:
                    backtest_df.to_excel(writer, sheet_name='거래내역', index=False)
                    stats_df.to_excel(writer, sheet_name='통계', index=False)
                
                backtest_result = {
                    'total_trades': total_trades,
                    'win_rate': win_rate,
                    'avg_return': avg_return,
                    'file_saved': excel_file.name
                }
        
        return {
            'code': code,
            'valid_dates': valid_dates if valid_dates else [],
            'file_name': file.name,
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'backtest_result': backtest_result,
            'resample_info': f"{int(current_interval)}분 -> {target_interval}분"
        }
        
    except Exception as e:
        return {
            'code': file.name,
            'error': f'파일 처리 중 예상치 못한 오류: {str(e)}',
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'backtest_result': None
        }

def process_chunk_with_resampling(chunk_files, selected_folder, target_interval, condition_set=1):
    """
    리샘플링이 적용된 파일 청크 처리 함수
    """
    chunk_results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_single_file_with_resampling, file, selected_folder, target_interval, condition_set): file 
            for file in chunk_files
        }
        
        for future in future_to_file:
            result = future.result()
            if result:
                chunk_results.append(result)
    
    return chunk_results

def calculate_exit_prices(daily_df, current_idx):
    """
    익절가와 손절가를 계산하는 함수
    """
    current_row = daily_df.iloc[current_idx]
    
    # 매수가: 당일 15:10 종가
    buy_price = current_row['close_1510']
    
    # 손절가: 50일선
    stop_loss = current_row['MA50']
    
    # 익절가 계산
    candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2
    
    if candle_center > stop_loss:
        take_profit = candle_center + ((candle_center - stop_loss) * 1.5)
    else:
        take_profit = buy_price * 1.05
        
    # 익절가가 매수가보다 낮으면 5% 익절로 조정
    if take_profit <= buy_price:
        take_profit = buy_price * 1.05
        
    return buy_price, take_profit, stop_loss

def backtest_strategy(daily_df, minute_df, valid_dates):
    """
    백테스팅을 수행하는 함수
    """
    backtest_results = []
    
    # 날짜 문자열을 datetime으로 변환
    valid_dates_dt = [pd.to_datetime(date) for date in valid_dates]
    
    for target_date in valid_dates_dt:
        # 해당 날짜의 일봉 데이터 찾기
        date_mask = daily_df['date'].dt.date == target_date.date()
        if not date_mask.any():
            continue
            
        current_idx = daily_df[date_mask].index[0]
        
        # 익절가, 손절가 계산
        buy_price, take_profit, stop_loss = calculate_exit_prices(daily_df, current_idx)
        
        # 다음 거래일들에서 익절/손절 확인
        exit_price = None
        exit_date = None
        exit_type = None
        
        for future_idx in range(current_idx + 1, min(current_idx + 11, len(daily_df))):  # 최대 10일간 확인
            future_row = daily_df.iloc[future_idx]
            
            # 익절 조건 확인 (고가가 익절가 도달)
            if future_row['high'] >= take_profit:
                exit_price = take_profit
                exit_date = future_row['date']
                exit_type = '익절'
                break
                
            # 손절 조건 확인 (저가가 손절가 이하)
            if future_row['low'] <= stop_loss:
                exit_price = stop_loss
                exit_date = future_row['date']
                exit_type = '손절'
                break
        
        # 10일 내에 청산되지 않으면 마지막 날 종가로 청산
        if exit_price is None:
            last_idx = min(current_idx + 10, len(daily_df) - 1)
            exit_price = daily_df.iloc[last_idx]['close']
            exit_date = daily_df.iloc[last_idx]['date']
            exit_type = '시간청산'
        
        # 수익률 계산
        return_rate = (exit_price / buy_price - 1) * 100
        
        # 결과 저장
        backtest_results.append({
            '매수일': target_date.strftime('%Y-%m-%d'),
            '청산일': exit_date.strftime('%Y-%m-%d'),
            '매수가': round(buy_price, 0),
            '청산가': round(exit_price, 0),
            '익절가': round(take_profit, 0),
            '손절가': round(stop_loss, 0),
            '수익률': round(return_rate, 2),
            '청산유형': exit_type,
            '종목번호': daily_df.iloc[current_idx].get('code', '')
        })
    
    return pd.DataFrame(backtest_results)

def save_backtest_results(backtest_df, folder_path, code, condition_set):
    """
    백테스팅 결과를 Excel 파일로 저장하는 함수
    """
    if backtest_df.empty:
        return None
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # backtest 폴더 생성
    backtest_dir = folder_path / 'backtest'
    backtest_dir.mkdir(exist_ok=True)
    
    # 조건 이름 매핑
    condition_names = {1: "기존조건", 2: "추가1조건", 3: "추가2조건"}
    condition_name = condition_names.get(condition_set, "알수없음")
    
    # 파일명 생성
    excel_file = backtest_dir / f"backtest_{code}_{condition_name}_{timestamp}.xlsx"
    
    # 통계 정보 생성
    total_trades = len(backtest_df)
    win_trades = len(backtest_df[backtest_df['수익률'] > 0])
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
    avg_return = backtest_df['수익률'].mean() if total_trades > 0 else 0
    
    stats_df = pd.DataFrame({
        '항목': ['종목코드', '총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)', '조건세트'],
        '값': [code, total_trades, win_trades, total_trades - win_trades, 
               round(win_rate, 2), round(avg_return, 2), condition_name]
    })
    
    # Excel 파일 저장
    with pd.ExcelWriter(excel_file) as writer:
        backtest_df.to_excel(writer, sheet_name='거래내역', index=False)
        stats_df.to_excel(writer, sheet_name='통계', index=False)
    
    return excel_file

def combine_all_backtest_results(folder_path, condition_set):
    """
    모든 백테스팅 결과를 하나의 파일로 통합하는 함수
    """
    backtest_dir = folder_path / 'backtest'
    if not backtest_dir.exists():
        return None
    
    # 조건 세트에 해당하는 엑셀 파일들 찾기
    condition_names = {1: "기존조건", 2: "추가1조건", 3: "추가2조건"}
    condition_name = condition_names.get(condition_set, "알수없음")
    
    excel_files = list(backtest_dir.glob(f"*{condition_name}*.xlsx"))
    if not excel_files:
        return None
    
    all_trades = []
    
    for excel_file in excel_files:
        try:
            # 거래내역 읽기
            trades_df = pd.read_excel(excel_file, sheet_name='거래내역')
            all_trades.append(trades_df)
            
        except Exception as e:
            print(f"파일 {excel_file.name} 읽기 오류: {e}")
            continue
    
    if not all_trades:
        return None
    
    # 모든 거래내역 통합
    combined_trades = pd.concat(all_trades, ignore_index=True)
    
    # 전체 통계 계산
    total_trades = len(combined_trades)
    win_trades = len(combined_trades[combined_trades['수익률'] > 0])
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
    avg_return = combined_trades['수익률'].mean() if total_trades > 0 else 0
    total_return = combined_trades['수익률'].sum()
    
    overall_stats = pd.DataFrame({
        '항목': ['전체 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)', '누적 수익률(%)', '조건세트'],
        '값': [total_trades, win_trades, total_trades - win_trades, 
               round(win_rate, 2), round(avg_return, 2), round(total_return, 2), condition_name]
    })
    
    # 통합 파일 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_file = backtest_dir / f"combined_backtest_{condition_name}_{timestamp}.xlsx"
    
    with pd.ExcelWriter(combined_file) as writer:
        combined_trades.to_excel(writer, sheet_name='전체거래내역', index=False)
        overall_stats.to_excel(writer, sheet_name='전체통계', index=False)
        
        # 종목별 통계도 추가
        stock_stats = combined_trades.groupby('종목번호').agg({
            '수익률': ['count', 'mean', 'sum', lambda x: (x > 0).sum()]
        }).round(2)
        stock_stats.columns = ['거래수', '평균수익률', '누적수익률', '수익거래수']
        stock_stats['승률'] = round(stock_stats['수익거래수'] / stock_stats['거래수'] * 100, 2)
        stock_stats.to_excel(writer, sheet_name='종목별통계')
    
    return combined_file

def process_all_conditions(daily_df):
    """
    모든 조건 세트(1,2,3)를 동시에 검색하는 함수
    """
    results = {}
    
    for condition_set in [1, 2, 3]:
        valid_dates = check_conditions(daily_df, condition_set)
        results[condition_set] = valid_dates
    
    return results

def process_single_file_all_conditions(file, selected_folder):
    """
    모든 조건을 동시에 검색하고 백테스팅하는 함수 (3번 메뉴용)
    """
    try:
        # JSON 파일 읽기
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # JSON 구조 확인
        if 'meta' not in data or 'data' not in data:
            return {
                'code': file.name,
                'error': 'JSON 파일 구조 오류: meta 또는 data 섹션이 없습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'all_conditions': {}
            }
        
        # 종목 코드 추출
        code = data['meta'].get('code', file.name)
        
        # 차트 데이터를 데이터프레임으로 변환
        chart_data = data['data']
        if not chart_data:
            return {
                'code': code,
                'error': '차트 데이터가 비어있습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'all_conditions': {}
            }
        
        df = pd.DataFrame(chart_data)
        
        # 필수 컬럼 확인
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                'code': code,
                'error': f'필수 컬럼 누락: {missing_columns}',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'all_conditions': {}
            }
        
        # date 컬럼을 datetime 형식으로 변환
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
        except Exception as date_error:
            try:
                df['date'] = pd.to_datetime(df['date'])
            except Exception:
                return {
                    'code': code,
                    'error': f'날짜 형식 변환 오류: {str(date_error)}',
                    'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'all_conditions': {}
                }
        
        # 필요한 컬럼만 선택
        df = df[required_columns]
        
        # 일봉 데이터로 변환 (분봉 리샘플링 없이 기존 데이터 사용)
        daily_df = convert_to_daily(df)
        
        if daily_df.empty:
            return {
                'code': code,
                'all_conditions': {},
                'file_name': file.name,
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # 모든 조건 검색 실행
        all_conditions_results = process_all_conditions(daily_df)
        
        # 각 조건별 백테스팅 실행
        backtest_results = {}
        for condition_set, valid_dates in all_conditions_results.items():
            if valid_dates:
                daily_df['code'] = code
                backtest_df = backtest_strategy(daily_df, df, valid_dates)
                
                if not backtest_df.empty:
                    backtest_df['종목번호'] = code
                    backtest_results[condition_set] = {
                        'dataframe': backtest_df,
                        'total_trades': len(backtest_df),
                        'win_rate': len(backtest_df[backtest_df['수익률'] > 0]) / len(backtest_df) * 100,
                        'avg_return': backtest_df['수익률'].mean()
                    }
        
        return {
            'code': code,
            'all_conditions': all_conditions_results,
            'backtest_results': backtest_results,
            'file_name': file.name,
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        return {
            'code': file.name,
            'error': f'파일 처리 중 예상치 못한 오류: {str(e)}',
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'all_conditions': {}
        }

def execute_integrated_backtest():
    """
    3번 메뉴: 전체 조건 통합 백테스팅 실행 함수
    """
    global MAX_WORKERS, CHUNK_SIZE
    
    # json_data 폴더 내의 폴더 목록 가져오기
    base_dir = Path('json_data')
    if not base_dir.exists():
        print("json_data 폴더가 존재하지 않습니다.")
        return
        
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        print("분석할 데이터 폴더가 없습니다.")
        return
        
    # 폴더 목록 표시
    print("\n=== 사용 가능한 데이터 폴더 ===")
    for idx, folder in enumerate(folders, 1):
        print(f"{idx}. {folder.name}")
    
    # 폴더 선택
    while True:
        try:
            folder_choice = int(input("\n분석할 폴더 번호를 선택하세요: "))
            if 1 <= folder_choice <= len(folders):
                selected_folder = folders[folder_choice - 1]
                break
            else:
                print(f"1부터 {len(folders)}까지의 번호만 입력 가능합니다.")
        except ValueError:
            print("올바른 숫자를 입력하세요.")
    
    # 프로세스 수 설정
    while True:
        try:
            num_workers = int(input(f"\n사용할 프로세스 수를 입력하세요 (1-{multiprocessing.cpu_count()}, 기본값: {MAX_WORKERS}): ") or MAX_WORKERS)
            if 1 <= num_workers <= multiprocessing.cpu_count():
                MAX_WORKERS = num_workers
                break
            else:
                print(f"1부터 {multiprocessing.cpu_count()}까지의 숫자만 입력 가능합니다.")
        except ValueError:
            print("올바른 숫자를 입력하세요.")
    
    # 선택된 폴더 내의 JSON 파일 목록 가져오기
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return
        
    print(f"\n=== 전체 조건 통합 백테스팅 시작 ===")
    print(f"처리할 파일 수: {len(json_files)}")
    print(f"사용할 프로세스 수: {MAX_WORKERS}")
    print("기존조건 + 추가1조건 + 추가2조건을 동시에 검색합니다.")
    
    # 파일을 청크로 나누기
    file_chunks = [json_files[i:i + CHUNK_SIZE] for i in range(0, len(json_files), CHUNK_SIZE)]
    
    # 전체 결과를 저장할 리스트
    all_results = []
    
    # 각 조건별 백테스팅 결과를 저장할 딕셔너리
    all_backtest_results = {1: [], 2: [], 3: []}
    
    # tqdm을 사용하여 진행 상황 표시
    with tqdm(total=len(json_files), desc="통합 분석 진행") as pbar:
        for chunk in file_chunks:
            # 청크별 병렬 처리
            chunk_results = []
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_file = {
                    executor.submit(process_single_file_all_conditions, file, selected_folder): file 
                    for file in chunk
                }
                
                for future in future_to_file:
                    result = future.result()
                    if result:
                        chunk_results.append(result)
                        
                        # 백테스팅 결과 수집
                        backtest_results = result.get('backtest_results', {})
                        for condition_set, bt_result in backtest_results.items():
                            all_backtest_results[condition_set].append(bt_result['dataframe'])
            
            all_results.extend(chunk_results)
            pbar.update(len(chunk))
    
    # 결과 출력
    print("\n=== 통합 분석 결과 ===")
    success_count = 0
    error_count = 0
    condition_stats = {1: 0, 2: 0, 3: 0}
    
    for result in all_results:
        if 'error' in result:
            error_count += 1
            print(f"\n{result['code']} 처리 중 오류 발생: {result['error']}")
        else:
            success_count += 1
            all_conditions = result.get('all_conditions', {})
            
            for condition_set, valid_dates in all_conditions.items():
                if valid_dates:
                    condition_stats[condition_set] += 1
                    print(f"\n{result['code']} - 조건{condition_set}: {len(valid_dates)}개 날짜 발견")
    
    print(f"\n처리 완료: 총 {len(all_results)}개 파일")
    print(f"성공: {success_count}개, 실패: {error_count}개")
    print(f"조건1 만족 종목: {condition_stats[1]}개")
    print(f"조건2 만족 종목: {condition_stats[2]}개")
    print(f"조건3 만족 종목: {condition_stats[3]}개")
    
    # 통합 백테스팅 결과 파일 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    integrated_dir = selected_folder / 'integrated_backtest'
    integrated_dir.mkdir(exist_ok=True)
    
    integrated_file = integrated_dir / f"integrated_backtest_all_conditions_{timestamp}.xlsx"
    
    condition_names = {1: "기존조건", 2: "추가1조건", 3: "추가2조건"}
    
    with pd.ExcelWriter(integrated_file) as writer:
        # 각 조건별 시트 생성
        for condition_set in [1, 2, 3]:
            if all_backtest_results[condition_set]:
                # 모든 백테스팅 결과 통합
                combined_df = pd.concat(all_backtest_results[condition_set], ignore_index=True)
                
                # 통계 계산
                total_trades = len(combined_df)
                win_trades = len(combined_df[combined_df['수익률'] > 0])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                avg_return = combined_df['수익률'].mean() if total_trades > 0 else 0
                total_return = combined_df['수익률'].sum()
                
                # 거래내역 시트
                sheet_name = f"{condition_names[condition_set]}_거래내역"
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 통계 시트
                stats_df = pd.DataFrame({
                    '항목': ['총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)', '누적 수익률(%)'],
                    '값': [total_trades, win_trades, total_trades - win_trades, 
                           round(win_rate, 2), round(avg_return, 2), round(total_return, 2)]
                })
                
                stats_sheet_name = f"{condition_names[condition_set]}_통계"
                stats_df.to_excel(writer, sheet_name=stats_sheet_name, index=False)
                
                print(f"\n{condition_names[condition_set]} 백테스팅 결과:")
                print(f"  총 거래수: {total_trades}건")
                print(f"  승률: {win_rate:.2f}%")
                print(f"  평균 수익률: {avg_return:.2f}%")
                print(f"  누적 수익률: {total_return:.2f}%")
        
        # 전체 요약 시트 생성
        summary_data = []
        for condition_set in [1, 2, 3]:
            if all_backtest_results[condition_set]:
                combined_df = pd.concat(all_backtest_results[condition_set], ignore_index=True)
                total_trades = len(combined_df)
                win_trades = len(combined_df[combined_df['수익률'] > 0])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                avg_return = combined_df['수익률'].mean() if total_trades > 0 else 0
                total_return = combined_df['수익률'].sum()
                
                summary_data.append({
                    '조건': condition_names[condition_set],
                    '종목수': condition_stats[condition_set],
                    '총거래수': total_trades,
                    '수익거래': win_trades,
                    '손실거래': total_trades - win_trades,
                    '승률(%)': round(win_rate, 2),
                    '평균수익률(%)': round(avg_return, 2),
                    '누적수익률(%)': round(total_return, 2)
                })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='전체요약', index=False)
    
    print(f"\n통합 백테스팅 결과가 {integrated_file.name}에 저장되었습니다.")
    
    # 개별 종목 파일들 삭제 (요청사항 3번)
    print("\n개별 종목 백테스팅 파일들을 정리합니다...")
    backtest_dir = selected_folder / 'backtest'
    if backtest_dir.exists():
        individual_files = list(backtest_dir.glob('backtest_*.xlsx'))
        deleted_count = 0
        for file in individual_files:
            try:
                file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"파일 {file.name} 삭제 실패: {e}")
        
        print(f"개별 파일 {deleted_count}개가 삭제되었습니다.")
        print(f"통합 결과 파일만 {integrated_file.name}에 남겨졌습니다.")

def main():
    global MAX_WORKERS, CHUNK_SIZE
    
    while True:
        print("\n=== 주식 데이터 분석 프로그램 ===")
        print("1. 데이터 폴더 선택 및 분석")
        print("2. 분봉 리샘플링 테스트")
        print("3. 전체 조건 통합 백테스팅")
        print("9. 프로그램 종료")
        
        choice = input("\n메뉴를 선택하세요 (1,2,3,9): ")
        
        if choice == '1':
            # 기존 1번 메뉴 로직
            # json_data 폴더 내의 폴더 목록 가져오기
            base_dir = Path('json_data')
            if not base_dir.exists():
                print("json_data 폴더가 존재하지 않습니다.")
                continue
                
            folders = [f for f in base_dir.iterdir() if f.is_dir()]
            if not folders:
                print("분석할 데이터 폴더가 없습니다.")
                continue
                
            # 폴더 목록 표시
            print("\n=== 사용 가능한 데이터 폴더 ===")
            for idx, folder in enumerate(folders, 1):
                print(f"{idx}. {folder.name}")
            
            # 폴더 선택
            while True:
                try:
                    folder_choice = int(input("\n분석할 폴더 번호를 선택하세요: "))
                    if 1 <= folder_choice <= len(folders):
                        selected_folder = folders[folder_choice - 1]
                        break
                    else:
                        print(f"1부터 {len(folders)}까지의 번호만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 프로세스 수 설정
            while True:
                try:
                    num_workers = int(input(f"\n사용할 프로세스 수를 입력하세요 (1-{multiprocessing.cpu_count()}, 기본값: {MAX_WORKERS}): ") or MAX_WORKERS)
                    if 1 <= num_workers <= multiprocessing.cpu_count():
                        MAX_WORKERS = num_workers
                        break
                    else:
                        print(f"1부터 {multiprocessing.cpu_count()}까지의 숫자만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 조건 세트 선택
            print("\n=== 조건 세트 선택 ===")
            print("1. 기존 조건 (8~2봉전 상승세, 2봉전/1봉전 교차)")
            print("2. 추가1 조건 (9~3봉전 상승세, 3봉전/2-1봉전 교차)")
            print("3. 추가2 조건 (10~4봉전 상승세, 4봉전/3-1봉전 교차)")
            
            while True:
                try:
                    condition_choice = int(input("\n사용할 조건 세트를 선택하세요 (1-3): "))
                    if 1 <= condition_choice <= 3:
                        break
                    else:
                        print("1부터 3까지의 번호만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 선택된 폴더 내의 JSON 파일 목록 가져오기
            json_files = list(selected_folder.glob('*.json'))
            if not json_files:
                print("선택한 폴더에 JSON 파일이 없습니다.")
                continue
                
            print(f"\n=== {selected_folder.name} 폴더의 파일 목록 ===")
            for idx, file in enumerate(json_files, 1):
                print(f"{idx}. {file.name}")
            
            # 파일 분석 시작
            print("\n파일 분석을 시작합니다...")
            print(f"사용할 프로세스 수: {MAX_WORKERS}")
            print(f"조건 세트: {condition_choice}")
            print(f"청크 크기: {CHUNK_SIZE}")
            
            # 파일을 청크로 나누기
            file_chunks = [json_files[i:i + CHUNK_SIZE] for i in range(0, len(json_files), CHUNK_SIZE)]
            
            # 전체 결과를 저장할 리스트
            all_results = []
            
            # tqdm을 사용하여 진행 상황 표시
            with tqdm(total=len(json_files), desc="파일 처리 중") as pbar:
                for chunk_idx, chunk in enumerate(file_chunks):
                    chunk_results = process_chunk(chunk, selected_folder, condition_choice)
                    all_results.extend(chunk_results)
                    pbar.update(len(chunk))
            
            # 최종 결과 저장
            result_file = save_results(all_results, selected_folder, condition_choice)
            
            # 결과 출력
            print("\n=== 분석 결과 ===")
            success_count = 0
            error_count = 0
            valid_dates_count = 0
            backtest_count = 0
            total_trades = 0
            
            for result in all_results:
                if 'error' in result:
                    error_count += 1
                    print(f"\n{result['code']} 처리 중 오류 발생: {result['error']}")
                else:
                    success_count += 1
                    if result.get('valid_dates'):
                        valid_dates_count += 1
                        print(f"\n{result['code']} 종목의 조건 만족 날짜:")
                        for date in result['valid_dates']:
                            print(f"  - {date}")
                        
                        # 백테스팅 결과 출력
                        backtest_result = result.get('backtest_result')
                        if backtest_result:
                            backtest_count += 1
                            total_trades += backtest_result['total_trades']
                            print(f"  백테스팅: {backtest_result['total_trades']}건 거래, " +
                                  f"승률 {backtest_result['win_rate']:.1f}%, " +
                                  f"평균 수익률 {backtest_result['avg_return']:.2f}%")
            
            print(f"\n처리 완료: 총 {len(all_results)}개 파일")
            print(f"성공: {success_count}개, 실패: {error_count}개")
            print(f"조건 만족 종목: {valid_dates_count}개")
            print(f"백테스팅 완료 종목: {backtest_count}개")
            print(f"총 거래 건수: {total_trades}건")
            
            if result_file:
                print(f"결과가 {result_file.name}에 저장되었습니다.")
                
                # 백테스팅 결과 통합
                if backtest_count > 0:
                    print("\n백테스팅 결과 통합을 시작합니다...")
                    combined_backtest_file = combine_all_backtest_results(selected_folder, condition_choice)
                    if combined_backtest_file:
                        print(f"전체 백테스팅 결과가 {combined_backtest_file.name}에 저장되었습니다.")
            else:
                print("저장할 유효한 결과가 없습니다.")
                
        elif choice == '2':
            # 2번 메뉴 로직 (분봉 리샘플링 테스트)
            # json_data 폴더 내의 폴더 목록 가져오기
            base_dir = Path('json_data')
            if not base_dir.exists():
                print("json_data 폴더가 존재하지 않습니다.")
                continue
                
            folders = [f for f in base_dir.iterdir() if f.is_dir()]
            if not folders:
                print("테스트할 데이터 폴더가 없습니다.")
                continue
                
            # 폴더 목록 표시
            print("\n=== 사용 가능한 데이터 폴더 ===")
            for idx, folder in enumerate(folders, 1):
                print(f"{idx}. {folder.name}")
            
            # 폴더 선택
            while True:
                try:
                    folder_choice = int(input("\n테스트할 폴더 번호를 선택하세요: "))
                    if 1 <= folder_choice <= len(folders):
                        selected_folder = folders[folder_choice - 1]
                        break
                    else:
                        print(f"1부터 {len(folders)}까지의 번호만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 프로세스 수 설정
            while True:
                try:
                    num_workers = int(input(f"\n사용할 프로세스 수를 입력하세요 (1-{multiprocessing.cpu_count()}, 기본값: {MAX_WORKERS}): ") or MAX_WORKERS)
                    if 1 <= num_workers <= multiprocessing.cpu_count():
                        MAX_WORKERS = num_workers
                        break
                    else:
                        print(f"1부터 {multiprocessing.cpu_count()}까지의 숫자만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 조건 세트 선택
            print("\n=== 조건 세트 선택 ===")
            print("1. 기존 조건 (8~2봉전 상승세, 2봉전/1봉전 교차)")
            print("2. 추가1 조건 (9~3봉전 상승세, 3봉전/2-1봉전 교차)")
            print("3. 추가2 조건 (10~4봉전 상승세, 4봉전/3-1봉전 교차)")
            
            while True:
                try:
                    condition_choice = int(input("\n사용할 조건 세트를 선택하세요 (1-3): "))
                    if 1 <= condition_choice <= 3:
                        break
                    else:
                        print("1부터 3까지의 번호만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 목표 분봉 간격 설정
            while True:
                try:
                    target_interval = int(input("\n변환할 분봉 간격을 입력하세요 (예: 10, 15, 30): "))
                    if target_interval > 0:
                        break
                    else:
                        print("양수만 입력 가능합니다.")
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
            
            # 리샘플링 분석 실행
            print(f"\n{target_interval}분봉 리샘플링 분석을 시작합니다...")
            results = test_resample_with_analysis(selected_folder, target_interval, condition_choice)
            
            # 결과 출력
            if results:
                print("\n=== 리샘플링 분석 결과 ===")
                success_count = 0
                error_count = 0
                valid_dates_count = 0
                backtest_count = 0
                total_trades = 0
                
                for result in results:
                    if 'error' in result:
                        error_count += 1
                        print(f"\n{result['code']} 처리 중 오류 발생: {result['error']}")
                    else:
                        success_count += 1
                        print(f"\n{result['code']} ({result.get('resample_info', '')}):")
                        
                        if result.get('valid_dates'):
                            valid_dates_count += 1
                            print(f"  조건 만족 날짜: {len(result['valid_dates'])}개")
                            
                            # 백테스팅 결과 출력
                            backtest_result = result.get('backtest_result')
                            if backtest_result:
                                backtest_count += 1
                                total_trades += backtest_result['total_trades']
                                print(f"  백테스팅: {backtest_result['total_trades']}건 거래, " +
                                      f"승률 {backtest_result['win_rate']:.1f}%, " +
                                      f"평균 수익률 {backtest_result['avg_return']:.2f}%")
                        else:
                            print("  조건 만족 날짜 없음")
                
                print(f"\n처리 완료: 총 {len(results)}개 파일")
                print(f"성공: {success_count}개, 실패: {error_count}개")
                print(f"조건 만족 종목: {valid_dates_count}개")
                print(f"백테스팅 완료 종목: {backtest_count}개")
                print(f"총 거래 건수: {total_trades}건")
                
                # 리샘플링 백테스팅 결과가 있으면 통합 파일 생성
                if backtest_count > 0:
                    print(f"\n백테스팅 결과는 backtest_resampled 폴더에 저장되었습니다.")
                    print(f"파일명에 '{target_interval}분' 정보가 포함되어 있습니다.")
            else:
                print("\n분석할 수 있는 파일이 없었습니다.")
                
        elif choice == '3':
            # 3번 메뉴: 전체 조건 통합 백테스팅
            execute_integrated_backtest()
            
        elif choice == '9':
            print("\n프로그램을 종료합니다.")
            break
            
        else:
            print("\n잘못된 선택입니다. 다시 선택해주세요.")

if __name__ == "__main__":
    main()