"""
🎯 MH_BT_03.py - EMA22 반등 조건 백테스팅 시스템 (추가 개선 버전)

📝 수정 이력:
- 2025.01.XX: 익절가 계산 로직 수정 (v02)
  * 기존: candle_center 기준 계산 (시가-종가 중간값)
  * 수정: buy_price 기준 계산 (실제 매수가)
  * 위험대비보상비 1.5배를 매수가 기준으로 적용
  * 최소 수익률 5% 보장, 안전구간에서는 8% 목표

- 2025.01.XX: 추가 개선사항 (v03)
  * 🔧 최소 5% 익절가 강제 설정 제거 - 계산된 익절가 그대로 사용
  * 📊 Total_거래내역 정렬을 시간순(오름차순)으로 변경
  * 💰 투자 수익 계산 컬럼 '1000000' 추가 (엑셀 수식 자동 생성)
  
🔧 주요 변경사항:
1. 모든 익절가 계산 함수에서 5% 최소값 강제 설정 제거
2. Total_거래내역 시트 정렬 방식 변경 (최신순 → 시간순)
3. 투자 수익 추적 컬럼 및 엑셀 수식 자동 생성 기능 추가
4. 누적 투자 수익률 계산 로직 구현

💡 개선 효과:
- 더 정확한 익절가: 강제 최소값 제거로 실제 계산값 사용
- 시간순 분석: 투자 시점 순서대로 결과 확인 가능
- 자동 수익 추적: 1,000,000원 초기 투자금 기준 누적 수익 자동 계산
"""

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
import configparser
import itertools
### TA 패키지를 이용한 볼린져 밴드 계산 함수
import ta
from pykrx import stock
from exchange_calendars import get_calendar
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt
from scipy import stats

# 전역 변수
MAX_WORKERS = 12
CHUNK_SIZE = 20

def load_config():
    """
    설정 파일에서 조건을 로드하는 함수
    """
    config = configparser.ConfigParser()
    # % 기호 보간(interpolation) 비활성화로 % 포함 주석 안전하게 처리
    config = configparser.ConfigParser(interpolation=None)
    config_file = Path('backtest_config.ini')
    
    # 기본 설정 파일이 없으면 생성
    if not config_file.exists():
        create_default_config()
    
    config.read(config_file, encoding='utf-8')
    return config

def safe_get_config(config, section, key, default_value=None):
    """
    설정값을 안전하게 가져오는 함수 (주석 제거, 공백 제거)
    """
    try:
        value = config[section][key]
        # 주석 제거 (# 이후 부분)
        if '#' in value:
            value = value.split('#')[0]
        # 앞뒤 공백 제거
        value = value.strip()
        # 빈 문자열이면 기본값 사용
        if not value:
            return default_value
        return value
    except (KeyError, TypeError):
        return default_value

def create_default_config():
    """
    기본 설정 파일 생성
    """
    config = configparser.ConfigParser()
    
    # 1봉 반등 조건 (조건 세트 1)
    config['CONDITION_1'] = {
        'name': '1봉반등조건',
        'description': '1봉전 EMA22아래, 2-8봉전 EMA22위, 당일 EMA22 회복',
        'cross_pattern': '1',
        'uptrend_candle_length': '7'
    }
    
    # 2봉 반등 조건 (조건 세트 2)
    config['CONDITION_2'] = {
        'name': '2봉반등조건',
        'description': '1-2봉전 EMA22아래, 3-9봉전 EMA22위, 당일 EMA22 회복',
        'cross_pattern': '2',
        'uptrend_candle_length': '7'
    }
    
    # 3봉 반등 조건 (조건 세트 3)
    config['CONDITION_3'] = {
        'name': '3봉반등조건',
        'description': '1-3봉전 EMA22아래, 4-10봉전 EMA22위, 당일 EMA22 회복',
        'cross_pattern': '3',
        'uptrend_candle_length': '7'
    }
    
    # 공통 설정
    config['COMMON'] = {
        'max_workers': str(MAX_WORKERS),
        'chunk_size': str(CHUNK_SIZE),
        'trade_amount': '100000',
        'max_hold_days': '4',
        'tax_rate': '0.0023',
        'min_price_increase': '2',
        'max_price_increase': '10.0',
        'min_profit_margin': '4.0',
        'absolute_stop_loss_pct': '8.0',  # 절대 손절값 8% 추가
        'recent_high_period': '30',       # 최고값 조사 기간 (일) - 기본값 30일
        'max_decline_from_high_pct': '30.0'  # 최고값 대비 최대 하락률 (%) - 기본값 30%
    }
    
    with open('backtest_config.ini', 'w', encoding='utf-8') as configfile:
        config.write(configfile)
    
    print("기본 설정 파일 'backtest_config.ini'가 생성되었습니다.")

def detect_time_interval(df):
    """
    데이터의 시간 간격을 감지하는 함수
    """
    morning_data = df[df['date'].dt.hour == 9].copy()
    if morning_data.empty:
        return None, None
    
    morning_data = morning_data.sort_values('date')
    time_diffs = morning_data['date'].diff().dt.total_seconds() / 60
    most_common_interval = time_diffs.mode().iloc[0]
    start_time = morning_data['date'].min().time()
    
    return most_common_interval, start_time

def convert_to_daily(df):
    """
    분봉 데이터를 일봉 데이터로 변환 (15:10 컬럼 포함)
    """
    interval, detected_start_time = detect_time_interval(df)
    
    if interval is None:
        print("09시~10시 사이의 데이터를 찾을 수 없습니다.")
        return pd.DataFrame()
    
    market_start = pd.Timestamp('09:00:00').time()
    market_end_full = pd.Timestamp('15:30:00').time()
    market_end_1510 = pd.Timestamp('15:10:00').time()
    
    df_full = df[
        (df['date'].dt.time >= market_start) & 
        (df['date'].dt.time <= market_end_full)
    ].copy()
    
    df_1510 = df[
        (df['date'].dt.time >= market_start) & 
        (df['date'].dt.time <= market_end_1510)
    ].copy()
    
    if df_full.empty or df_1510.empty:
        print("거래 시간대 데이터가 없습니다.")
        return pd.DataFrame()
    
    # 완전한 일봉 데이터 생성 (09:00~15:30)
    daily_df_full = df_full.groupby(df_full['date'].dt.date).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    # 15:10까지 일봉 데이터 생성 (09:00~15:10)
    daily_df_1510 = df_1510.groupby(df_1510['date'].dt.date).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    daily_df_1510 = daily_df_1510.rename(columns={
        'open': 'open_1510',
        'high': 'high_1510',
        'low': 'low_1510',
        'close': 'close_1510',
        'volume': 'volume_1510'
    })
    
    daily_df = pd.merge(daily_df_full, daily_df_1510, on='date', how='inner')
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df = daily_df[daily_df['volume'] > 0]
    daily_df = daily_df.sort_values('date')
    
    return daily_df

def calculate_ema(data, period):
    """지수이동평균(EMA) 계산"""
    return data.ewm(span=period, adjust=False).mean()

def calculate_ema50(daily_df):
    """50일 지수이동평균 계산"""
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    return daily_df

def calculate_linear_regression_value(data, period):
    """
    Linear Regression Value (LRL) 계산
    주어진 기간 동안의 선형회귀선의 끝점 값을 반환
    """
    lrl_values = []
    
    for i in range(len(data)):
        if i < period - 1:
            lrl_values.append(np.nan)
        else:
            # 최근 period 기간의 데이터
            y_values = data.iloc[i-period+1:i+1].values
            x_values = np.arange(period)
            
            # 선형회귀 계산
            slope, intercept, _, _, _ = stats.linregress(x_values, y_values)
            
            # 기간 끝점(현재)의 회귀선 값
            lrl_value = slope * (period - 1) + intercept
            lrl_values.append(lrl_value)
    
    return pd.Series(lrl_values, index=data.index)

def check_conditions(daily_df, condition_set=1):
    """
    새로운 EMA22 반등 패턴으로 검색하는 함수 (오류 수정 버전)
    """
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    
    if condition_key not in config:
        print(f"조건 세트 {condition_set}를 찾을 수 없습니다.")
        return []
    
    condition = config[condition_key]
    common = config['COMMON']
    
    # 설정에서 값 읽기
    cross_pattern = int(safe_get_config(config, condition_key, 'cross_pattern', '1'))
    uptrend_length = int(safe_get_config(config, condition_key, 'uptrend_candle_length', '7'))
    min_increase = float(safe_get_config(config, 'COMMON', 'min_price_increase', '2'))
    max_increase = float(safe_get_config(config, 'COMMON', 'max_price_increase', '10'))
    min_profit = float(safe_get_config(config, 'COMMON', 'min_profit_margin', '4'))
    
    # 🔧 오류 수정 1: EMA 계산을 15:10 종가 기준으로 변경
    daily_df['EMA22_1510'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60_1510'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120_1510'] = calculate_ema(daily_df['close_1510'], 120)
    
    # 🔧 오류 수정 2: EMA50 계산 추가 (손절가 계산용)
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    
    # 기존 계산 유지 (호환성)
    daily_df['MA22'] = daily_df['close'].rolling(window=22).mean()
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 🔧 새로운 조건: 최고값 기준 하락률 계산 (기본값 30일)
    recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
    daily_df[f'recent_high_{recent_high_period}d'] = daily_df['high'].rolling(window=recent_high_period).max()
    
    valid_dates = []
    
    # 🔧 오류 수정 4: 인덱스 범위 체크 강화 + 최고값 기간 반영
    min_data_needed = max(120, 50, cross_pattern + uptrend_length + 1, recent_high_period)  # 최고값 계산을 위한 충분한 데이터
    
    for idx in range(min_data_needed, len(daily_df)):
        # 🔧 오류 수정 4: 인덱스 범위 체크
        if idx < cross_pattern + uptrend_length:
            continue
            
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        try:
            # 조건 1: EMA22 회복 패턴 확인
            condition_1 = True
            
            # 🔧 오류 수정 3: 과거 데이터도 15:10 기준으로 비교
            # 1-N봉전: EMA22 아래 (cross_pattern 기간)
            for i in range(1, cross_pattern + 1):
                if idx - i < 0:  # 🔧 추가 범위 체크
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] >= past_row['EMA22_1510']:  # 15:10 기준으로 변경
                    condition_1 = False
                    break
            
            # (cross_pattern+1)봉전부터 (cross_pattern+uptrend_length)봉전: EMA22 위
            if condition_1:
                for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                    if idx - i < 0:  # 🔧 추가 범위 체크
                        condition_1 = False
                        break
                    past_row = daily_df.iloc[idx-i]
                    if past_row['close_1510'] <= past_row['EMA22_1510']:  # 15:10 기준으로 변경
                        condition_1 = False
                        break
            
            # 조건 2: 1봉전 EMA22 > EMA60 > EMA120 (15:10 기준으로 변경)
            condition_2 = (
                prev_row['EMA22_1510'] > prev_row['EMA60_1510'] > prev_row['EMA120_1510']
            )
            
            # 조건 3: 당일 EMA22 회복 (15:10 기준 일관성 유지)
            condition_3 = (
                current_row['close_1510'] > current_row['EMA22_1510']
            )
            
            # 조건 4: 기준일 close_1510 > MA22_1510 (돌파)
            condition_4 = (
                current_row['close_1510'] > current_row['MA22_1510']
            )
            
            # 조건 5: 기준일 최근10봉최고가 < (close_1510 * 1.3)
            condition_5 = (
                current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3)
            )
            
            # 조건 6: 기준일 상승률 (공통 설정 기반)
            price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
            condition_6 = min_increase <= price_increase <= max_increase
            
            # 조건 7: 익절가가 매수가보다 설정% 이상 높음 (공통 설정 기반)
            buy_price = current_row['close_1510']
            
            # 🔧 개선: 절대 손절값 설정 읽기
            absolute_stop_loss_pct = float(safe_get_config(config, 'COMMON', 'absolute_stop_loss_pct', '5.0'))
            
            # 🔧 개선: 두 가지 손절값 계산
            ema50_stop_loss = current_row['EMA50']
            absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            
            # 🎯 익절가/손절가 통합 계산 (중심값 기준)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2  # 중심값
            
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나 매수가보다 높으면 절대손절 기준으로 계산
                risk_amount = buy_price * (absolute_stop_loss_pct / 100)
                take_profit = buy_price + (risk_amount * 1.5)
                # 손절가는 절대손절값만 사용
                final_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            else:
                # EMA50 기준 중심값으로 위험금액 계산
                risk_amount = candle_center - ema50_stop_loss
                take_profit = buy_price + (risk_amount * 1.5)
                # 손절가는 EMA50과 절대손절값 중 높은 값
                final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
            
            # 🔧 최소 5% 강제 설정 제거 - 계산된 익절가 그대로 사용
                
            condition_7 = take_profit >= buy_price * (1 + min_profit / 100)
            
            # 🔧 조건 8: 최고값 대비 하락률 제한
            recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
            max_decline_from_high_pct = float(safe_get_config(config, 'COMMON', 'max_decline_from_high_pct', '30.0'))
            
            recent_high_column = f'recent_high_{recent_high_period}d'
            if recent_high_column in current_row.index and not pd.isna(current_row[recent_high_column]):
                recent_high = current_row[recent_high_column]
                decline_from_high = ((recent_high - buy_price) / recent_high) * 100 if recent_high > 0 else 0  # 🔧 올바른 하락률 계산
                if decline_from_high > max_decline_from_high_pct:
                    continue
            
            # 모든 조건 만족 시 해당 날짜 추가
            if (condition_1 and condition_2 and condition_3 and 
                condition_4 and condition_5 and condition_6 and condition_7):
                valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
                
        except Exception as e:
            print(f"날짜 {current_row.get('date', 'Unknown')} 처리 중 오류: {repr(e)}")
            continue
    
    return valid_dates

def calculate_exit_prices(buy_row, absolute_stop_loss_pct=8.0):
    """매수일 기준으로 손절가와 익절가 계산 (절대 손절값 추가, 익절가 로직 수정)"""
    buy_price = buy_row['close_1510']  # 실제 매수가
    
    # 🔧 개선: 두 가지 손절값 계산
    # 1) EMA50 손절값
    ema50_stop_loss = buy_row['EMA50']
    
    # 2) 절대 손절값 (매수가 기준 -5%)
    absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
    
    # 🔧 손절가 계산은 익절가 계산과 함께 처리됨
    
    # 🎯 익절가는 EMA50과 중심값 기준으로 계산 (손절가 계산과 분리)
    candle_center = (buy_row['open_1510'] + buy_row['close_1510']) / 2  # 중심값
    
    if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
        # EMA50이 없거나 매수가보다 높으면 절대손절 기준으로 계산
        risk_amount = buy_price * (absolute_stop_loss_pct / 100)
        take_profit = buy_price + (risk_amount * 1.5)
        # 손절가는 절대손절값만 사용
        final_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
        stop_loss_type = "절대손절"
        ema50_invalid = True  # EMA50 무효 상태
    else:
        # EMA50 기준 중심값으로 위험금액 계산
        risk_amount = candle_center - ema50_stop_loss
        take_profit = buy_price + (risk_amount * 1.5)
        # 손절가는 EMA50과 절대손절값 중 높은 값
        absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
        final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
        stop_loss_type = "EMA50손절" if final_stop_loss == ema50_stop_loss else "절대손절"
        ema50_invalid = False  # EMA50 유효 상태
    
    # 🔧 최소 5% 강제 설정 제거 - 계산된 익절가 그대로 사용
    
    return final_stop_loss, take_profit, stop_loss_type, ema50_invalid

def check_exit_conditions_minute_data(minute_df, buy_date, stop_loss, take_profit, max_hold_days=4):
    """5분봉 데이터로 매도 조건 체크"""
    buy_date = pd.to_datetime(buy_date)
    start_date = buy_date + pd.Timedelta(days=1)
    end_date = buy_date + pd.Timedelta(days=max_hold_days)
    
    sell_period = minute_df[
        (minute_df['date'].dt.date >= start_date.date()) &
        (minute_df['date'].dt.date <= end_date.date())
    ].copy()
    
    if sell_period.empty:
        return None, None, "데이터없음"
    
    for day_num, (date, day_data) in enumerate(sell_period.groupby(sell_period['date'].dt.date), 1):
        day_data = day_data.sort_values('date')
        
        for _, row in day_data.iterrows():
            if row['high'] >= take_profit:
                return pd.to_datetime(date), take_profit, "익절"
            if row['low'] <= stop_loss:
                return pd.to_datetime(date), stop_loss, "손절"
        
        if day_num >= max_hold_days:
            last_close = day_data.iloc[-1]['close']
            return pd.to_datetime(date), last_close, "강제매도"
    
    return None, None, "미매도"

def backtest_strategy(daily_df, minute_df, valid_dates):
    """백테스팅 실행"""
    # 🔧 설정에서 절대 손절값과 최대 보유기간 읽기
    config = load_config()
    absolute_stop_loss_pct = float(safe_get_config(config, 'COMMON', 'absolute_stop_loss_pct', '5.0'))
    max_hold_days = int(safe_get_config(config, 'COMMON', 'max_hold_days', '4'))
    
    daily_df = calculate_ema50(daily_df)
    
    # 🔧 Linear Regression Value 계산 추가
    daily_df['LRL22'] = calculate_linear_regression_value(daily_df['close'], 22)
    daily_df['linear99'] = daily_df['LRL22'] * 0.99
    daily_df['ema50_99'] = daily_df['EMA50'] * 0.99
    
    results = []
    
    for date_str in valid_dates:
        try:
            buy_date = pd.to_datetime(date_str)
            buy_day_data = daily_df[daily_df['date'].dt.date == buy_date.date()]
            if buy_day_data.empty:
                continue
                
            buy_row = buy_day_data.iloc[0]
            
            # 🔧 절대 손절값과 함께 손절/익절가 계산
            buy_price = buy_row['close_1510']
            stop_loss, take_profit, stop_loss_type, ema50_invalid = calculate_exit_prices(buy_row, absolute_stop_loss_pct)
            
            sell_date, sell_price, sell_reason = check_exit_conditions_minute_data(
                minute_df, buy_date, stop_loss, take_profit, max_hold_days
            )
            
            if sell_date is not None and sell_price is not None:
                hold_days = (sell_date - buy_date).days
                
                # 🔧 Linear Regression 관련 값들 계산
                linear99_value = buy_row['linear99'] if not pd.isna(buy_row['linear99']) else 0
                ema50_99_value = buy_row['ema50_99'] if not pd.isna(buy_row['ema50_99']) else 0
                
                # linear99가 ema50_99보다 위에 있는지 확인
                linear99_above_ema50_99 = linear99_value > ema50_99_value if not (pd.isna(linear99_value) or pd.isna(ema50_99_value)) else False
                
                # linear99가 전봉 대비 상승 중인지 확인
                linear99_rising = False
                buy_index = daily_df[daily_df['date'].dt.date == buy_date.date()].index
                if len(buy_index) > 0:
                    current_idx = buy_index[0]
                    if current_idx > 0:
                        prev_linear99 = daily_df.iloc[current_idx-1]['linear99']
                        if not pd.isna(prev_linear99) and not pd.isna(linear99_value):
                            linear99_rising = linear99_value > prev_linear99
                
                # 실제 적용된 손절 퍼센트 계산
                stop_loss_pct = round(((buy_price - stop_loss) / buy_price) * 100, 2)
                
                result = {
                    '종목번호': buy_row.get('code', 'Unknown'),
                    '매수일': buy_date.strftime('%Y-%m-%d'),
                    '매수값': round(buy_price, 0),
                    '익절가(목표)': round(take_profit, 0),
                    '손절가(목표)': round(stop_loss, 0),
                    '손절%': stop_loss_pct,  # 🔧 실제 적용된 손절 퍼센트 추가
                    '매도일': sell_date.strftime('%Y-%m-%d'),
                    '매도값': round(sell_price, 0),
                    '보유기간': hold_days,
                    '손절익절': sell_reason,
                    '수익률': round(((sell_price / buy_price) - 1) * 100, 2),
                    '손절타입': stop_loss_type,  # 🔧 손절 타입 추가
                    'EMA50무효': ema50_invalid,  # 🔧 EMA50 무효 여부 추가
                    'linear99': round(linear99_value, 2),  # 🔧 linear99 값
                    'ema50_99': round(ema50_99_value, 2),  # 🔧 ema50*0.99 값
                    'linear99>ema50_99': linear99_above_ema50_99,  # 🔧 linear99가 ema50_99보다 위에 있는지
                    'linear99상승': linear99_rising  # 🔧 linear99가 전봉 대비 상승 중인지
                }
                results.append(result)
                
        except Exception as e:
            continue
    
    return pd.DataFrame(results)

def process_single_file_all_conditions(file, selected_folder):
    """단일 파일을 모든 조건으로 처리하는 함수"""
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if 'meta' not in data or 'data' not in data:
            return {
                'code': file.name,
                'error': 'JSON 파일 구조 오류',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        code = data['meta'].get('code', file.name)
        chart_data = data['data']
        
        if not chart_data:
            return {
                'code': code,
                'error': '차트 데이터가 비어있습니다',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        df = pd.DataFrame(chart_data)
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return {
                'code': code,
                'error': f'필수 컬럼 누락: {missing_columns}',
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
        except Exception as date_error:
            try:
                df['date'] = pd.to_datetime(df['date'])
            except Exception:
                return {
                    'code': code,
                    'error': f'날짜 형식 변환 오류: {str(date_error)}',
                    'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
        
        df = df[required_columns]
        daily_df = convert_to_daily(df)
        
        if daily_df.empty:
            return {
                'code': code,
                'condition_1': {'valid_dates': []},
                'condition_2': {'valid_dates': []},
                'condition_3': {'valid_dates': []},
                'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        daily_df['code'] = code
        result = {
            'code': code,
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 각 조건별로 검색 및 백테스팅 수행
        for condition_set in [1, 2, 3]:
            condition_key = f'condition_{condition_set}'
            valid_dates = check_conditions(daily_df, condition_set)
            condition_result = {'valid_dates': valid_dates if valid_dates else []}
            
            if valid_dates:
                backtest_df = backtest_strategy(daily_df, df, valid_dates)
                
                if not backtest_df.empty:
                    backtest_df['종목번호'] = code
                    backtest_file = save_backtest_results_integrated(
                        backtest_df, selected_folder, code, condition_set
                    )
                    
                    condition_result['backtest_result'] = {
                        'total_trades': len(backtest_df),
                        'win_rate': len(backtest_df[backtest_df['수익률'] > 0]) / len(backtest_df) * 100 if len(backtest_df) > 0 else 0,
                        'avg_return': backtest_df['수익률'].mean() if len(backtest_df) > 0 else 0,
                        'file_saved': backtest_file.name if backtest_file else None
                    }
            
            result[condition_key] = condition_result
        
        return result
        
    except Exception as e:
        return {
            'code': file.name,
            'error': f'파일 처리 중 오류: {repr(e)}',
            'processed_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

def process_chunk_all_conditions(chunk_files, selected_folder):
    """모든 조건으로 파일 청크를 처리하는 함수"""
    chunk_results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_single_file_all_conditions, file, selected_folder): file 
            for file in chunk_files
        }
        
        for future in future_to_file:
            result = future.result()
            if result:
                chunk_results.append(result)
    
    return chunk_results

def save_backtest_results_integrated(results_df, folder_path, stock_code, condition_set):
    """통합 백테스팅용 결과 저장 함수"""
    if results_df.empty:
        return None
    
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    condition_name = safe_get_config(config, condition_key, 'name', f'조건{condition_set}')
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backtest_dir = folder_path / 'backtest_integrated'
    backtest_dir.mkdir(exist_ok=True)
    
    excel_file = backtest_dir / f"backtest_{stock_code}_{condition_name}_{timestamp}.xlsx"
    
    total_trades = len(results_df)
    win_trades = len(results_df[results_df['수익률'] > 0])
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
    avg_return = results_df['수익률'].mean() if total_trades > 0 else 0
    
    stats_df = pd.DataFrame({
        '항목': ['총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)', '조건세트'],
        '값': [total_trades, win_trades, total_trades - win_trades, round(win_rate, 2), round(avg_return, 2), condition_name]
    })
    
    with pd.ExcelWriter(excel_file) as writer:
        results_df.to_excel(writer, sheet_name='거래내역', index=False)
        stats_df.to_excel(writer, sheet_name='통계', index=False)
    
    return excel_file

def combine_integrated_results(all_results, folder_path):
    """모든 조건의 백테스팅 결과를 하나의 엑셀 파일로 통합"""
    backtest_dir = folder_path / 'backtest_integrated'
    if not backtest_dir.exists():
        return None
    
    config = load_config()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_file = backtest_dir / f"TOTAL_통합백테스팅결과_{timestamp}.xlsx"
    
    all_condition_data = {}
    
    for condition_set in [1, 2, 3]:
        condition_key = f'CONDITION_{condition_set}'
        condition_name = safe_get_config(config, condition_key, 'name', f'조건{condition_set}')
        
        excel_files = list(backtest_dir.glob(f'backtest_*{condition_name}*.xlsx'))
        combined_trades = []
        total_stats = {'total_trades': 0, 'win_trades': 0, 'total_return': 0, 'stock_count': 0}
        
        for excel_file in excel_files:
            try:
                trades_df = pd.read_excel(excel_file, sheet_name='거래내역')
                if not trades_df.empty:
                    combined_trades.append(trades_df)
                    total_stats['total_trades'] += len(trades_df)
                    total_stats['win_trades'] += len(trades_df[trades_df['수익률'] > 0])
                    total_stats['total_return'] += trades_df['수익률'].sum()
                    total_stats['stock_count'] += 1
            except Exception as e:
                continue
        
        if combined_trades:
            condition_trades = pd.concat(combined_trades, ignore_index=True)
            condition_trades = condition_trades.sort_values(['종목번호', '매수일'])
            
            win_rate = (total_stats['win_trades'] / total_stats['total_trades'] * 100) if total_stats['total_trades'] > 0 else 0
            avg_return = total_stats['total_return'] / total_stats['total_trades'] if total_stats['total_trades'] > 0 else 0
            
            condition_summary = pd.DataFrame({
                '항목': ['종목 수', '총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)'],
                '값': [
                    total_stats['stock_count'], total_stats['total_trades'], total_stats['win_trades'],
                    total_stats['total_trades'] - total_stats['win_trades'], round(win_rate, 2), round(avg_return, 2)
                ]
            })
            
            all_condition_data[condition_name] = {
                'trades': condition_trades,
                'summary': condition_summary
            }
    
    if all_condition_data:
        with pd.ExcelWriter(final_file) as writer:
            # 🎯 Total_거래내역 시트 생성 (모든 조건의 거래내역 통합)
            all_trades = []
            for condition_name, data in all_condition_data.items():
                trades_with_condition = data['trades'].copy()
                trades_with_condition['조건명'] = condition_name  # 조건명 컬럼 추가
                all_trades.append(trades_with_condition)
            
            if all_trades:
                total_trades_df = pd.concat(all_trades, ignore_index=True)
                # 🔧 매수일 기준으로 정렬 (시간순으로 변경)
                total_trades_df = total_trades_df.sort_values(['매수일', '조건명'], ascending=[True, True])
                
                # 🎯 투자 수익 계산 컬럼 추가 (초기 투자금 1,000,000원)
                initial_investment = 1000000
                
                # 수익률과 매수값 컬럼 위치 찾기
                profit_rate_col = None
                for i, col in enumerate(total_trades_df.columns):
                    if col == '수익률':
                        profit_rate_col = i
                        break
                
                # 투자 수익 계산
                investment_values = []
                current_investment = initial_investment
                
                for idx, row in total_trades_df.iterrows():
                    profit_rate = row['수익률']
                    # 수익률 적용: 현재 투자금 * (1 + 수익률/100)
                    current_investment = current_investment * (1 + profit_rate / 100)
                    investment_values.append(round(current_investment, 0))
                
                total_trades_df['1000000'] = investment_values  # 계산된 값으로 컬럼 추가
                
                total_trades_df.to_excel(writer, sheet_name='Total_거래내역', index=False)
                
                # Total_거래내역 통계 생성
                total_count = len(total_trades_df)
                total_win = len(total_trades_df[total_trades_df['수익률'] > 0])
                total_win_rate = (total_win / total_count * 100) if total_count > 0 else 0
                total_avg_return = total_trades_df['수익률'].mean() if total_count > 0 else 0
                
                total_stats_df = pd.DataFrame({
                    '항목': ['총 거래수', '수익 거래', '손실 거래', '승률(%)', '평균 수익률(%)'],
                    '값': [total_count, total_win, total_count - total_win, round(total_win_rate, 2), round(total_avg_return, 2)]
                })
                total_stats_df.to_excel(writer, sheet_name='Total_통계', index=False)
            
            # 각 조건별 개별 시트 생성
            for condition_name, data in all_condition_data.items():
                data['trades'].to_excel(writer, sheet_name=f'{condition_name}_거래내역', index=False)
                data['summary'].to_excel(writer, sheet_name=f'{condition_name}_요약', index=False)
            
            # 전체 요약 시트 생성
            overall_summary = []
            for condition_name, data in all_condition_data.items():
                summary_data = data['summary']
                row_data = {'조건': condition_name}
                for _, row in summary_data.iterrows():
                    row_data[row['항목']] = row['값']
                overall_summary.append(row_data)
            
            if overall_summary:
                overall_df = pd.DataFrame(overall_summary)
                overall_df.to_excel(writer, sheet_name='전체요약', index=False)
        
        return final_file
    return None

def cleanup_temp_files(folder_path):
    """임시 파일들을 정리하는 함수"""
    backtest_dir = folder_path / 'backtest_integrated'
    if not backtest_dir.exists():
        return
    
    temp_files = [f for f in backtest_dir.glob('*.xlsx') if not f.name.startswith('TOTAL_')]
    deleted_count = 0
    
    for temp_file in temp_files:
        try:
            temp_file.unlink()
            deleted_count += 1
        except Exception as e:
            print(f"파일 {temp_file.name} 삭제 중 오류: {repr(e)}")
    
    print(f"임시 파일 {deleted_count}개가 정리되었습니다.")

def execute_integrated_backtest():
    """통합 백테스팅 실행 함수"""
    global MAX_WORKERS, CHUNK_SIZE
    
    # 설정 로드
    config = load_config()
    common = config['COMMON']
    MAX_WORKERS = int(safe_get_config(config, 'COMMON', 'max_workers', 12))
    CHUNK_SIZE = int(safe_get_config(config, 'COMMON', 'chunk_size', 20))
    
    print("\n=== 통합 백테스팅 프로그램 ===")
    print("설정 파일에서 조건을 로드했습니다.")
    
    # 폴더 선택
    base_dir = Path('json_data')
    if not base_dir.exists():
        print("json_data 폴더가 존재하지 않습니다.")
        return
        
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        print("분석할 데이터 폴더가 없습니다.")
        return
        
    print("\n=== 사용 가능한 데이터 폴더 ===")
    for idx, folder in enumerate(folders, 1):
        print(f"{idx}. {folder.name}")
    
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
    
    # 파일 처리
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return
        
    print(f"\n=== {selected_folder.name} 폴더 분석 시작 ===")
    print(f"처리할 파일 수: {len(json_files)}")
    print(f"사용할 프로세스 수: {MAX_WORKERS}")
    print(f"검색 조건: 모든 조건 동시 처리")
    
    # 조건 정보 출력
    for i in [1, 2, 3]:
        condition_key = f'CONDITION_{i}'
        condition_name = safe_get_config(config, condition_key, 'name', f'조건{i}')
        condition_desc = safe_get_config(config, condition_key, 'description', f'조건{i} 설명')
        print(f"  - {condition_name}: {condition_desc}")
    
    file_chunks = [json_files[i:i + CHUNK_SIZE] for i in range(0, len(json_files), CHUNK_SIZE)]
    all_results = []
    
    with tqdm(total=len(json_files), desc="통합 백테스팅 진행 중") as pbar:
        for chunk in file_chunks:
            chunk_results = process_chunk_all_conditions(chunk, selected_folder)
            all_results.extend(chunk_results)
            pbar.update(len(chunk))
    
    # 결과 분석
    print("\n=== 분석 결과 ===")
    success_count = 0
    error_count = 0
    
    condition_stats = {}
    for i in [1, 2, 3]:
        condition_key = f'CONDITION_{i}'
        condition_name = safe_get_config(config, condition_key, 'name', f'조건{i}')
        condition_stats[i] = {'종목수': 0, '거래수': 0, '조건명': condition_name}
    
    for result in all_results:
        if 'error' in result:
            error_count += 1
        else:
            success_count += 1
            for condition_set in [1, 2, 3]:
                condition_result = result.get(f'condition_{condition_set}', {})
                if condition_result.get('valid_dates'):
                    condition_stats[condition_set]['종목수'] += 1
                    backtest_result = condition_result.get('backtest_result')
                    if backtest_result:
                        condition_stats[condition_set]['거래수'] += backtest_result['total_trades']
    
    print(f"처리 완료: 총 {len(all_results)}개 파일")
    print(f"성공: {success_count}개, 실패: {error_count}개")
    
    print("\n=== 조건별 결과 요약 ===")
    for condition_set, stats in condition_stats.items():
        print(f"{stats['조건명']}: {stats['종목수']}개 종목, {stats['거래수']}건 거래")
    
    # 통합 결과 파일 생성
    if success_count > 0:
        print("\n통합 결과 파일을 생성합니다...")
        final_file = combine_integrated_results(all_results, selected_folder)
        if final_file:
            print(f"✅ 최종 통합 결과: {final_file.name}")
            print("📋 생성된 시트:")
            print("   📊 Total_거래내역 - 모든 조건의 거래내역 통합 (조건명 포함)")
            print("   📈 Total_통계 - 전체 통합 통계")
            print("   📑 각 조건별 거래내역 및 요약 시트")
            print("   📋 전체요약 - 조건별 비교 요약")
            cleanup_temp_files(selected_folder)
            print("📁 임시 파일들이 정리되었습니다.")
    else:
        print("생성할 결과가 없습니다.")

def optimize_parameters():
    """
    파라미터 최적화 실행 함수 (멀티프로세싱 적용)
    """
    global MAX_WORKERS, CHUNK_SIZE
    
    print("\n=== 파라미터 최적화 프로그램 ===")
    print("다양한 설정 조합을 테스트하여 최적의 조건을 찾습니다.")
    
    # 폴더 선택
    base_dir = Path('json_data')
    if not base_dir.exists():
        print("json_data 폴더가 존재하지 않습니다.")
        return
        
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        print("분석할 데이터 폴더가 없습니다.")
        return
        
    print("\n=== 사용 가능한 데이터 폴더 ===")
    for idx, folder in enumerate(folders, 1):
        print(f"{idx}. {folder.name}")
    
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
    
    # 사용자가 직접 프로세스 수 선택 가능
    num_workers = int(input(f"사용할 프로세스 수 (1-{multiprocessing.cpu_count()}, 기본값: {MAX_WORKERS}): "))
    
    # 테스트할 파일 수 제한 (빠른 테스트를 위해)
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return
    
    # 파일 수 제한 옵션
    print(f"\n총 {len(json_files)}개 파일이 있습니다.")
    print("1. 전체 파일로 테스트 (정확하지만 느림)")
    print("2. 샘플 파일로 테스트 (빠르지만 근사치)")
    
    test_choice = input("선택하세요 (1 또는 2): ")
    if test_choice == '2':
        sample_size = min(50, len(json_files))  # 최대 50개 파일
        json_files = json_files[:sample_size]
        print(f"샘플 {sample_size}개 파일로 테스트합니다.")
    
    # 최적화 파라미터 정의
    optimization_params = {
        'cross_pattern': [1, 2, 3],
        'uptrend_candle_length': [5, 7, 10, 12],
        'min_price_increase': [1, 2, 3],
        'max_price_increase': [8, 10, 12, 15],
        'min_profit_margin': [2, 3, 4, 5],
        'absolute_stop_loss_pct': [3, 4, 5, 6, 7],  # 🔧 절대 손절값 최적화 추가
        'recent_high_period': [20, 30, 40],  # 🔧 최고값 조사 기간 최적화 추가
        'max_decline_from_high_pct': [20.0, 25.0, 30.0, 35.0]  # 🔧 최대 하락률 최적화 추가
    }
    
    print(f"\n=== 최적화 파라미터 범위 ===")
    for param, values in optimization_params.items():
        print(f"{param}: {values}")
    
    # 모든 조합 계산
    param_combinations = list(itertools.product(*optimization_params.values()))
    total_combinations = len(param_combinations)
    
    print(f"\n총 {total_combinations}개 조합을 테스트합니다.")
    print(f"🚀 멀티프로세싱: {MAX_WORKERS}개 프로세스 사용")
    
    # 최적화 시작 확인
    confirm = input("최적화를 시작하시겠습니까? (y/n): ")
    if confirm.lower() != 'y':
        return
    
    # 🚀 멀티프로세싱으로 최적화 실행
    print(f"\n=== 최적화 진행 중 (멀티프로세싱: {MAX_WORKERS}개 프로세스) ===")
    
    # 조합을 청크로 나누기
    combination_chunks = [param_combinations[i:i + MAX_WORKERS] 
                         for i in range(0, len(param_combinations), MAX_WORKERS)]
    
    optimization_results = []
    
    # tqdm을 사용하여 진행 상황 표시
    with tqdm(total=total_combinations, desc="파라미터 최적화 진행") as pbar:
        for chunk in combination_chunks:
            # 🚀 병렬 처리
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_combination = {
                    executor.submit(
                        test_parameter_combination_wrapper,
                        combination, json_files, selected_folder, idx + len(optimization_results) + 1
                    ): combination 
                    for idx, combination in enumerate(chunk)
                }
                
                for future in future_to_combination:
                    result = future.result()
                    if result:
                        optimization_results.append(result)
                        
                        # 중간 결과 출력
                        if result['total_trades'] > 0:
                            tqdm.write(f"조합 {result['combination_id']}: {result['total_trades']}건 거래, "
                                     f"승률 {result['win_rate']:.1f}%, "
                                     f"평균수익률 {result['avg_return']:.2f}%")
                    
                    pbar.update(1)
    
    # 최적화 결과 분석 및 저장
    if optimization_results:
        save_optimization_results(optimization_results, selected_folder)
        analyze_optimization_results(optimization_results)
    else:
        print("최적화 결과가 없습니다.")

def test_parameter_combination_wrapper(combination, json_files, selected_folder, combination_id):
    """
    멀티프로세싱용 래퍼 함수
    """
    cross_pattern, uptrend_length, min_increase, max_increase, min_profit, absolute_stop_loss_pct, recent_high_period, max_decline_from_high_pct = combination
    
    result = test_parameter_combination(
        json_files, selected_folder, 
        cross_pattern, uptrend_length, min_increase, max_increase, min_profit, absolute_stop_loss_pct,
        recent_high_period, max_decline_from_high_pct
    )
    
    if result:
        result['combination_id'] = combination_id
        result['parameters'] = {
            'cross_pattern': cross_pattern,
            'uptrend_candle_length': uptrend_length,
            'min_price_increase': min_increase,
            'max_price_increase': max_increase,
            'min_profit_margin': min_profit,
            'absolute_stop_loss_pct': absolute_stop_loss_pct,  # 🔧 절대 손절값 파라미터 추가
            'recent_high_period': recent_high_period,  # 🔧 최고값 조사 기간 파라미터 추가
            'max_decline_from_high_pct': max_decline_from_high_pct  # 🔧 최대 하락률 파라미터 추가
        }
    
    return result

def test_parameter_combination(json_files, selected_folder, cross_pattern, uptrend_length, 
                             min_increase, max_increase, min_profit, absolute_stop_loss_pct,
                             recent_high_period, max_decline_from_high_pct):
    """
    특정 파라미터 조합으로 백테스팅 테스트
    """
    all_trades = []
    
    for file in json_files[:10]:  # 처리 속도를 위해 10개 파일만 테스트
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'meta' not in data or 'data' not in data:
                continue
                
            code = data['meta'].get('code', file.name)
            chart_data = data['data']
            
            if not chart_data:
                continue
                
            df = pd.DataFrame(chart_data)
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            
            if not all(col in df.columns for col in required_columns):
                continue
                
            try:
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
            except:
                try:
                    df['date'] = pd.to_datetime(df['date'])
                except:
                    continue
            
            df = df[required_columns]
            daily_df = convert_to_daily(df)
            
            if daily_df.empty:
                continue
                
            daily_df['code'] = code
            
            # 임시 파라미터로 조건 검색 (디버그 출력 없이)
            valid_dates = check_conditions_silent(daily_df, cross_pattern, uptrend_length, 
                                                min_increase, max_increase, min_profit, absolute_stop_loss_pct,
                                                recent_high_period, max_decline_from_high_pct)
            
            if valid_dates:
                backtest_df = backtest_strategy(daily_df, df, valid_dates)
                if not backtest_df.empty:
                    all_trades.append(backtest_df)
                    
        except Exception:
            continue
    
    # 결과 집계
    if all_trades:
        combined_trades = pd.concat(all_trades, ignore_index=True)
        total_trades = len(combined_trades)
        win_trades = len(combined_trades[combined_trades['수익률'] > 0])
        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
        avg_return = combined_trades['수익률'].mean() if total_trades > 0 else 0
        max_return = combined_trades['수익률'].max() if total_trades > 0 else 0
        min_return = combined_trades['수익률'].min() if total_trades > 0 else 0
        
        return {
            'total_trades': total_trades,
            'win_trades': win_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'max_return': max_return,
            'min_return': min_return
        }
    
    return {
        'total_trades': 0,
        'win_trades': 0,
        'win_rate': 0,
        'avg_return': 0,
        'max_return': 0,
        'min_return': 0
    }

def check_conditions_silent(daily_df, cross_pattern, uptrend_length, min_increase, max_increase, min_profit, absolute_stop_loss_pct,
                            recent_high_period, max_decline_from_high_pct):
    """
    디버그 출력 없이 조건 검색하는 함수 (최적화용)
    """
    # EMA 계산 (15:10 데이터 기준)
    daily_df['EMA22'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 🔧 새로운 조건: 최고값 기준 하락률 계산 (기본값 30일)
    recent_high_period = 30  # 최적화용에서는 고정값 사용
    daily_df[f'recent_high_{recent_high_period}d'] = daily_df['high'].rolling(window=recent_high_period).max()
    max_decline_from_high_pct = 30.0  # 기본값 30% 사용
    
    valid_dates = []
    
    # 🔧 오류 수정 4: 인덱스 범위 체크 강화 + 최고값 기간 반영
    min_data_needed = max(120, 50, cross_pattern + uptrend_length + 1, recent_high_period)  # 최고값 계산을 위한 충분한 데이터
    
    for idx in range(min_data_needed, len(daily_df)):
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        try:
            # 조건 1: EMA22 회복 패턴 확인
            condition_1 = True
            
            # 1-N봉전: EMA22 아래
            for i in range(1, cross_pattern + 1):
                if idx - i < 0:
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] >= past_row['EMA22']:
                    condition_1 = False
                    break
            
            # (cross_pattern+1)봉전부터: EMA22 위
            if condition_1:
                for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                    if idx - i < 0:
                        condition_1 = False
                        break
                    past_row = daily_df.iloc[idx-i]
                    if past_row['close_1510'] <= past_row['EMA22']:
                        condition_1 = False
                        break
            
            if not condition_1:
                continue
                
            # 조건 2: 1봉전 EMA22 > EMA60 > EMA120
            if not (prev_row['EMA22'] > prev_row['EMA60'] > prev_row['EMA120']):
                continue
            
            # 조건 3: 당일 EMA22 회복
            if not (current_row['close_1510'] > current_row['EMA22']):
                continue
            
            # 조건 4: MA22 돌파
            if not (current_row['close_1510'] > current_row['MA22_1510']):
                continue
            
            # 조건 5: 고가 제한
            if not (current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3)):
                continue
            
            # 조건 6: 상승률 범위
            price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
            if not (min_increase <= price_increase <= max_increase):
                continue
            
                        # 조건 7: 수익률 마진
            buy_price = current_row['close_1510']
            
            # 🔧 개선: 절대 손절값 계산 (5% 기본값)
            ema50_stop_loss = current_row['EMA50']
            absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            
            # 🎯 익절가/손절가 통합 계산 (중심값 기준)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2  # 중심값
            
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나 매수가보다 높으면 절대손절 기준으로 계산
                risk_amount = buy_price * (absolute_stop_loss_pct / 100)
                take_profit = buy_price + (risk_amount * 1.5)
                # 손절가는 절대손절값만 사용
                final_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            else:
                # EMA50 기준 중심값으로 위험금액 계산
                risk_amount = candle_center - ema50_stop_loss
                take_profit = buy_price + (risk_amount * 1.5)
                # 손절가는 EMA50과 절대손절값 중 높은 값
                final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
            
            # 🔧 최소 5% 강제 설정 제거 - 계산된 익절가 그대로 사용
            
            if not (take_profit >= buy_price * (1 + min_profit / 100)):
                continue
            
            # 🔧 조건 8: 최고값 대비 하락률 제한
            recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
            max_decline_from_high_pct = float(safe_get_config(config, 'COMMON', 'max_decline_from_high_pct', '30.0'))
            
            recent_high_column = f'recent_high_{recent_high_period}d'
            if recent_high_column in current_row.index and not pd.isna(current_row[recent_high_column]):
                recent_high = current_row[recent_high_column]
                decline_from_high = ((recent_high - buy_price) / recent_high) * 100 if recent_high > 0 else 0  # 🔧 올바른 하락률 계산
                if decline_from_high > max_decline_from_high_pct:
                    continue
            
            valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
                
        except Exception:
            continue
    
    return valid_dates

def save_optimization_results(results, folder_path):
    """
    최적화 결과를 엑셀 파일로 저장
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    optimization_dir = folder_path / 'optimization_results'
    optimization_dir.mkdir(exist_ok=True)
    
    # 결과를 DataFrame으로 변환
    results_data = []
    for result in results:
        row = {
            'ID': result['combination_id'],
            'cross_pattern': result['parameters']['cross_pattern'],
            'uptrend_length': result['parameters']['uptrend_candle_length'],
            'min_price_increase': result['parameters']['min_price_increase'],
            'max_price_increase': result['parameters']['max_price_increase'],
            'min_profit_margin': result['parameters']['min_profit_margin'],
            'absolute_stop_loss_pct': result['parameters']['absolute_stop_loss_pct'],
            'recent_high_period': result['parameters']['recent_high_period'],  # 🔧 최고값 조사 기간 추가
            'max_decline_from_high_pct': result['parameters']['max_decline_from_high_pct'],  # 🔧 최대 하락률 추가
            'total_trades': result['total_trades'],
            'win_trades': result['win_trades'],
            'win_rate': round(result['win_rate'], 2),
            'avg_return': round(result['avg_return'], 2),
            'max_return': round(result['max_return'], 2),
            'min_return': round(result['min_return'], 2)
        }
        results_data.append(row)
    
    results_df = pd.DataFrame(results_data)
    
    # 성과 순으로 정렬 (승률 우선, 평균수익률 차순)
    results_df = results_df.sort_values(['win_rate', 'avg_return'], ascending=[False, False])
    
    excel_file = optimization_dir / f"optimization_results_{timestamp}.xlsx"
    results_df.to_excel(excel_file, index=False)
    
    print(f"\n최적화 결과가 {excel_file.name}에 저장되었습니다.")
    return excel_file

def analyze_optimization_results(results):
    """
    최적화 결과 분석 및 추천
    """
    print(f"\n=== 최적화 결과 분석 ===")
    
    # 거래가 있는 결과만 필터링
    valid_results = [r for r in results if r['total_trades'] > 0]
    
    if not valid_results:
        print("거래가 발생한 조합이 없습니다.")
        return
    
    print(f"총 {len(results)}개 조합 중 {len(valid_results)}개 조합에서 거래 발생")
    
    # 최고 승률 조합
    best_winrate = max(valid_results, key=lambda x: x['win_rate'])
    print(f"\n🏆 최고 승률: {best_winrate['win_rate']:.1f}%")
    print(f"   파라미터: {best_winrate['parameters']}")
    print(f"   거래수: {best_winrate['total_trades']}건, 평균수익률: {best_winrate['avg_return']:.2f}%")
    
    # 최고 평균수익률 조합
    best_return = max(valid_results, key=lambda x: x['avg_return'])
    print(f"\n💰 최고 평균수익률: {best_return['avg_return']:.2f}%")
    print(f"   파라미터: {best_return['parameters']}")
    print(f"   거래수: {best_return['total_trades']}건, 승률: {best_return['win_rate']:.1f}%")
    
    # 균형잡힌 조합 (승률 50% 이상, 평균수익률 양수)
    balanced_results = [r for r in valid_results if r['win_rate'] >= 50 and r['avg_return'] > 0]
    if balanced_results:
        # 승률과 평균수익률의 곱으로 점수 계산
        for r in balanced_results:
            r['score'] = r['win_rate'] * r['avg_return'] / 100
        
        best_balanced = max(balanced_results, key=lambda x: x['score'])
        print(f"\n⚖️ 균형잡힌 추천: 승률 {best_balanced['win_rate']:.1f}%, 평균수익률 {best_balanced['avg_return']:.2f}%")
        print(f"   파라미터: {best_balanced['parameters']}")
        print(f"   거래수: {best_balanced['total_trades']}건")
        
        # 추천 설정 적용 제안
        print(f"\n💡 추천 설정을 backtest_config.ini에 적용하시겠습니까? (y/n): ", end="")
        apply_choice = input()
        if apply_choice.lower() == 'y':
            apply_recommended_settings(best_balanced['parameters'])
    else:
        print(f"\n⚠️ 승률 50% 이상의 균형잡힌 조합이 없습니다.")
        print("파라미터 범위를 조정하거나 조건을 완화해보세요.")

def apply_recommended_settings(recommended_params):
    """
    추천 설정을 config 파일에 적용
    """
    try:
        config = configparser.ConfigParser()
        config.read('backtest_config.ini', encoding='utf-8')
        
        # 각 조건에 추천 파라미터 적용
        for condition_num in [1, 2, 3]:
            section = f'CONDITION_{condition_num}'
            if section in config:
                if condition_num == 1:
                    config[section]['cross_pattern'] = str(recommended_params['cross_pattern'])
                elif condition_num == 2:
                    config[section]['cross_pattern'] = str(min(recommended_params['cross_pattern'] + 1, 3))
                elif condition_num == 3:
                    config[section]['cross_pattern'] = str(min(recommended_params['cross_pattern'] + 2, 3))
                
                config[section]['uptrend_candle_length'] = str(recommended_params['uptrend_candle_length'])
        
        # 공통 설정 적용
        if 'COMMON' in config:
            config['COMMON']['min_price_increase'] = str(recommended_params['min_price_increase'])
            config['COMMON']['max_price_increase'] = str(recommended_params['max_price_increase'])
            config['COMMON']['min_profit_margin'] = str(recommended_params['min_profit_margin'])
            config['COMMON']['absolute_stop_loss_pct'] = str(recommended_params['absolute_stop_loss_pct'])  # 🔧 절대 손절값 적용
            config['COMMON']['recent_high_period'] = str(recommended_params['recent_high_period'])  # 🔧 최고값 조사 기간 적용
            config['COMMON']['max_decline_from_high_pct'] = str(recommended_params['max_decline_from_high_pct'])  # 🔧 최대 하락률 적용
        
        # 파일 저장
        with open('backtest_config.ini', 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        
        print("✅ 추천 설정이 backtest_config.ini에 적용되었습니다!")
        print("이제 메뉴 1번으로 전체 백테스팅을 실행해보세요.")
        
    except Exception as e:
        print(f"❌ 설정 적용 중 오류: {e}")

def optimize_absolute_stop_loss():
    """
    절대 손절값 최적화 실행 함수
    """
    global MAX_WORKERS, CHUNK_SIZE
    
    print("\n=== 절대 손절값 최적화 프로그램 ===")
    print("다양한 절대 손절값을 테스트하여 최적의 손절값을 찾습니다.")
    
    # 🔧 현재 설정 파일에서 고정 파라미터 읽기
    config = load_config()
    common = config['COMMON']
    condition_1 = config['CONDITION_1']
    
    # 현재 설정값으로 고정
    cross_pattern = int(condition_1['cross_pattern'])
    uptrend_length = int(condition_1['uptrend_candle_length'])
    min_increase = float(common['min_price_increase'])
    max_increase = float(common['max_price_increase'])
    min_profit = float(common['min_profit_margin'])
    current_absolute_stop_loss = float(common['absolute_stop_loss_pct'])
    
    print(f"\n📋 사용 중인 고정 파라미터:")
    print(f"   - cross_pattern: {cross_pattern}")
    print(f"   - uptrend_length: {uptrend_length}")
    print(f"   - min_price_increase: {min_increase}%")
    print(f"   - max_price_increase: {max_increase}%")
    print(f"   - min_profit_margin: {min_profit}%")
    print(f"   - 현재 절대손절값: {current_absolute_stop_loss}%")
    
    # 폴더 선택
    base_dir = Path('json_data')
    if not base_dir.exists():
        print("json_data 폴더가 존재하지 않습니다.")
        return
        
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        print("분석할 데이터 폴더가 없습니다.")
        return
        
    print("\n=== 사용 가능한 데이터 폴더 ===")
    for idx, folder in enumerate(folders, 1):
        print(f"{idx}. {folder.name}")
    
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
    
    # 사용자가 직접 프로세스 수 선택 가능
    num_workers = input(f"사용할 프로세스 수 (1-{multiprocessing.cpu_count()}, 기본값: {MAX_WORKERS}): ") or str(MAX_WORKERS)
    MAX_WORKERS = int(num_workers)
    
    # 테스트할 파일 수 제한 (빠른 테스트를 위해)
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return
    
    # 파일 수 제한 옵션
    print(f"\n총 {len(json_files)}개 파일이 있습니다.")
    print("1. 전체 파일로 테스트 (정확하지만 느림)")
    print("2. 샘플 파일로 테스트 (빠르지만 근사치)")
    
    test_choice = input("선택하세요 (1 또는 2): ")
    if test_choice == '2':
        sample_size = min(50, len(json_files))  # 최대 50개 파일
        json_files = json_files[:sample_size]
        print(f"샘플 {sample_size}개 파일로 테스트합니다.")
    
    # 최적화 파라미터 정의
    optimization_params = {
        'absolute_stop_loss_pct': [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0]  # 5%-15% 범위
    }
    
    print(f"\n=== 최적화 파라미터 범위 ===")
    for param, values in optimization_params.items():
        print(f"{param}: {values}")
    
    # 모든 조합 계산
    param_combinations = list(itertools.product(*optimization_params.values()))
    total_combinations = len(param_combinations)
    
    print(f"\n총 {total_combinations}개 조합을 테스트합니다.")
    print(f"🚀 멀티프로세싱: {MAX_WORKERS}개 프로세스 사용")
    
    # 최적화 시작 확인
    confirm = input("최적화를 시작하시겠습니까? (y/n): ")
    if confirm.lower() != 'y':
        return
    
    # 🚀 멀티프로세싱으로 최적화 실행
    print(f"\n=== 최적화 진행 중 (멀티프로세싱: {MAX_WORKERS}개 프로세스) ===")
    
    # 조합을 청크로 나누기
    combination_chunks = [param_combinations[i:i + MAX_WORKERS] 
                         for i in range(0, len(param_combinations), MAX_WORKERS)]
    
    optimization_results = []
    
    # tqdm을 사용하여 진행 상황 표시
    with tqdm(total=total_combinations, desc="절대 손절값 최적화 진행") as pbar:
        for chunk in combination_chunks:
            # 🚀 병렬 처리
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_combination = {
                    executor.submit(
                        optimize_absolute_stop_loss_wrapper,
                        combination, json_files, selected_folder, idx + len(optimization_results) + 1
                    ): combination 
                    for idx, combination in enumerate(chunk)
                }
                
                for future in future_to_combination:
                    result = future.result()
                    if result:
                        optimization_results.append(result)
                        
                        # 중간 결과 출력
                        if result['total_trades'] > 0:
                            tqdm.write(f"손절값 {result['parameters']['absolute_stop_loss_pct']:.1f}%: "
                                     f"{result['total_trades']}건 거래, "
                                     f"승률 {result['win_rate']:.1f}%, "
                                     f"평균수익률 {result['avg_return']:.2f}%")
                    
                    pbar.update(1)
    
    # 최적화 결과 분석 및 저장
    if optimization_results:
        save_absolute_stop_loss_results(optimization_results, selected_folder)
        analyze_absolute_stop_loss_results(optimization_results)
    else:
        print("최적화 결과가 없습니다.")

def optimize_absolute_stop_loss_wrapper(combination, json_files, selected_folder, combination_id):
    """
    멀티프로세싱용 래퍼 함수
    """
    absolute_stop_loss_pct = combination[0]
    
    result = optimize_absolute_stop_loss_test(
        json_files, selected_folder, absolute_stop_loss_pct
    )
    
    if result:
        result['combination_id'] = combination_id
        result['parameters'] = {
            'absolute_stop_loss_pct': absolute_stop_loss_pct
        }
    
    return result

def optimize_absolute_stop_loss_test(json_files, selected_folder, absolute_stop_loss_pct):
    """
    특정 절대 손절값으로 백테스팅 테스트 (현재 설정값 사용)
    """
    # 🔧 현재 설정 파일에서 고정 파라미터 읽기
    config = load_config()
    common = config['COMMON']
    condition_1 = config['CONDITION_1']
    
    # 현재 설정값으로 고정
    cross_pattern = int(condition_1['cross_pattern'])
    uptrend_length = int(condition_1['uptrend_candle_length'])
    min_increase = float(common['min_price_increase'])
    max_increase = float(common['max_price_increase'])
    min_profit = float(common['min_profit_margin'])
    
    all_trades = []
    
    for file in json_files[:20]:  # 절대손절값 최적화는 더 많은 파일로 테스트
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'meta' not in data or 'data' not in data:
                continue
                
            code = data['meta'].get('code', file.name)
            chart_data = data['data']
            
            if not chart_data:
                continue
                
            df = pd.DataFrame(chart_data)
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            
            if not all(col in df.columns for col in required_columns):
                continue
                
            try:
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
            except:
                try:
                    df['date'] = pd.to_datetime(df['date'])
                except:
                    continue
            
            df = df[required_columns]
            daily_df = convert_to_daily(df)
            
            if daily_df.empty:
                continue
                
            daily_df['code'] = code
            
            # 🔧 현재 설정값과 테스트할 절대손절값으로 조건 검색
            valid_dates = check_conditions_silent(
                daily_df, cross_pattern, uptrend_length, 
                min_increase, max_increase, min_profit, absolute_stop_loss_pct,
                recent_high_period, max_decline_from_high_pct
            )
            
            if valid_dates:
                backtest_df = backtest_strategy(daily_df, df, valid_dates)
                if not backtest_df.empty:
                    all_trades.append(backtest_df)
                    
        except Exception:
            continue
    
    # 결과 집계
    if all_trades:
        combined_trades = pd.concat(all_trades, ignore_index=True)
        total_trades = len(combined_trades)
        win_trades = len(combined_trades[combined_trades['수익률'] > 0])
        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
        avg_return = combined_trades['수익률'].mean() if total_trades > 0 else 0
        max_return = combined_trades['수익률'].max() if total_trades > 0 else 0
        min_return = combined_trades['수익률'].min() if total_trades > 0 else 0
        
        return {
            'total_trades': total_trades,
            'win_trades': win_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'max_return': max_return,
            'min_return': min_return
        }
    
    return {
        'total_trades': 0,
        'win_trades': 0,
        'win_rate': 0,
        'avg_return': 0,
        'max_return': 0,
        'min_return': 0
    }

def save_absolute_stop_loss_results(results, folder_path):
    """
    절대 손절값 최적화 결과를 엑셀 파일로 저장
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    optimization_dir = folder_path / 'optimization_results'
    optimization_dir.mkdir(exist_ok=True)
    
    # 결과를 DataFrame으로 변환
    results_data = []
    for result in results:
        row = {
            'ID': result['combination_id'],
            'absolute_stop_loss_pct': result['parameters']['absolute_stop_loss_pct'],
            'total_trades': result['total_trades'],
            'win_trades': result['win_trades'],
            'win_rate': round(result['win_rate'], 2),
            'avg_return': round(result['avg_return'], 2),
            'max_return': round(result['max_return'], 2),
            'min_return': round(result['min_return'], 2)
        }
        results_data.append(row)
    
    results_df = pd.DataFrame(results_data)
    
    # 성과 순으로 정렬 (승률 우선, 평균수익률 차순)
    results_df = results_df.sort_values(['win_rate', 'avg_return'], ascending=[False, False])
    
    excel_file = optimization_dir / f"absolute_stop_loss_optimization_{timestamp}.xlsx"
    results_df.to_excel(excel_file, index=False)
    
    print(f"\n절대 손절값 최적화 결과가 {excel_file.name}에 저장되었습니다.")
    return excel_file

def analyze_absolute_stop_loss_results(results):
    """
    절대 손절값 최적화 결과 분석 및 추천
    """
    print(f"\n=== 절대 손절값 최적화 결과 분석 ===")
    
    # 거래가 있는 결과만 필터링
    valid_results = [r for r in results if r['total_trades'] > 0]
    
    if not valid_results:
        print("거래가 발생한 조합이 없습니다.")
        return
    
    print(f"총 {len(results)}개 조합 중 {len(valid_results)}개 조합에서 거래 발생")
    
    # 최고 승률 조합
    best_winrate = max(valid_results, key=lambda x: x['win_rate'])
    print(f"\n🏆 최고 승률: {best_winrate['win_rate']:.1f}%")
    print(f"   절대손절값: {best_winrate['parameters']['absolute_stop_loss_pct']:.1f}%")
    print(f"   거래수: {best_winrate['total_trades']}건, 평균수익률: {best_winrate['avg_return']:.2f}%")
    
    # 최고 평균수익률 조합
    best_return = max(valid_results, key=lambda x: x['avg_return'])
    print(f"\n💰 최고 평균수익률: {best_return['avg_return']:.2f}%")
    print(f"   절대손절값: {best_return['parameters']['absolute_stop_loss_pct']:.1f}%")
    print(f"   거래수: {best_return['total_trades']}건, 승률: {best_return['win_rate']:.1f}%")
    
    # 균형잡힌 조합 (승률 50% 이상, 평균수익률 양수)
    balanced_results = [r for r in valid_results if r['win_rate'] >= 50 and r['avg_return'] > 0]
    if balanced_results:
        # 승률과 평균수익률의 곱으로 점수 계산
        for r in balanced_results:
            r['score'] = r['win_rate'] * r['avg_return'] / 100
        
        best_balanced = max(balanced_results, key=lambda x: x['score'])
        print(f"\n⚖️ 균형잡힌 추천: 승률 {best_balanced['win_rate']:.1f}%, 평균수익률 {best_balanced['avg_return']:.2f}%")
        print(f"   절대손절값: {best_balanced['parameters']['absolute_stop_loss_pct']:.1f}%")
        print(f"   거래수: {best_balanced['total_trades']}건")
        
        # 추천 설정 적용 제안
        print(f"\n💡 추천 절대손절값 {best_balanced['parameters']['absolute_stop_loss_pct']:.1f}%를 backtest_config.ini에 적용하시겠습니까? (y/n): ", end="")
        apply_choice = input()
        if apply_choice.lower() == 'y':
            apply_recommended_absolute_stop_loss(best_balanced['parameters']['absolute_stop_loss_pct'])
    else:
        print(f"\n⚠️ 승률 50% 이상의 균형잡힌 조합이 없습니다.")
        print("절대손절값 범위를 조정해보세요.")

def apply_recommended_absolute_stop_loss(recommended_value):
    """
    추천 절대손절값을 config 파일에 적용
    """
    try:
        config = configparser.ConfigParser()
        config.read('backtest_config.ini', encoding='utf-8')
        
        # 공통 설정 적용
        if 'COMMON' in config:
            config['COMMON']['absolute_stop_loss_pct'] = str(recommended_value)
        
        # 파일 저장
        with open('backtest_config.ini', 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        
        print(f"✅ 추천 절대손절값 {recommended_value:.1f}%가 backtest_config.ini에 적용되었습니다!")
        print("이제 메뉴 1번으로 전체 백테스팅을 실행해보세요.")
        
    except Exception as e:
        print(f"❌ 설정 적용 중 오류: {repr(e)}")

def debug_conditions(daily_df, condition_set=1, debug_mode=True):
    """
    조건별 통과 현황을 디버깅하는 함수
    """
    if not debug_mode:
        return check_conditions(daily_df, condition_set)
    
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    
    if condition_key not in config:
        print(f"조건 세트 {condition_set}를 찾을 수 없습니다.")
        return []
    
    condition = config[condition_key]
    common = config['COMMON']
    
    # 설정에서 값 읽기
    cross_pattern = int(safe_get_config(config, condition_key, 'cross_pattern', '1'))
    uptrend_length = int(safe_get_config(config, condition_key, 'uptrend_candle_length', '7'))
    min_increase = float(safe_get_config(config, 'COMMON', 'min_price_increase', '2'))
    max_increase = float(safe_get_config(config, 'COMMON', 'max_price_increase', '10'))
    min_profit = float(safe_get_config(config, 'COMMON', 'min_profit_margin', '4'))
    
    # EMA 계산
    daily_df['EMA22_1510'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60_1510'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120_1510'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    daily_df['MA22'] = daily_df['close'].rolling(window=22).mean()
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 최고값 기준 하락률 계산
    recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
    daily_df[f'recent_high_{recent_high_period}d'] = daily_df['high'].rolling(window=recent_high_period).max()
    max_decline_from_high_pct = float(safe_get_config(config, 'COMMON', 'max_decline_from_high_pct', '50.0'))
    
    valid_dates = []
    condition_stats = {
        'total_checked': 0,
        'condition_1_pass': 0,
        'condition_2_pass': 0,
        'condition_3_pass': 0,
        'condition_4_pass': 0,
        'condition_5_pass': 0,
        'condition_6_pass': 0,
        'condition_7_pass': 0,
        'condition_8_pass': 0,
        'all_conditions_pass': 0
    }
    
    min_data_needed = max(120, 50, cross_pattern + uptrend_length + 1, recent_high_period)
    
    print(f"🔍 디버깅 모드: 최소 데이터 {min_data_needed}봉, 총 데이터 {len(daily_df)}봉")
    print(f"📊 조건 설정: cross_pattern={cross_pattern}, uptrend_length={uptrend_length}")
    print(f"📊 가격 조건: min_increase={min_increase}%, max_increase={max_increase}%")
    print(f"📊 최고값 조건: recent_high_period={recent_high_period}일, max_decline={max_decline_from_high_pct}%")
    
    for idx in range(min_data_needed, len(daily_df)):
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        condition_stats['total_checked'] += 1
        
        try:
            # 조건 1: EMA22 회복 패턴 확인
            condition_1 = True
            for i in range(1, cross_pattern + 1):
                if idx - i < 0:
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] >= past_row['EMA22_1510']:
                    condition_1 = False
                    break
            
            if condition_1:
                for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                    if idx - i < 0:
                        condition_1 = False
                        break
                    past_row = daily_df.iloc[idx-i]
                    if past_row['close_1510'] <= past_row['EMA22_1510']:
                        condition_1 = False
                        break
            
            if condition_1:
                condition_stats['condition_1_pass'] += 1
                
                # 조건 2: EMA 순서
                condition_2 = (prev_row['EMA22_1510'] > prev_row['EMA60_1510'] > prev_row['EMA120_1510'])
                if condition_2:
                    condition_stats['condition_2_pass'] += 1
                    
                    # 조건 3: 당일 EMA22 회복
                    condition_3 = (current_row['close_1510'] > current_row['EMA22_1510'])
                    if condition_3:
                        condition_stats['condition_3_pass'] += 1
                        
                        # 조건 4: MA22 돌파
                        condition_4 = (current_row['close_1510'] > current_row['MA22_1510'])
                        if condition_4:
                            condition_stats['condition_4_pass'] += 1
                            
                            # 조건 5: 고가 제한
                            condition_5 = (current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3))
                            if condition_5:
                                condition_stats['condition_5_pass'] += 1
                                
                                # 조건 6: 상승률 범위
                                price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
                                condition_6 = min_increase <= price_increase <= max_increase
                                if condition_6:
                                    condition_stats['condition_6_pass'] += 1
                                    
                                    # 조건 7: 수익 마진
                                    candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2
                                    buy_price = current_row['close_1510']
                                    
                                    absolute_stop_loss_pct = float(safe_get_config(config, 'COMMON', 'absolute_stop_loss_pct', '5.0'))
                                    ema50_stop_loss = current_row['EMA50']
                                    absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
                                    
                                    # 🔧 올바른 손절 로직: EMA50이 매수가보다 낮을 때만 손절가로 사용
                                    if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                                        # EMA50이 없거나, 매수가보다 높으면(이익구간) 절대손절만 사용
                                        final_stop_loss = absolute_stop_loss
                                    else:
                                        # EMA50이 매수가보다 낮으면 두 손절가 중 더 높은(안전한) 값 사용
                                        final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
                                    
                                    if candle_center > final_stop_loss:
                                        take_profit = candle_center + ((candle_center - final_stop_loss) * 1.5)
                                    else:
                                        take_profit = buy_price * 1.05
                                    
                                    if take_profit <= buy_price:
                                        take_profit = buy_price * 1.05
                                    
                                    condition_7 = take_profit >= buy_price * (1 + min_profit / 100)
                                    if condition_7:
                                        condition_stats['condition_7_pass'] += 1
                                        
                                        # 조건 8: 최고값 대비 하락률 제한
                                        recent_high_column = f'recent_high_{recent_high_period}d'
                                        condition_8 = True
                                        if recent_high_column in current_row.index and not pd.isna(current_row[recent_high_column]):
                                            recent_high = current_row[recent_high_column]
                                            decline_from_high = ((recent_high - buy_price) / recent_high) * 100 if recent_high > 0 else 0
                                            condition_8 = decline_from_high <= max_decline_from_high_pct
                                            
                                            if condition_stats['total_checked'] % 100 == 0:  # 100개마다 샘플 출력
                                                print(f"📊 샘플: 날짜={current_row['date'].strftime('%Y-%m-%d')}, "
                                                      f"최고값={recent_high:.0f}, 매수값={buy_price:.0f}, "
                                                      f"하락률={decline_from_high:.1f}% (기준: {max_decline_from_high_pct}%)")
                                        
                                        if condition_8:
                                            condition_stats['condition_8_pass'] += 1
                                            condition_stats['all_conditions_pass'] += 1
                                            valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
                                        
        except Exception as e:
            continue
    
    # 🔧 순차적 필터링 방식으로 변경
    return check_conditions_with_stats(daily_df, condition_set)

def check_conditions_with_stats(daily_df, condition_set=1):
    """
    조건별 통과 현황을 순차적으로 보여주는 함수
    """
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    
    if condition_key not in config:
        print(f"조건 세트 {condition_set}를 찾을 수 없습니다.")
        return []
    
    condition = config[condition_key]
    common = config['COMMON']
    
    # 설정에서 값 읽기
    cross_pattern = int(safe_get_config(config, condition_key, 'cross_pattern', '1'))
    uptrend_length = int(safe_get_config(config, condition_key, 'uptrend_candle_length', '7'))
    min_increase = float(safe_get_config(config, 'COMMON', 'min_price_increase', '2'))
    max_increase = float(safe_get_config(config, 'COMMON', 'max_price_increase', '10'))
    min_profit = float(safe_get_config(config, 'COMMON', 'min_profit_margin', '4'))
    
    # EMA 계산 (15:10 데이터 기준)
    daily_df['EMA22_1510'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60_1510'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120_1510'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    daily_df['MA22'] = daily_df['close'].rolling(window=22).mean()
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 최고값 기준 하락률 계산
    recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
    daily_df[f'recent_high_{recent_high_period}d'] = daily_df['high'].rolling(window=recent_high_period).max()
    max_decline_from_high_pct = float(safe_get_config(config, 'COMMON', 'max_decline_from_high_pct', '30.0'))
    
    min_data_needed = max(120, 50, cross_pattern + uptrend_length + 1, recent_high_period)
    
    # 검사 대상 데이터 준비
    candidates = []
    for idx in range(min_data_needed, len(daily_df)):
        if idx < cross_pattern + uptrend_length:
            continue
        candidates.append(idx)
    
    print(f"\n📊 조건별 통과 현황 (순차 필터링)")
    print(f"   🔍 검사 대상: {len(candidates):,}개 봉")
    
    # 조건 1: EMA22 반등 패턴
    condition1_pass = []
    for idx in candidates:
        try:
            condition_1 = True
            
            # 1-N봉전: EMA22 아래
            for i in range(1, cross_pattern + 1):
                if idx - i < 0:
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] >= past_row['EMA22_1510']:
                    condition_1 = False
                    break
            
            # (cross_pattern+1)봉전부터: EMA22 위
            if condition_1:
                for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                    if idx - i < 0:
                        condition_1 = False
                        break
                    past_row = daily_df.iloc[idx-i]
                    if past_row['close_1510'] <= past_row['EMA22_1510']:
                        condition_1 = False
                        break
            
            if condition_1:
                condition1_pass.append(idx)
                
        except Exception:
            continue
    
    print(f"   ✅ 조건1 (EMA22반등): {len(condition1_pass):,}개 통과 ({len(condition1_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 2: EMA 순서
    condition2_pass = []
    for idx in condition1_pass:
        prev_row = daily_df.iloc[idx-1]
        if prev_row['EMA22_1510'] > prev_row['EMA60_1510'] > prev_row['EMA120_1510']:
            condition2_pass.append(idx)
    
    print(f"   ✅ 조건2 (EMA순서): {len(condition2_pass):,}개 통과 ({len(condition2_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 3: 당일 EMA22 회복
    condition3_pass = []
    for idx in condition2_pass:
        current_row = daily_df.iloc[idx]
        if current_row['close_1510'] > current_row['EMA22_1510']:
            condition3_pass.append(idx)
    
    print(f"   ✅ 조건3 (EMA회복): {len(condition3_pass):,}개 통과 ({len(condition3_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 4: MA22 돌파
    condition4_pass = []
    for idx in condition3_pass:
        current_row = daily_df.iloc[idx]
        if current_row['close_1510'] > current_row['MA22_1510']:
            condition4_pass.append(idx)
    
    print(f"   ✅ 조건4 (MA22돌파): {len(condition4_pass):,}개 통과 ({len(condition4_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 5: 고가 제한
    condition5_pass = []
    for idx in condition4_pass:
        current_row = daily_df.iloc[idx]
        if current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3):
            condition5_pass.append(idx)
    
    print(f"   ✅ 조건5 (고가제한): {len(condition5_pass):,}개 통과 ({len(condition5_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 6: 상승률 범위
    condition6_pass = []
    for idx in condition5_pass:
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
        if min_increase <= price_increase <= max_increase:
            condition6_pass.append(idx)
    
    print(f"   ✅ 조건6 (상승률{min_increase}-{max_increase}%): {len(condition6_pass):,}개 통과 ({len(condition6_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 7: 수익 마진
    condition7_pass = []
    for idx in condition6_pass:
        current_row = daily_df.iloc[idx]
        
        try:
            buy_price = current_row['close_1510']
            
            absolute_stop_loss_pct = float(safe_get_config(config, 'COMMON', 'absolute_stop_loss_pct', '5.0'))
            ema50_stop_loss = current_row['EMA50']
            absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            
            # 🔧 올바른 손절 로직: EMA50이 매수가보다 낮을 때만 손절가로 사용
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나, 매수가보다 높으면(이익구간) 절대손절만 사용
                final_stop_loss = absolute_stop_loss
            else:
                # EMA50이 매수가보다 낮으면 두 손절가 중 더 높은(안전한) 값 사용
                final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
            
            # 🎯 익절가/손절가 통합 계산 (중심값 기준)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2  # 중심값
            
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나 매수가보다 높으면 절대손절 기준으로 계산
                risk_amount = buy_price * (absolute_stop_loss_pct / 100)
                take_profit = buy_price + (risk_amount * 1.5)
            else:
                # EMA50 기준 중심값으로 위험금액 계산
                risk_amount = candle_center - ema50_stop_loss
                take_profit = buy_price + (risk_amount * 1.5)
            
            if take_profit >= buy_price * (1 + min_profit / 100):
                condition7_pass.append(idx)
                
        except Exception:
            continue
    
    print(f"   ✅ 조건7 (수익마진{min_profit}%+): {len(condition7_pass):,}개 통과 ({len(condition7_pass)/len(candidates)*100:.1f}%)")
    
    # 조건 8: 최고값 대비 하락률 제한
    condition8_pass = []
    for idx in condition7_pass:
        current_row = daily_df.iloc[idx]
        buy_price = current_row['close_1510']
        
        recent_high_column = f'recent_high_{recent_high_period}d'
        if recent_high_column in current_row.index and not pd.isna(current_row[recent_high_column]):
            recent_high = current_row[recent_high_column]
            decline_from_high = ((recent_high - buy_price) / recent_high) * 100 if recent_high > 0 else 0
            if decline_from_high <= max_decline_from_high_pct:
                condition8_pass.append(idx)
        else:
            condition8_pass.append(idx)  # 데이터가 없으면 통과
    
    print(f"   ✅ 조건8 (하락률≤{max_decline_from_high_pct}%): {len(condition8_pass):,}개 통과 ({len(condition8_pass)/len(candidates)*100:.1f}%)")
    
    # 최종 결과
    valid_dates = []
    for idx in condition8_pass:
        current_row = daily_df.iloc[idx]
        valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
    
    print(f"   🎯 최종결과: {len(valid_dates):,}개 매수신호 ({len(valid_dates)/len(candidates)*100:.1f}%)")
    
    return valid_dates

def get_file_condition_stats(daily_df, condition_set=1):
    """
    개별 파일의 조건별 통과 현황을 수집하는 함수 (출력 없음)
    """
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    
    if condition_key not in config:
        return {
            'total_candidates': 0,
            'condition1_pass': 0, 'condition2_pass': 0, 'condition3_pass': 0, 'condition4_pass': 0,
            'condition5_pass': 0, 'condition6_pass': 0, 'condition7_pass': 0, 'condition8_pass': 0,
            'final_signals': 0
        }
    
    condition = config[condition_key]
    common = config['COMMON']
    
    # 설정에서 값 읽기
    cross_pattern = int(safe_get_config(config, condition_key, 'cross_pattern', '1'))
    uptrend_length = int(safe_get_config(config, condition_key, 'uptrend_candle_length', '7'))
    min_increase = float(safe_get_config(config, 'COMMON', 'min_price_increase', '2'))
    max_increase = float(safe_get_config(config, 'COMMON', 'max_price_increase', '10'))
    min_profit = float(safe_get_config(config, 'COMMON', 'min_profit_margin', '4'))
    
    # EMA 계산 (15:10 데이터 기준)
    daily_df['EMA22_1510'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60_1510'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120_1510'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['EMA50'] = calculate_ema(daily_df['close'], 50)
    daily_df['MA22'] = daily_df['close'].rolling(window=22).mean()
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    # 최고값 기준 하락률 계산
    recent_high_period = int(safe_get_config(config, 'COMMON', 'recent_high_period', '30'))
    daily_df[f'recent_high_{recent_high_period}d'] = daily_df['high'].rolling(window=recent_high_period).max()
    max_decline_from_high_pct = float(safe_get_config(config, 'COMMON', 'max_decline_from_high_pct', '30.0'))
    
    min_data_needed = max(120, 50, cross_pattern + uptrend_length + 1, recent_high_period)
    candidates = list(range(min_data_needed, len(daily_df)))
    
    if not candidates:
        return {
            'total_candidates': 0,
            'condition1_pass': 0, 'condition2_pass': 0, 'condition3_pass': 0, 'condition4_pass': 0,
            'condition5_pass': 0, 'condition6_pass': 0, 'condition7_pass': 0, 'condition8_pass': 0,
            'final_signals': 0
        }
    
    # 조건 1: EMA22 반등 패턴
    condition1_pass = []
    for idx in candidates:
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        condition_1 = True
        for i in range(1, cross_pattern + 1):
            if idx - i < 0:
                condition_1 = False
                break
            past_row = daily_df.iloc[idx-i]
            if past_row['close_1510'] >= past_row['EMA22_1510']:
                condition_1 = False
                break
        
        if condition_1:
            for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                if idx - i < 0:
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] <= past_row['EMA22_1510']:
                    condition_1 = False
                    break
        
        if condition_1:
            condition1_pass.append(idx)
    
    # 조건 2: EMA 순서
    condition2_pass = []
    for idx in condition1_pass:
        prev_row = daily_df.iloc[idx-1]
        if prev_row['EMA22_1510'] > prev_row['EMA60_1510'] > prev_row['EMA120_1510']:
            condition2_pass.append(idx)
    
    # 조건 3: 당일 EMA22 회복
    condition3_pass = []
    for idx in condition2_pass:
        current_row = daily_df.iloc[idx]
        if current_row['close_1510'] > current_row['EMA22_1510']:
            condition3_pass.append(idx)
    
    # 조건 4: MA22 돌파
    condition4_pass = []
    for idx in condition3_pass:
        current_row = daily_df.iloc[idx]
        if current_row['close_1510'] > current_row['MA22_1510']:
            condition4_pass.append(idx)
    
    # 조건 5: 고가 제한
    condition5_pass = []
    for idx in condition4_pass:
        current_row = daily_df.iloc[idx]
        if current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3):
            condition5_pass.append(idx)
    
    # 조건 6: 상승률 범위
    condition6_pass = []
    for idx in condition5_pass:
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
        if min_increase <= price_increase <= max_increase:
            condition6_pass.append(idx)
    
    # 조건 7: 수익 마진
    condition7_pass = []
    for idx in condition6_pass:
        current_row = daily_df.iloc[idx]
        
        try:
            buy_price = current_row['close_1510']
            
            absolute_stop_loss_pct = float(safe_get_config(config, 'COMMON', 'absolute_stop_loss_pct', '5.0'))
            ema50_stop_loss = current_row['EMA50']
            absolute_stop_loss = buy_price * (1 - absolute_stop_loss_pct / 100)
            
            # 🔧 올바른 손절 로직: EMA50이 매수가보다 낮을 때만 손절가로 사용
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나, 매수가보다 높으면(이익구간) 절대손절만 사용
                final_stop_loss = absolute_stop_loss
            else:
                # EMA50이 매수가보다 낮으면 두 손절가 중 더 높은(안전한) 값 사용
                final_stop_loss = max(ema50_stop_loss, absolute_stop_loss)
            
            # 🎯 익절가/손절가 통합 계산 (중심값 기준)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2  # 중심값
            
            if pd.isna(ema50_stop_loss) or ema50_stop_loss <= 0 or ema50_stop_loss >= buy_price:
                # EMA50이 없거나 매수가보다 높으면 절대손절 기준으로 계산
                risk_amount = buy_price * (absolute_stop_loss_pct / 100)
                take_profit = buy_price + (risk_amount * 1.5)
            else:
                # EMA50 기준 중심값으로 위험금액 계산
                risk_amount = candle_center - ema50_stop_loss
                take_profit = buy_price + (risk_amount * 1.5)
            
            # 🔧 최소 5% 강제 설정 제거 - 계산된 익절가 그대로 사용
            
            if take_profit >= buy_price * (1 + min_profit / 100):
                condition7_pass.append(idx)
                
        except Exception:
            continue
    
    # 조건 8: 최고값 대비 하락률 제한
    condition8_pass = []
    for idx in condition7_pass:
        current_row = daily_df.iloc[idx]
        buy_price = current_row['close_1510']
        
        recent_high_column = f'recent_high_{recent_high_period}d'
        if recent_high_column in current_row.index and not pd.isna(current_row[recent_high_column]):
            recent_high = current_row[recent_high_column]
            decline_from_high = ((recent_high - buy_price) / recent_high) * 100 if recent_high > 0 else 0
            if decline_from_high <= max_decline_from_high_pct:
                condition8_pass.append(idx)
        else:
            condition8_pass.append(idx)  # 데이터가 없으면 통과
    
    return {
        'total_candidates': len(candidates),
        'condition1_pass': len(condition1_pass),
        'condition2_pass': len(condition2_pass),
        'condition3_pass': len(condition3_pass),
        'condition4_pass': len(condition4_pass),
        'condition5_pass': len(condition5_pass),
        'condition6_pass': len(condition6_pass),
        'condition7_pass': len(condition7_pass),
        'condition8_pass': len(condition8_pass),
        'final_signals': len(condition8_pass)
    }

def execute_debug_test():
    """
    전체 폴더 조건별 통과 현황 디버깅 테스트
    """
    print("\n=== 🐛 디버깅 모드 (전체 파일 처리) ===")
    
    # 폴더 선택
    base_dir = Path('json_data')
    if not base_dir.exists():
        print("json_data 폴더가 존재하지 않습니다.")
        return
        
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        print("분석할 데이터 폴더가 없습니다.")
        return
        
    print("\n=== 사용 가능한 데이터 폴더 ===")
    for idx, folder in enumerate(folders, 1):
        print(f"{idx}. {folder.name}")
    
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
    
    # 모든 JSON 파일 처리
    json_files = list(selected_folder.glob('*.json'))
    if not json_files:
        print("선택한 폴더에 JSON 파일이 없습니다.")
        return
    
    print(f"\n🔍 전체 파일 처리: {len(json_files)}개 파일")
    print("="*60)
    
    # 통합 통계 초기화
    total_stats = {
        'processed_files': 0,
        'error_files': 0,
        'total_candidates': 0,
        'condition1_total': 0,
        'condition2_total': 0,
        'condition3_total': 0,
        'condition4_total': 0,
        'condition5_total': 0,
        'condition6_total': 0,
        'condition7_total': 0,
        'condition8_total': 0,
        'final_signals': 0
    }
    
    # 각 파일 처리
    for idx, json_file in enumerate(json_files, 1):
        try:
            print(f"📁 처리중... {idx}/{len(json_files)} {json_file.name[:15]}...", end=" ")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            code = data['meta'].get('code', json_file.name)
            chart_data = data['data']
            
            df = pd.DataFrame(chart_data)
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
            
            daily_df = convert_to_daily(df)
            if daily_df.empty:
                print("❌ 일봉변환 실패")
                total_stats['error_files'] += 1
                continue
                
            daily_df['code'] = code
            
            # 개별 파일의 조건별 통과 현황 수집 (출력 없이)
            file_stats = get_file_condition_stats(daily_df, condition_set=1)
            
            # 통합 통계에 누적
            total_stats['processed_files'] += 1
            total_stats['total_candidates'] += file_stats['total_candidates']
            total_stats['condition1_total'] += file_stats['condition1_pass']
            total_stats['condition2_total'] += file_stats['condition2_pass']
            total_stats['condition3_total'] += file_stats['condition3_pass']
            total_stats['condition4_total'] += file_stats['condition4_pass']
            total_stats['condition5_total'] += file_stats['condition5_pass']
            total_stats['condition6_total'] += file_stats['condition6_pass']
            total_stats['condition7_total'] += file_stats['condition7_pass']
            total_stats['condition8_total'] += file_stats['condition8_pass']
            total_stats['final_signals'] += file_stats['final_signals']
            
            print("✅ 완료")
            
        except Exception as e:
            print(f"❌ 오류: {repr(e)}")
            total_stats['error_files'] += 1
    
    # 통합 결과 출력
    print("\n" + "="*60)
    print("📊 **전체 통합 결과**")
    print("="*60)
    print(f"처리 완료: {total_stats['processed_files']}개 파일")
    print(f"처리 실패: {total_stats['error_files']}개 파일")
    print(f"전체 검사 대상: {total_stats['total_candidates']:,}개 봉")
    print()
    print("📈 **조건별 통과 현황 (전체 누적)**")
    
    if total_stats['total_candidates'] > 0:
        candidates = total_stats['total_candidates']
        print(f"   ✅ 조건1 (EMA22반등): {total_stats['condition1_total']:,}개 통과 ({total_stats['condition1_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건2 (EMA순서): {total_stats['condition2_total']:,}개 통과 ({total_stats['condition2_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건3 (EMA회복): {total_stats['condition3_total']:,}개 통과 ({total_stats['condition3_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건4 (MA22돌파): {total_stats['condition4_total']:,}개 통과 ({total_stats['condition4_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건5 (고가제한): {total_stats['condition5_total']:,}개 통과 ({total_stats['condition5_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건6 (상승률범위): {total_stats['condition6_total']:,}개 통과 ({total_stats['condition6_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건7 (수익마진): {total_stats['condition7_total']:,}개 통과 ({total_stats['condition7_total']/candidates*100:.1f}%)")
        print(f"   ✅ 조건8 (하락률제한): {total_stats['condition8_total']:,}개 통과 ({total_stats['condition8_total']/candidates*100:.1f}%)")
        print()
        print(f"🎯 **최종 결과: {total_stats['final_signals']:,}개 매수신호 ({total_stats['final_signals']/candidates*100:.1f}%)**")
    else:
        print("❌ 처리된 데이터가 없습니다.")
    
    print("="*60)

def main():
    """메인 함수"""
    while True:
        print("\n" + "="*50)
        print("  📊 통합 백테스팅 시스템 📊")
        print("="*50)
        print("1. 📈 EMA22 반등 조건 백테스팅")
        print("2. 🔧 파라미터 최적화 (전체)")
        print("3. 🎯 절대 손절값 최적화")
        print("4. 🐛 디버깅 모드 (조건별 통과 현황)")
        print("5. ❌ 종료")
        print("="*50)
        
        choice = input("원하는 작업을 선택하세요 (1-5): ")
        
        if choice == '1':
            execute_integrated_backtest()
        elif choice == '2':
            optimize_parameters()
        elif choice == '3':
            optimize_absolute_stop_loss()
        elif choice == '4':
            execute_debug_test()
        elif choice == '5':
            print("프로그램을 종료합니다.")
            break
        else:
            print("올바른 번호를 입력하세요 (1-5).")

if __name__ == "__main__":
    main() 