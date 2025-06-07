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
### TA 패키지를 이용한 볼린져 밴드 계산 함수
import ta
from pykrx import stock
from exchange_calendars import get_calendar
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt

# 전역 변수
MAX_WORKERS = 10
CHUNK_SIZE = 20

def load_config():
    """
    설정 파일에서 조건을 로드하는 함수
    """
    config = configparser.ConfigParser()
    config_file = Path('backtest_config.ini')
    
    # 기본 설정 파일이 없으면 생성
    if not config_file.exists():
        create_default_config()
    
    config.read(config_file, encoding='utf-8')
    return config

def get_config_value(config, section, key, value_type=str):
    """
    🔧 설정값을 읽고 주석을 제거하는 함수
    """
    raw_value = config[section][key]
    
    # 주석 제거 (# 이후 부분 제거)
    if '#' in raw_value:
        clean_value = raw_value.split('#')[0].strip()
    else:
        clean_value = raw_value.strip()
    
    # 타입 변환
    if value_type == int:
        return int(clean_value)
    elif value_type == float:
        return float(clean_value)
    else:
        return clean_value

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
        'max_hold_days': '5',
        'tax_rate': '0.0023',
        'min_price_increase': '2',
        'max_price_increase': '10.0',
        'min_profit_margin': '4.0'
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

def calculate_ma50(daily_df):
    """50일 이동평균 계산"""
    daily_df['MA50'] = daily_df['close'].rolling(window=50).mean()
    return daily_df

def check_conditions(daily_df, condition_set=1):
    """
    새로운 EMA22 반등 패턴으로 검색하는 함수
    """
    config = load_config()
    condition_key = f'CONDITION_{condition_set}'
    
    if condition_key not in config:
        print(f"조건 세트 {condition_set}를 찾을 수 없습니다.")
        return []
    
    condition = config[condition_key]
    common = config['COMMON']
    
    # 설정에서 값 읽기
    cross_pattern = get_config_value(config, condition_key, 'cross_pattern', int)  # EMA22 아래 있는 기간
    uptrend_length = get_config_value(config, condition_key, 'uptrend_candle_length', int)  # EMA22 위에 있어야 하는 기간
    min_increase = get_config_value(config, 'COMMON', 'min_price_increase', float)
    max_increase = get_config_value(config, 'COMMON', 'max_price_increase', float)
    min_profit = get_config_value(config, 'COMMON', 'min_profit_margin', float)
    
    # EMA 계산 (15:10 데이터 기준)
    daily_df['EMA22'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['MA22'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['MA50'] = daily_df['close'].rolling(window=50).mean()  # 50일선 추가
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    valid_dates = []
    
    # 최소 필요한 과거 데이터: 120일 + cross_pattern + uptrend_length
    min_data_needed = 120 + cross_pattern + uptrend_length
    
    print(f"\n=== 조건 {condition_set} 검색 시작 ===")
    print(f"cross_pattern: {cross_pattern}, uptrend_length: {uptrend_length}")
    print(f"상승률 범위: {min_increase}% ~ {max_increase}%")
    print(f"최소 데이터 필요량: {min_data_needed}일")
    
    condition_check_counts = {
        'total_checked': 0,
        'pattern_ok': 0,
        'ema_order_ok': 0,
        'ema22_recovery_ok': 0,
        'ma22_break_ok': 0,
        'high_limit_ok': 0,
        'price_range_ok': 0,
        'profit_margin_ok': 0,
        'all_conditions_ok': 0
    }
    
    for idx in range(min_data_needed, len(daily_df)):
        current_row = daily_df.iloc[idx]
        prev_row = daily_df.iloc[idx-1]
        
        condition_check_counts['total_checked'] += 1
        
        try:
            # 조건 1: EMA22 회복 패턴 확인
            condition_1 = True
            
            # 1-N봉전: EMA22 아래 (cross_pattern 기간)
            for i in range(1, cross_pattern + 1):
                if idx - i < 0:
                    condition_1 = False
                    break
                past_row = daily_df.iloc[idx-i]
                if past_row['close_1510'] >= past_row['EMA22']:
                    condition_1 = False
                    break
            
            # (cross_pattern+1)봉전부터 (cross_pattern+uptrend_length)봉전: EMA22 위
            if condition_1:
                for i in range(cross_pattern + 1, cross_pattern + uptrend_length + 1):
                    if idx - i < 0:
                        condition_1 = False
                        break
                    past_row = daily_df.iloc[idx-i]
                    if past_row['close_1510'] <= past_row['EMA22']:
                        condition_1 = False
                        break
            
            if condition_1:
                condition_check_counts['pattern_ok'] += 1
            
            # 조건 2: 1봉전 EMA22 > EMA60 > EMA120
            condition_2 = (
                prev_row['EMA22'] > prev_row['EMA60'] > prev_row['EMA120']
            )
            
            if condition_1 and condition_2:
                condition_check_counts['ema_order_ok'] += 1
            
            # 조건 3: 당일 EMA22 회복 (필수 - 설정 불가)
            condition_3 = (
                current_row['close_1510'] > current_row['EMA22']
            )
            
            if condition_1 and condition_2 and condition_3:
                condition_check_counts['ema22_recovery_ok'] += 1
            
            # 조건 4: 기준일 close_1510 > MA22_1510 (돌파)
            condition_4 = (
                current_row['close_1510'] > current_row['MA22_1510']
            )
            
            if condition_1 and condition_2 and condition_3 and condition_4:
                condition_check_counts['ma22_break_ok'] += 1
            
            # 조건 5: 기준일 최근10봉최고가 < (close_1510 * 1.3)
            condition_5 = (
                current_row['recent_high_1510'] < (current_row['close_1510'] * 1.3)
            )
            
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5:
                condition_check_counts['high_limit_ok'] += 1
            
            # 조건 6: 기준일 상승률 (공통 설정 기반)
            price_increase = (current_row['close_1510'] / prev_row['close'] - 1) * 100
            condition_6 = min_increase <= price_increase <= max_increase
            
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6:
                condition_check_counts['price_range_ok'] += 1
            
            # 조건 7: 익절가가 매수가보다 설정% 이상 높음 (공통 설정 기반)
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2
            buy_price = current_row['close_1510']
            stop_loss = current_row['MA50']
            
            if pd.isna(stop_loss):
                continue
                
            if candle_center > stop_loss:
                take_profit = candle_center + ((candle_center - stop_loss) * 1.5)
            else:
                take_profit = buy_price * 1.05
                
            if take_profit <= buy_price:
                take_profit = buy_price * 1.05
                
            condition_7 = take_profit >= buy_price * (1 + min_profit / 100)
            
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6 and condition_7:
                condition_check_counts['profit_margin_ok'] += 1
            
            # 모든 조건 만족 시 해당 날짜 추가
            if (condition_1 and condition_2 and condition_3 and 
                condition_4 and condition_5 and condition_6 and condition_7):
                valid_dates.append(current_row['date'].strftime('%Y-%m-%d'))
                condition_check_counts['all_conditions_ok'] += 1
                
        except Exception as e:
            print(f"조건 검사 중 오류 (idx={idx}): {str(e)}")
            continue
    
    # 디버깅 정보 출력
    print(f"\n=== 조건 {condition_set} 검사 결과 ===")
    print(f"총 검사한 날짜: {condition_check_counts['total_checked']}")
    print(f"패턴 조건 통과: {condition_check_counts['pattern_ok']}")
    print(f"EMA 정렬 통과: {condition_check_counts['ema_order_ok']}")
    print(f"EMA22 회복 통과: {condition_check_counts['ema22_recovery_ok']}")
    print(f"MA22 돌파 통과: {condition_check_counts['ma22_break_ok']}")
    print(f"고가 제한 통과: {condition_check_counts['high_limit_ok']}")
    print(f"상승률 범위 통과: {condition_check_counts['price_range_ok']}")
    print(f"수익률 마진 통과: {condition_check_counts['profit_margin_ok']}")
    print(f"최종 조건 만족: {condition_check_counts['all_conditions_ok']}")
    
    return valid_dates

def calculate_exit_prices(buy_row):
    """매수일 기준으로 손절가와 익절가 계산"""
    candle_center = (buy_row['open_1510'] + buy_row['close_1510']) / 2
    buy_price = buy_row['close_1510']
    stop_loss = buy_row['MA50']
    
    if candle_center > stop_loss:
        take_profit = candle_center + ((candle_center - stop_loss) * 1.5)
    else:
        take_profit = buy_price * 1.05
        
    if take_profit <= buy_price:
        take_profit = buy_price * 1.05
    
    return stop_loss, take_profit

def check_exit_conditions_minute_data(minute_df, buy_date, stop_loss, take_profit, max_hold_days=5):
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
    daily_df = calculate_ma50(daily_df)
    results = []
    
    for date_str in valid_dates:
        try:
            buy_date = pd.to_datetime(date_str)
            buy_day_data = daily_df[daily_df['date'].dt.date == buy_date.date()]
            if buy_day_data.empty:
                continue
                
            buy_row = buy_day_data.iloc[0]
            
            if pd.isna(buy_row['MA50']):
                continue
            
            buy_price = buy_row['close_1510']
            stop_loss, take_profit = calculate_exit_prices(buy_row)
            
            sell_date, sell_price, sell_reason = check_exit_conditions_minute_data(
                minute_df, buy_date, stop_loss, take_profit
            )
            
            if sell_date is not None and sell_price is not None:
                hold_days = (sell_date - buy_date).days
                
                result = {
                    '종목번호': buy_row.get('code', 'Unknown'),
                    '매수일': buy_date.strftime('%Y-%m-%d'),
                    '매수값': round(buy_price, 0),
                    '익절가(목표)': round(take_profit, 0),
                    '손절가(목표)': round(stop_loss, 0),
                    '매도일': sell_date.strftime('%Y-%m-%d'),
                    '매도값': round(sell_price, 0),
                    '보유기간': hold_days,
                    '손절익절': sell_reason,
                    '수익률': round(((sell_price / buy_price) - 1) * 100, 2)
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
            'error': f'파일 처리 중 오류: {str(e)}',
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
    condition_name = config[condition_key]['name']
    
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
        condition_name = config[condition_key]['name']
        
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
            for condition_name, data in all_condition_data.items():
                data['trades'].to_excel(writer, sheet_name=f'{condition_name}_거래내역', index=False)
                data['summary'].to_excel(writer, sheet_name=f'{condition_name}_요약', index=False)
            
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
            print(f"파일 {temp_file.name} 삭제 중 오류: {e}")
    
    print(f"임시 파일 {deleted_count}개가 정리되었습니다.")

def run_integrated_backtest():
    """통합 백테스팅 실행 함수"""
    global MAX_WORKERS, CHUNK_SIZE
    
    # 설정 로드
    config = load_config()
    MAX_WORKERS = get_config_value(config, 'COMMON', 'max_workers', int)
    CHUNK_SIZE = get_config_value(config, 'COMMON', 'chunk_size', int)
    
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
        condition = config[f'CONDITION_{i}']
        print(f"  - {condition['name']}: {condition['description']}")
    
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
        condition_name = config[f'CONDITION_{i}']['name']
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
            cleanup_temp_files(selected_folder)
            print("📁 임시 파일들이 정리되었습니다.")
    else:
        print("생성할 결과가 없습니다.")

def optimize_parameters():
    """
    파라미터 최적화 실행 함수
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
        'min_profit_margin': [2, 3, 4, 5]
    }
    
    print(f"\n=== 최적화 파라미터 범위 ===")
    for param, values in optimization_params.items():
        print(f"{param}: {values}")
    
    # 모든 조합 계산
    import itertools
    param_combinations = list(itertools.product(*optimization_params.values()))
    total_combinations = len(param_combinations)
    
    print(f"\n총 {total_combinations}개 조합을 테스트합니다.")
    
    # 최적화 시작 확인
    confirm = input("최적화를 시작하시겠습니까? (y/n): ")
    if confirm.lower() != 'y':
        return
    
    # 최적화 실행
    optimization_results = []
    
    print(f"\n=== 최적화 진행 중 ===")
    
    for idx, combination in enumerate(param_combinations, 1):
        cross_pattern, uptrend_length, min_increase, max_increase, min_profit = combination
        
        print(f"\n진행률: {idx}/{total_combinations} ({idx/total_combinations*100:.1f}%)")
        print(f"테스트 중: cross_pattern={cross_pattern}, uptrend_length={uptrend_length}, "
              f"price_range={min_increase}-{max_increase}%, profit_margin={min_profit}%")
        
        # 임시 설정으로 백테스팅 실행
        result = test_parameter_combination(
            json_files, selected_folder, 
            cross_pattern, uptrend_length, min_increase, max_increase, min_profit
        )
        
        if result:
            result['combination_id'] = idx
            result['parameters'] = {
                'cross_pattern': cross_pattern,
                'uptrend_candle_length': uptrend_length,
                'min_price_increase': min_increase,
                'max_price_increase': max_increase,
                'min_profit_margin': min_profit
            }
            optimization_results.append(result)
            
            # 중간 결과 출력
            if result['total_trades'] > 0:
                print(f"  결과: {result['total_trades']}건 거래, "
                      f"승률 {result['win_rate']:.1f}%, "
                      f"평균수익률 {result['avg_return']:.2f}%")
            else:
                print("  결과: 거래 없음")
    
    # 최적화 결과 분석 및 저장
    if optimization_results:
        save_optimization_results(optimization_results, selected_folder)
        analyze_optimization_results(optimization_results)
    else:
        print("최적화 결과가 없습니다.")

def test_parameter_combination(json_files, selected_folder, cross_pattern, uptrend_length, 
                             min_increase, max_increase, min_profit):
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
                                                min_increase, max_increase, min_profit)
            
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

def check_conditions_silent(daily_df, cross_pattern, uptrend_length, min_increase, max_increase, min_profit):
    """
    디버그 출력 없이 조건 검색하는 함수 (최적화용)
    """
    # EMA 계산 (15:10 데이터 기준)
    daily_df['EMA22'] = calculate_ema(daily_df['close_1510'], 22)
    daily_df['EMA60'] = calculate_ema(daily_df['close_1510'], 60)
    daily_df['EMA120'] = calculate_ema(daily_df['close_1510'], 120)
    daily_df['MA22_1510'] = daily_df['close_1510'].rolling(window=22).mean()
    daily_df['MA50'] = daily_df['close'].rolling(window=50).mean()
    daily_df['recent_high_1510'] = daily_df['close_1510'].rolling(window=10).max()
    
    valid_dates = []
    min_data_needed = 120 + cross_pattern + uptrend_length
    
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
            candle_center = (current_row['open_1510'] + current_row['close_1510']) / 2
            buy_price = current_row['close_1510']
            stop_loss = current_row['MA50']
            
            if pd.isna(stop_loss):
                continue
                
            if candle_center > stop_loss:
                take_profit = candle_center + ((candle_center - stop_loss) * 1.5)
            else:
                take_profit = buy_price * 1.05
                
            if take_profit <= buy_price:
                take_profit = buy_price * 1.05
                
            if not (take_profit >= buy_price * (1 + min_profit / 100)):
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
        
        # 파일 저장
        with open('backtest_config.ini', 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        
        print("✅ 추천 설정이 backtest_config.ini에 적용되었습니다!")
        print("이제 메뉴 1번으로 전체 백테스팅을 실행해보세요.")
        
    except Exception as e:
        print(f"❌ 설정 적용 중 오류: {e}")

def main():
    """메인 함수"""
    while True:
        print("\n" + "="*50)
        print("  📊 통합 백테스팅 시스템 📊")
        print("="*50)
        print("1. 📈 EMA22 반등 조건 백테스팅")
        print("2. 🔧 파라미터 최적화")
        print("3. ❌ 종료")
        print("="*50)
        
        choice = input("원하는 작업을 선택하세요 (1-3): ")
        
        if choice == '1':
            run_integrated_backtest()
        elif choice == '2':
            optimize_parameters()
        elif choice == '3':
            print("프로그램을 종료합니다.")
            break
        else:
            print("올바른 번호를 입력하세요 (1-3).")

if __name__ == "__main__":
    main() 