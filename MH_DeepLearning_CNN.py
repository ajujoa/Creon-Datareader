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
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import matplotlib.pyplot as plt
from tqdm import tqdm

# 기존 백테스팅 모듈에서 필요한 함수들 임포트
from MH_BackTest_Integrated_04 import (
    load_config, prepare_chart_data, calculate_all_indicators,
    check_conditions_vectorized, calculate_exit_prices_improved,
    check_exit_conditions_unified
)

class CNNTradingSystem:
    """
    🧠 하이브리드 CNN 트레이딩 시스템
    - EMA22 패턴으로 매수 시점 후보 감지
    - CNN 모델로 최종 매수/매도 판단
    """
    
    def __init__(self, window_size=30):
        self.window_size = window_size  # 30개 캔들 윈도우
        self.scaler = MinMaxScaler()
        self.model = None
        self.feature_columns = [
            'open', 'high', 'low', 'close', 'volume',
            'EMA22', 'EMA50', 'EMA120', 'MA22', 'MA50',
            'RSI', 'MACD', 'volume_ratio', 'linear_slope', 'linear_r2'
        ]
        
    def calculate_linear_features(self, df):
        """
        🔢 Linear 값 계산 (선형 회귀 기울기 및 R²)
        """
        df = df.copy()
        
        # 롤링 윈도우로 선형 회귀 계산
        window = 10  # 10일간 선형 회귀
        
        linear_slopes = []
        linear_r2s = []
        
        for i in range(len(df)):
            if i < window:
                linear_slopes.append(0)
                linear_r2s.append(0)
                continue
                
            # 과거 window일간의 종가 데이터
            y = df['close'].iloc[i-window:i].values
            x = np.arange(len(y)).reshape(-1, 1)
            
            if len(y) < 2:
                linear_slopes.append(0)
                linear_r2s.append(0)
                continue
                
            try:
                # 선형 회귀 모델 피팅
                reg = LinearRegression().fit(x, y)
                slope = reg.coef_[0]
                r2 = reg.score(x, y)
                
                linear_slopes.append(slope)
                linear_r2s.append(r2)
            except:
                linear_slopes.append(0)
                linear_r2s.append(0)
        
        df['linear_slope'] = linear_slopes
        df['linear_r2'] = linear_r2s
        
        return df
    
    def calculate_technical_indicators(self, df):
        """
        📊 추가 기술적 지표 계산
        """
        df = df.copy()
        
        # RSI 계산
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # MACD 계산
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        df['MACD'] = ema12 - ema26
        
        # 거래량 비율 (최근 평균 대비)
        df['volume_avg'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_avg']
        
        # NaN 값 처리
        df = df.fillna(0)
        
        return df
    
    def prepare_training_data(self, json_files, condition_set=1):
        """
        🎯 CNN 학습용 데이터 준비
        - 기존 백테스팅 결과에서 매수 시점 추출
        - 30개 캔들 윈도우 생성
        - 레이블링 (수익/손실)
        """
        print(f"\n=== CNN 학습 데이터 준비 (조건 {condition_set}) ===")
        
        all_features = []
        all_labels = []
        
        for file_path in tqdm(json_files, desc="데이터 처리 중"):
            try:
                # JSON 파일 로드
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'meta' not in data or 'data' not in data:
                    continue
                
                code = data['meta'].get('code', file_path.name)
                chart_data = data['data']
                
                if not chart_data:
                    continue
                
                # 차트 데이터 준비
                df = pd.DataFrame(chart_data)
                required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                
                if not all(col in df.columns for col in required_columns):
                    continue
                
                df = df[required_columns]
                chart_df = prepare_chart_data(df)
                
                if chart_df.empty or len(chart_df) < self.window_size + 120:
                    continue
                
                # 기술적 지표 계산
                chart_df = calculate_all_indicators(chart_df)
                chart_df = self.calculate_technical_indicators(chart_df)
                chart_df = self.calculate_linear_features(chart_df)
                
                # EMA22 패턴으로 매수 시점 후보 감지
                valid_dates = check_conditions_vectorized(chart_df, condition_set)
                
                if not valid_dates:
                    continue
                
                # 각 매수 시점에 대해 데이터 생성
                for date_str in valid_dates:
                    buy_date = pd.to_datetime(date_str)
                    
                    # 매수일 찾기
                    buy_candidates = chart_df[chart_df['date'].dt.date == buy_date.date()]
                    if buy_candidates.empty:
                        continue
                    
                    buy_idx = buy_candidates.index[-1]
                    
                    # 30개 캔들 윈도우 확인
                    if buy_idx < self.window_size:
                        continue
                    
                    # 특성 데이터 추출 (과거 30개 캔들)
                    start_idx = buy_idx - self.window_size
                    window_data = chart_df.iloc[start_idx:buy_idx][self.feature_columns].values
                    
                    if window_data.shape[0] != self.window_size:
                        continue
                    
                    # 레이블 생성 (실제 백테스팅 수행)
                    buy_row = chart_df.iloc[buy_idx]
                    stop_loss, take_profit = calculate_exit_prices_improved(buy_row)
                    
                    sell_datetime, sell_price, sell_reason = check_exit_conditions_unified(
                        chart_df, buy_date, stop_loss, take_profit
                    )
                    
                    if sell_datetime is not None and sell_price is not None:
                        buy_price = buy_row['close']
                        profit_rate = (sell_price / buy_price - 1) * 100
                        
                        # 레이블: 수익 나면 1 (매수), 손실 나면 0 (매수안함)
                        label = 1 if profit_rate > 0 else 0
                        
                        all_features.append(window_data)
                        all_labels.append(label)
                
                # 🔸 부정 샘플 추가 (매수하지 않은 시점들)
                # 랜덤하게 몇 개 시점을 선택해서 레이블 0으로 추가
                chart_len = len(chart_df)
                negative_samples = min(len(valid_dates) * 2, 10)  # 긍정 샘플의 2배 또는 최대 10개
                
                for _ in range(negative_samples):
                    random_idx = np.random.randint(self.window_size + 120, chart_len - 5)
                    random_date = chart_df.iloc[random_idx]['date']
                    
                    # 이미 매수 신호가 있었던 날은 제외
                    if random_date.strftime('%Y-%m-%d') in valid_dates:
                        continue
                    
                    # 30개 캔들 윈도우 데이터
                    start_idx = random_idx - self.window_size
                    window_data = chart_df.iloc[start_idx:random_idx][self.feature_columns].values
                    
                    if window_data.shape[0] != self.window_size:
                        continue
                    
                    all_features.append(window_data)
                    all_labels.append(0)  # 매수안함
                    
            except Exception as e:
                print(f"파일 처리 중 오류 ({file_path.name}): {str(e)}")
                continue
        
        if not all_features:
            print("❌ 학습 데이터가 생성되지 않았습니다.")
            return None, None
        
        # NumPy 배열로 변환
        X = np.array(all_features)
        y = np.array(all_labels)
        
        print(f"✅ 학습 데이터 생성 완료:")
        print(f"   - 총 샘플 수: {len(X)}")
        print(f"   - 입력 shape: {X.shape}")
        print(f"   - 매수 신호: {np.sum(y)} 개")
        print(f"   - 매수안함: {len(y) - np.sum(y)} 개")
        print(f"   - 긍정 비율: {np.mean(y)*100:.1f}%")
        
        return X, y
    
    def build_cnn_model(self, input_shape):
        """
        🏗️ CNN 모델 구축
        """
        model = keras.Sequential([
            # 1D CNN for time series
            layers.Conv1D(filters=64, kernel_size=3, activation='relu', input_shape=input_shape),
            layers.MaxPooling1D(pool_size=2),
            layers.Conv1D(filters=32, kernel_size=3, activation='relu'),
            layers.MaxPooling1D(pool_size=2),
            layers.Conv1D(filters=16, kernel_size=3, activation='relu'),
            
            # Global pooling and dense layers
            layers.GlobalAveragePooling1D(),
            layers.Dense(50, activation='relu'),
            layers.Dropout(0.3),
            layers.Dense(25, activation='relu'),
            layers.Dropout(0.2),
            layers.Dense(1, activation='sigmoid')  # 이진 분류
        ])
        
        model.compile(
            optimizer='adam',
            loss='binary_crossentropy',
            metrics=['accuracy', 'precision', 'recall']
        )
        
        return model
    
    def train_model(self, X, y, validation_split=0.2, epochs=50):
        """
        🎓 모델 학습
        """
        print(f"\n=== CNN 모델 학습 시작 ===")
        
        # 데이터 정규화
        X_scaled = self.scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape)
        
        # 모델 구축
        input_shape = (X.shape[1], X.shape[2])  # (30, features)
        self.model = self.build_cnn_model(input_shape)
        
        print(f"모델 구조:")
        self.model.summary()
        
        # 학습
        callbacks = [
            keras.callbacks.EarlyStopping(patience=10, restore_best_weights=True),
            keras.callbacks.ReduceLROnPlateau(factor=0.5, patience=5)
        ]
        
        history = self.model.fit(
            X_scaled, y,
            validation_split=validation_split,
            epochs=epochs,
            batch_size=32,
            callbacks=callbacks,
            verbose=1
        )
        
        print("✅ 모델 학습 완료")
        return history
    
    def predict_buy_signal(self, window_data):
        """
        🔮 매수 신호 예측
        """
        if self.model is None:
            raise ValueError("모델이 학습되지 않았습니다.")
        
        # 데이터 정규화
        window_scaled = self.scaler.transform(window_data.reshape(1, -1)).reshape(1, window_data.shape[0], window_data.shape[1])
        
        # 예측
        prediction = self.model.predict(window_scaled, verbose=0)[0][0]
        
        return prediction
    
    def hybrid_backtest(self, chart_df, condition_set=1, threshold=0.5):
        """
        🚀 하이브리드 백테스팅 (EMA22 + CNN)
        """
        print(f"\n=== 하이브리드 백테스팅 시작 ===")
        print(f"조건: {condition_set}, CNN 임계값: {threshold}")
        
        if self.model is None:
            raise ValueError("모델이 학습되지 않았습니다.")
        
        # 기술적 지표 계산
        chart_df = calculate_all_indicators(chart_df)
        chart_df = self.calculate_technical_indicators(chart_df)
        chart_df = self.calculate_linear_features(chart_df)
        
        # EMA22 패턴으로 매수 시점 후보 감지
        candidate_dates = check_conditions_vectorized(chart_df, condition_set)
        
        if not candidate_dates:
            print("❌ 매수 시점 후보가 없습니다.")
            return pd.DataFrame()
        
        print(f"📊 EMA22 패턴 매수 후보: {len(candidate_dates)}개")
        
        results = []
        cnn_approved = 0
        
        for date_str in candidate_dates:
            try:
                buy_date = pd.to_datetime(date_str)
                
                # 매수일 찾기
                buy_candidates = chart_df[chart_df['date'].dt.date == buy_date.date()]
                if buy_candidates.empty:
                    continue
                
                buy_idx = buy_candidates.index[-1]
                
                # 30개 캔들 윈도우 확인
                if buy_idx < self.window_size:
                    continue
                
                # CNN 입력 데이터 준비
                start_idx = buy_idx - self.window_size
                window_data = chart_df.iloc[start_idx:buy_idx][self.feature_columns].values
                
                if window_data.shape[0] != self.window_size:
                    continue
                
                # CNN 예측
                cnn_score = self.predict_buy_signal(window_data)
                
                # 임계값 이상일 때만 매수
                if cnn_score >= threshold:
                    cnn_approved += 1
                    
                    # 실제 백테스팅 수행
                    buy_row = chart_df.iloc[buy_idx]
                    buy_price = buy_row['close']
                    buy_datetime = buy_row['date']
                    
                    stop_loss, take_profit = calculate_exit_prices_improved(buy_row)
                    
                    sell_datetime, sell_price, sell_reason = check_exit_conditions_unified(
                        chart_df, buy_date, stop_loss, take_profit
                    )
                    
                    if sell_datetime is not None and sell_price is not None:
                        hold_days = (sell_datetime - buy_datetime).days
                        if hold_days == 0:
                            hold_hours = (sell_datetime - buy_datetime).total_seconds() / 3600
                            hold_display = f"당일({hold_hours:.1f}시간)"
                        else:
                            hold_display = f"{hold_days}일"
                        
                        result = {
                            '종목번호': buy_row.get('code', 'Unknown'),
                            '매수일시': buy_datetime.strftime('%Y-%m-%d %H:%M'),
                            '매수값': round(buy_price, 0),
                            'CNN점수': round(cnn_score * 100, 1),
                            '익절가(목표)': round(take_profit, 0),
                            '손절가(목표)': round(stop_loss, 0),
                            '매도일시': sell_datetime.strftime('%Y-%m-%d %H:%M'),
                            '매도값': round(sell_price, 0),
                            '보유기간': hold_display,
                            '손절익절': sell_reason,
                            '수익률': round(((sell_price / buy_price) - 1) * 100, 2)
                        }
                        results.append(result)
                        
            except Exception as e:
                continue
        
        print(f"🤖 CNN 승인: {cnn_approved}개 (전체 후보의 {cnn_approved/len(candidate_dates)*100:.1f}%)")
        print(f"✅ 최종 거래: {len(results)}개")
        
        return pd.DataFrame(results)
    
    def save_model(self, filepath):
        """모델 저장"""
        if self.model is not None:
            self.model.save(filepath)
            # 스케일러도 함께 저장
            import pickle
            with open(f"{filepath}_scaler.pkl", 'wb') as f:
                pickle.dump(self.scaler, f)
            print(f"✅ 모델 저장: {filepath}")
    
    def load_model(self, filepath):
        """모델 로드"""
        self.model = keras.models.load_model(filepath)
        # 스케일러도 함께 로드
        import pickle
        with open(f"{filepath}_scaler.pkl", 'rb') as f:
            self.scaler = pickle.load(f)
        print(f"✅ 모델 로드: {filepath}")

def main():
    """메인 함수"""
    print("\n" + "="*60)
    print("  🧠 CNN 하이브리드 트레이딩 시스템  🧠")
    print("="*60)
    print("1. 📚 CNN 모델 학습")
    print("2. 🚀 하이브리드 백테스팅 (EMA22 + CNN)")
    print("3. 💾 모델 저장/로드")
    print("4. ❌ 종료")
    print("="*60)
    
    cnn_system = CNNTradingSystem(window_size=30)
    
    while True:
        choice = input("\n원하는 작업을 선택하세요 (1-4): ")
        
        if choice == '1':
            # CNN 모델 학습
            base_dir = Path('json_data')
            if not base_dir.exists():
                print("json_data 폴더가 존재하지 않습니다.")
                continue
                
            folders = [f for f in base_dir.iterdir() if f.is_dir()]
            if not folders:
                print("분석할 데이터 폴더가 없습니다.")
                continue
            
            print("\n=== 사용 가능한 데이터 폴더 ===")
            for idx, folder in enumerate(folders, 1):
                print(f"{idx}. {folder.name}")
            
            try:
                folder_choice = int(input("\n학습할 폴더 번호를 선택하세요: "))
                if 1 <= folder_choice <= len(folders):
                    selected_folder = folders[folder_choice - 1]
                else:
                    print("올바른 번호를 입력하세요.")
                    continue
            except ValueError:
                print("올바른 숫자를 입력하세요.")
                continue
            
            # JSON 파일 로드
            json_files = list(selected_folder.glob('*.json'))
            if not json_files:
                print("선택한 폴더에 JSON 파일이 없습니다.")
                continue
            
            # 샘플 크기 선택
            print(f"\n총 {len(json_files)}개 파일이 있습니다.")
            sample_choice = input("전체 파일로 학습하시겠습니까? (y/n): ")
            if sample_choice.lower() != 'y':
                try:
                    sample_size = int(input("사용할 파일 개수를 입력하세요: "))
                    json_files = json_files[:sample_size]
                except ValueError:
                    print("올바른 숫자를 입력하세요.")
                    continue
            
            # 조건 선택
            try:
                condition_set = int(input("학습할 조건 (1, 2, 3): "))
                if condition_set not in [1, 2, 3]:
                    print("1, 2, 3 중에서 선택하세요.")
                    continue
            except ValueError:
                print("올바른 숫자를 입력하세요.")
                continue
            
            # 학습 데이터 준비
            X, y = cnn_system.prepare_training_data(json_files, condition_set)
            if X is None:
                continue
            
            # 모델 학습
            history = cnn_system.train_model(X, y)
            
        elif choice == '2':
            # 하이브리드 백테스팅
            if cnn_system.model is None:
                print("❌ 먼저 모델을 학습하거나 로드하세요.")
                continue
            
            # 임계값 설정
            try:
                threshold = float(input("CNN 판단 임계값 (0.0-1.0, 기본값 0.5): ") or "0.5")
                if not 0 <= threshold <= 1:
                    print("0과 1 사이의 값을 입력하세요.")
                    continue
            except ValueError:
                print("올바른 숫자를 입력하세요.")
                continue
            
            print("백테스팅 기능은 개별 파일 테스트용입니다.")
            print("전체 폴더 테스트는 별도 구현이 필요합니다.")
            
        elif choice == '3':
            # 모델 저장/로드
            action = input("저장(s) 또는 로드(l)? (s/l): ")
            if action.lower() == 's':
                if cnn_system.model is None:
                    print("❌ 저장할 모델이 없습니다.")
                    continue
                filepath = input("저장할 파일명 (예: cnn_model): ")
                cnn_system.save_model(f"models/{filepath}")
            elif action.lower() == 'l':
                filepath = input("로드할 파일명 (예: cnn_model): ")
                try:
                    cnn_system.load_model(f"models/{filepath}")
                except Exception as e:
                    print(f"❌ 모델 로드 실패: {e}")
            
        elif choice == '4':
            print("프로그램을 종료합니다.")
            break
        else:
            print("올바른 번호를 입력하세요 (1-4).")

if __name__ == "__main__":
    # models 폴더 생성
    Path("models").mkdir(exist_ok=True)
    main() 