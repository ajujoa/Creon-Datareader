#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creon API 연결 테스트 스크립트
32비트 Python 환경에서 실행해야 합니다.
"""

import sys
import os

print("=" * 60)
print("Creon API 연결 테스트")
print("=" * 60)

# Python 아키텍처 확인
import struct
arch = struct.calcsize('P') * 8
print(f"Python 아키텍처: {arch}비트")

if arch != 32:
    print("\n[WARNING] 32비트 Python이 아닙니다!")
    print("Creon API는 32비트 Python이 필요합니다.")
    print("가상환경이 32비트 Python을 사용하는지 확인하세요.")
    print("=" * 60)
    sys.exit(1)

print("\n[INFO] 32비트 Python 확인 완료")

# pywin32 설치 확인
try:
    import win32com.client
    import pythoncom
    print("[OK] pywin32 설치됨")
except ImportError:
    print("[ERROR] pywin32가 설치되지 않았습니다.")
    print("설치 명령: pip install pywin32")
    print("=" * 60)
    sys.exit(1)

# Creon API 연결 테스트
print("\n[INFO] Creon API 연결 테스트 중...")

try:
    # Cybos Plus 연결 상태 확인
    cp = win32com.client.Dispatch("CpUtil.CpCybos")
    
    # 연결 상태
    is_connected = cp.IsConnect
    if is_connected == 1:
        print("[OK] Creon Plus에 연결되었습니다.")
        
        # 버전 정보
        version = cp.GetVersion()
        print(f"  버전: {version}")
        
        # 제한 정보
        limit_remain = cp.GetLimitRemainCount(1)  # LIMIT_REMAIN
        limit_request = cp.GetLimitRemainCount(0)  # LIMIT_REQUEST
        
        print(f"  남은 요청 횟수: {limit_remain}")
        print(f"  시간당 요청 제한: {limit_request}")
        
    else:
        print("[ERROR] Creon Plus에 연결되지 않았습니다.")
        print("  다음을 확인하세요:")
        print("  1. Creon Plus 프로그램이 실행 중인가?")
        print("  2. 로그인이 되어 있는가?")
        print("  3. 방화벽 설정을 확인하세요.")
        
except Exception as e:
    print(f"[ERROR] Creon API 연결 실패: {e}")
    print("  오류 타입:", type(e).__name__)
    
    # 일반적인 오류 원인
    if "CoCreateInstance" in str(e):
        print("  가능한 원인:")
        print("  1. Creon Plus가 설치되지 않았음")
        print("  2. COM 객체 등록 문제")
        print("  3. 32비트/64비트 불일치")
    elif "클래스를 등록하지 못했습니다" in str(e):
        print("  가능한 원인:")
        print("  1. Creon Plus 재설치 필요")
        print("  2. 관리자 권한으로 등록 필요")

# 추가 테스트: 기본적인 Creon 컴포넌트 확인
print("\n[INFO] Creon 컴포넌트 확인 중...")

components = [
    "CpUtil.CpCodeMgr",      # 종목 코드 관리
    "CpUtil.CpMarketWatch",  # 시장 감시
    "Dscbo1.StockMst",       # 주식 마스터
    "Dscbo1.StockChart",     # 주식 차트
]

for comp in components:
    try:
        obj = win32com.client.Dispatch(comp)
        print(f"  [OK] {comp}: 사용 가능")
        del obj
    except Exception as e:
        print(f"  [WARNING] {comp}: 사용 불가 - {str(e)[:50]}...")

# 샘플 데이터 조회 테스트 (연결된 경우)
try:
    cp = win32com.client.Dispatch("CpUtil.CpCybos")
    if cp.IsConnect == 1:
        print("\n[INFO] 샘플 데이터 조회 테스트...")
        
        # 종목 코드 관리자
        code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
        
        # KOSPI 시장 코드 개수
        kospi_count = code_mgr.GetStockListCount(1)  # 1: KOSPI
        kosdaq_count = code_mgr.GetStockListCount(2)  # 2: KOSDAQ
        
        print(f"  KOSPI 종목 수: {kospi_count}")
        print(f"  KOSDAQ 종목 수: {kosdaq_count}")
        
        # 삼성전자 코드 확인
        samsung_code = "A005930"
        samsung_name = code_mgr.CodeToName(samsung_code)
        print(f"  삼성전자: {samsung_code} - {samsung_name}")
        
except Exception as e:
    print(f"  [INFO] 샘플 데이터 조회 실패: {e}")

print("\n" + "=" * 60)
print("테스트 완료")
print("=" * 60)

print("\n다음 단계:")
print("1. Creon Plus가 정상적으로 연결되었는지 확인")
print("2. 데이터베이스 테스트: python test_database_simple.py")
print("3. Creon DataReader 실행: python creon_main.py -h")

if arch == 32:
    print("\n[SUCCESS] 32비트 Python 환경 설정 완료!")
else:
    print("\n[WARNING] 32비트 Python 환경이 아닙니다. Creon API 사용에 제한이 있을 수 있습니다.")