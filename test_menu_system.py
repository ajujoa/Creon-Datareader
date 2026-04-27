#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
메뉴 시스템 테스트 스크립트
"""

import os
import sys

def test_menu_system():
    """메뉴 시스템 테스트"""
    print("메뉴 시스템 테스트 시작")
    print("=" * 60)
    
    # 설정 파일 확인
    config_path = "./config/creon_config.ini"
    if os.path.exists(config_path):
        print(f"✓ 설정 파일 존재: {config_path}")
        
        # 설정 파일 내용 확인
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # 분봉 설정 확인
            if "minute_3_enabled" in content:
                print("✓ 3분봉 설정 확인")
            else:
                print("✗ 3분봉 설정 없음")
                
            if "minute_10_enabled" in content:
                print("✓ 10분봉 설정 확인")
            else:
                print("✗ 10분봉 설정 없음")
                
            if "minute_15_enabled" in content:
                print("✓ 15분봉 설정 확인")
            else:
                print("✗ 15분봉 설정 없음")
                
            # 최대 조회 개수 확인
            if "max_data_counts" in content:
                print("✓ 최대 조회 개수 설정 확인")
            else:
                print("✗ 최대 조회 개수 설정 없음")
                
            # 마켓 설정 확인
            if "market_types = ALL" in content:
                print("✓ 모든 마켓 선택 설정 확인")
            else:
                print("✗ 모든 마켓 선택 설정 없음")
    else:
        print(f"✗ 설정 파일 없음: {config_path}")
    
    print("\n" + "=" * 60)
    
    # 메인 메뉴 파일 확인
    menu_file = "./creon_main_menu.py"
    if os.path.exists(menu_file):
        print(f"✓ 메뉴 선택형 메인 파일 존재: {menu_file}")
        
        # 파일 내용 확인
        with open(menu_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # 메뉴 옵션 확인
            if "1분봉 데이터 수집" in content:
                print("✓ 1분봉 메뉴 옵션 확인")
            else:
                print("✗ 1분봉 메뉴 옵션 없음")
                
            if "3분봉 데이터 수집" in content:
                print("✓ 3분봉 메뉴 옵션 확인")
            else:
                print("✗ 3분봉 메뉴 옵션 없음")
                
            if "5분봉 데이터 수집" in content:
                print("✓ 5분봉 메뉴 옵션 확인")
            else:
                print("✗ 5분봉 메뉴 옵션 없음")
                
            if "10분봉 데이터 수집" in content:
                print("✓ 10분봉 메뉴 옵션 확인")
            else:
                print("✗ 10분봉 메뉴 옵션 없음")
                
            if "15분봉 데이터 수집" in content:
                print("✓ 15분봉 메뉴 옵션 확인")
            else:
                print("✗ 15분봉 메뉴 옵션 없음")
                
            if "일봉 데이터 수집" in content:
                print("✓ 일봉 메뉴 옵션 확인")
            else:
                print("✗ 일봉 메뉴 옵션 없음")
    else:
        print(f"✗ 메뉴 선택형 메인 파일 없음: {menu_file}")
    
    print("\n" + "=" * 60)
    
    # API 파일 확인
    api_file = "./creon_api.py"
    if os.path.exists(api_file):
        print(f"✓ API 파일 존재: {api_file}")
        
        # 분봉 간격 확인
        with open(api_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            if "분봉 간격 (1, 3, 5, 10, 15, 30, 60)" in content:
                print("✓ 새로운 분봉 간격 설정 확인")
            else:
                print("✗ 새로운 분봉 간격 설정 없음")
    else:
        print(f"✗ API 파일 없음: {api_file}")
    
    print("\n" + "=" * 60)
    
    # 분봉별 최대 조회 가능 개수 계산
    print("분봉별 최대 조회 가능 개수 (약치):")
    print("-" * 50)
    
    # 서버 보유 기간 (영업일)
    retention_days = {
        1: 500,   # 2년 (약 500영업일)
        3: 1250,  # 5년 (약 1,250영업일)
        5: 1250,  # 5년 (약 1,250영업일)
        10: 1250, # 5년 (약 1,250영업일)
        15: 1250, # 5년 (약 1,250영업일)
        30: 1250, # 5년 (약 1,250영업일)
        60: 1250  # 5년 (약 1,250영업일)
    }
    
    # 하루 생성 개수 (거래시간 09:00~15:30 = 6.5시간 = 390분)
    daily_counts = {
        1: 390,   # 390분 / 1분 = 390개
        3: 130,   # 390분 / 3분 = 130개
        5: 78,    # 390분 / 5분 = 78개
        10: 39,   # 390분 / 10분 = 39개
        15: 26,   # 390분 / 15분 = 26개
        30: 13,   # 390분 / 30분 = 13개
        60: 7     # 390분 / 60분 = 6.5개 (반올림)
    }
    
    print("구분\t서버 보유 기간\t하루 생성 개수\t최대 조회 가능 개수")
    print("-" * 70)
    
    for interval in [1, 3, 5, 10, 15, 30, 60]:
        days = retention_days.get(interval, 1250)
        daily = daily_counts.get(interval, 0)
        max_count = days * daily
        
        print(f"{interval}분봉\t{days}영업일\t\t{daily}개\t\t{max_count:,}개")
    
    print("\n" + "=" * 60)
    print("테스트 완료")
    print("=" * 60)
    
    # 실행 방법 안내
    print("\n실행 방법:")
    print("1. Creon Plus 프로그램 실행 및 로그인")
    print("2. 가상환경 활성화:")
    print("   venv_creon32\\Scripts\\activate")
    print("3. 메뉴 선택형 프로그램 실행:")
    print("   python creon_main_menu.py")
    print("\n주의: Creon API는 32비트 Python에서만 작동합니다.")


if __name__ == "__main__":
    test_menu_system()