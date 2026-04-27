#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.8.10 32비트 설치 프로그램 다운로드
"""

import urllib.request
import os
import sys

def download_file(url, filename):
    """파일 다운로드"""
    print(f"[INFO] 다운로드 중: {url}")
    print(f"       저장 위치: {filename}")
    
    try:
        # 진행 표시기
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = downloaded / total_size * 100
            sys.stdout.write(f"\r  진행률: {percent:.1f}% ({downloaded:,} / {total_size:,} bytes)")
            sys.stdout.flush()
        
        urllib.request.urlretrieve(url, filename, report_progress)
        print(f"\n[OK] 다운로드 완료: {filename}")
        
        # 파일 크기 확인
        file_size = os.path.getsize(filename)
        print(f"  파일 크기: {file_size:,} bytes")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] 다운로드 실패: {e}")
        return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("Python 3.8.10 32비트 설치 프로그램 다운로드")
    print("=" * 60)
    
    url = "https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe"
    filename = "python-3.8.10-32bit.exe"
    
    # 이미 파일이 있는지 확인
    if os.path.exists(filename):
        file_size = os.path.getsize(filename)
        print(f"[INFO] 파일이 이미 존재합니다: {filename}")
        print(f"  파일 크기: {file_size:,} bytes")
        
        choice = input("재다운로드하시겠습니까? (y/n): ").lower()
        if choice != 'y':
            print("[INFO] 기존 파일을 사용합니다.")
            return True
    
    # 다운로드
    success = download_file(url, filename)
    
    if success:
        print("\n" + "=" * 60)
        print("다운로드 완료!")
        print("=" * 60)
        print("\n다음 단계:")
        print("1. 설치 프로그램 실행: python-3.8.10-32bit.exe")
        print("2. 설치 시 주의사항:")
        print("   - 설치 경로: C:\\Python38-32 (권장)")
        print("   - 'Add Python to PATH' 체크")
        print("   - 'Install for all users' 선택")
        print("3. 설치 완료 후 setup_32bit_environment.py 다시 실행")
        print("\n" + "=" * 60)
        return True
    else:
        print("\n[ERROR] 다운로드에 실패했습니다.")
        print("수동으로 다운로드하세요:")
        print(f"  URL: {url}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[INFO] 다운로드가 취소되었습니다.")
        sys.exit(1)