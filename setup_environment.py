#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creon DataReader 환경 설정 Python 스크립트
"""

import os
import sys
import subprocess
import venv
import shutil
from pathlib import Path

def run_command(cmd, check=True, capture_output=False):
    """명령어 실행 헬퍼 함수"""
    print(f"  실행: {cmd}")
    try:
        if capture_output:
            result = subprocess.run(cmd, shell=True, check=check, 
                                  capture_output=True, text=True)
            return result.stdout.strip()
        else:
            subprocess.run(cmd, shell=True, check=check)
            return ""
    except subprocess.CalledProcessError as e:
        if check:
            print(f"  [ERROR] 명령 실행 실패: {e}")
            sys.exit(1)
        else:
            print(f"  [WARNING] 명령 실행 실패: {e}")
            return ""

def check_python_architecture():
    """Python 아키텍처 확인"""
    import struct
    arch = struct.calcsize('P') * 8
    print(f"Python 아키텍처: {arch}비트")
    return arch

def create_virtualenv(venv_name):
    """가상환경 생성"""
    print(f"\n[2/4] 가상환경 생성 중: {venv_name}")
    
    if os.path.exists(venv_name):
        print(f"  [INFO] 기존 가상환경이 존재합니다: {venv_name}")
        choice = input("  재생성하시겠습니까? (y/n): ").lower()
        if choice == 'y':
            shutil.rmtree(venv_name, ignore_errors=True)
            print("  [OK] 기존 가상환경 삭제 완료")
        else:
            print("  [INFO] 기존 가상환경을 사용합니다.")
            return False
    
    print(f"  가상환경 생성 중...")
    venv.create(venv_name, with_pip=True)
    print("  [OK] 가상환경 생성 완료")
    return True

def install_packages(venv_name):
    """필수 패키지 설치"""
    print(f"\n[3/4] 필수 패키지 설치 중...")
    
    # 가상환경 Python 경로
    if sys.platform == "win32":
        python_path = os.path.join(venv_name, "Scripts", "python.exe")
        pip_path = os.path.join(venv_name, "Scripts", "pip.exe")
    else:
        python_path = os.path.join(venv_name, "bin", "python")
        pip_path = os.path.join(venv_name, "bin", "pip")
    
    # 패키지 목록
    packages = [
        "pywin32",      # Windows API
        "pandas",       # 데이터 분석
        "numpy",        # 수치 계산
        "pyyaml",       # YAML 설정
        "sqlalchemy",   # 데이터베이스 ORM
        "pymysql",      # MySQL/MariaDB
        "psycopg2-binary", # PostgreSQL
    ]
    
    for package in packages:
        print(f"  - {package} 설치 중...")
        cmd = f'"{pip_path}" install {package} --quiet'
        run_command(cmd, check=False)
    
    # requirements.txt 설치
    if os.path.exists("requirements.txt"):
        print("  - requirements.txt 설치 중...")
        cmd = f'"{pip_path}" install -r requirements.txt'
        run_command(cmd, check=False)
    else:
        print("  [INFO] requirements.txt 파일이 없습니다.")
    
    print("  [OK] 패키지 설치 완료")
    return python_path

def test_environment(python_path):
    """환경 테스트"""
    print(f"\n[4/4] 환경 테스트 중...")
    
    test_script = """
import sys
import struct
import pandas as pd
import numpy as np
import yaml
import sqlalchemy

print("=" * 60)
print("Creon DataReader 환경 테스트")
print("=" * 60)

print(f"Python 버전: {sys.version}")
print(f"아키텍처: {struct.calcsize('P') * 8}비트")

print("\\n필수 패키지 버전:")
print(f"  pandas: {pd.__version__}")
print(f"  numpy: {np.__version__}")
print(f"  PyYAML: {yaml.__version__}")
print(f"  SQLAlchemy: {sqlalchemy.__version__}")

print("\\n" + "=" * 60)
print("테스트 완료")
print("=" * 60)
"""
    
    # 테스트 스크립트 실행
    test_file = "temp_test.py"
    with open(test_file, "w", encoding="utf-8") as f:
        f.write(test_script)
    
    cmd = f'"{python_path}" {test_file}'
    run_command(cmd, check=False)
    
    # 테스트 파일 삭제
    os.remove(test_file)

def create_activation_script(venv_name):
    """활성화 스크립트 생성"""
    print(f"\n활성화 스크립트 생성 중...")
    
    if sys.platform == "win32":
        script_content = f"""@echo off
REM Creon DataReader 가상환경 활성화 스크립트

echo ========================================
echo Creon DataReader 환경 활성화
echo ========================================

call {venv_name}\\Scripts\\activate.bat

if %errorlevel% equ 0 (
    echo [OK] 환경 활성화 완료
    echo.
    echo 사용 가능한 명령어:
    echo   python creon_main.py -h          도움말 보기
    echo   python creon_main.py collect     데이터 수집
    echo   python creon_main.py merge       데이터 병합
    echo   python test_database_simple.py   데이터베이스 테스트
    echo   python test_creon_api.py         Creon API 테스트
    echo.
    echo 환경 비활성화: deactivate
) else (
    echo [ERROR] 환경 활성화 실패
    echo 가상환경이 올바르게 생성되었는지 확인하세요.
)
"""
        script_file = "activate_creon.bat"
    else:
        script_content = f"""#!/bin/bash
# Creon DataReader 가상환경 활성화 스크립트

echo "========================================"
echo "Creon DataReader 환경 활성화"
echo "========================================"

source {venv_name}/bin/activate

if [ $? -eq 0 ]; then
    echo "[OK] 환경 활성화 완료"
    echo ""
    echo "사용 가능한 명령어:"
    echo "  python creon_main.py -h          도움말 보기"
    echo "  python creon_main.py collect     데이터 수집"
    echo "  python creon_main.py merge       데이터 병합"
    echo "  python test_database_simple.py   데이터베이스 테스트"
    echo "  python test_creon_api.py         Creon API 테스트"
    echo ""
    echo "환경 비활성화: deactivate"
else
    echo "[ERROR] 환경 활성화 실패"
    echo "가상환경이 올바르게 생성되었는지 확인하세요."
fi
"""
        script_file = "activate_creon.sh"
        # 실행 권한 추가
        os.chmod(script_file, 0o755)
    
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(script_content)
    
    print(f"  [OK] 활성화 스크립트 생성 완료: {script_file}")

def main():
    """메인 함수"""
    print("=" * 60)
    print("Creon DataReader 환경 설정")
    print("=" * 60)
    
    # 1. Python 아키텍처 확인
    print("\n[1/4] Python 아키텍처 확인 중...")
    arch = check_python_architecture()
    
    if arch != 32:
        print(f"\n[WARNING] 32비트 Python이 아닙니다!")
        print("Creon API는 32비트 Python이 필요합니다.")
        print("32비트 Python을 설치하거나 가상환경이 32비트를 사용하는지 확인하세요.")
    
    # 2. 가상환경 생성
    venv_name = "venv_creon"
    env_created = create_virtualenv(venv_name)
    
    # 3. 패키지 설치
    python_path = install_packages(venv_name)
    
    # 4. 환경 테스트
    test_environment(python_path)
    
    # 5. 활성화 스크립트 생성
    create_activation_script(venv_name)
    
    # 완료 메시지
    print("\n" + "=" * 60)
    print("환경 설정 완료!")
    print("=" * 60)
    print("\n다음 단계:")
    print("1. 환경 활성화:")
    if sys.platform == "win32":
        print("   activate_creon.bat")
    else:
        print("   source activate_creon.sh")
    print("2. Creon Plus 프로그램 실행 및 로그인")
    print("3. 데이터베이스 테스트: python test_database_simple.py")
    print("4. Creon API 테스트: python test_creon_api.py")
    print("5. Creon DataReader 실행: python creon_main.py -h")
    print("\n참고: Creon API 테스트를 위해 Creon Plus가 실행 중이어야 합니다.")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()