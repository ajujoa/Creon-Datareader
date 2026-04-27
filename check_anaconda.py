#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Anaconda 설치 및 PATH 설정 확인 스크립트
"""

import os
import sys
import subprocess
import platform

def check_anaconda_installation():
    """Anaconda 설치 확인"""
    print("=" * 60)
    print("Anaconda 설치 및 PATH 설정 확인")
    print("=" * 60)
    
    # Anaconda 설치 경로 확인
    anaconda_paths = [
        r"C:\ProgramData\Anaconda3",
        r"C:\Users\{}\Anaconda3".format(os.getenv("USERNAME")),
        r"C:\Program Files\Anaconda3",
        r"C:\Program Files (x86)\Anaconda3",
    ]
    
    found_anaconda = False
    anaconda_path = ""
    
    for path in anaconda_paths:
        if os.path.exists(path):
            found_anaconda = True
            anaconda_path = path
            print(f"[OK] Anaconda 발견: {path}")
            break
    
    if not found_anaconda:
        print("[ERROR] Anaconda를 찾을 수 없습니다.")
        print("설치된 경로를 확인하세요.")
        return False, ""
    
    return True, anaconda_path

def check_anaconda_files(anaconda_path):
    """Anaconda 실행 파일 확인"""
    print("\n[INFO] Anaconda 실행 파일 확인 중...")
    
    conda_exe = os.path.join(anaconda_path, "Scripts", "conda.exe")
    python_exe = os.path.join(anaconda_path, "python.exe")
    
    if os.path.exists(conda_exe):
        print(f"[OK] conda.exe 발견: {conda_exe}")
    else:
        print(f"[ERROR] conda.exe를 찾을 수 없습니다.")
    
    if os.path.exists(python_exe):
        print(f"[OK] python.exe 발견: {python_exe}")
        
        # Python 아키텍처 확인
        try:
            result = subprocess.run(
                [python_exe, "-c", "import struct; print(struct.calcsize('P') * 8)"],
                capture_output=True, text=True, check=True
            )
            arch = result.stdout.strip()
            print(f"[INFO] Anaconda Python 아키텍처: {arch}비트")
        except:
            print("[WARNING] Python 아키텍처 확인 실패")
    else:
        print(f"[ERROR] python.exe를 찾을 수 없습니다.")
    
    return conda_exe, python_exe

def check_path_settings():
    """PATH 설정 확인"""
    print("\n[INFO] PATH 설정 확인 중...")
    
    path = os.environ.get("PATH", "")
    path_entries = path.split(";")
    
    anaconda_in_path = False
    conda_in_path = False
    
    for entry in path_entries:
        if "Anaconda3" in entry or "anaconda3" in entry:
            anaconda_in_path = True
            print(f"[OK] PATH에 Anaconda 경로 있음: {entry}")
        if "conda" in entry.lower():
            conda_in_path = True
            print(f"[OK] PATH에 Conda 경로 있음: {entry}")
    
    if not anaconda_in_path:
        print("[WARNING] PATH에 Anaconda 경로가 없습니다.")
    
    if not conda_in_path:
        print("[WARNING] PATH에 Conda 경로가 없습니다.")
    
    return anaconda_in_path, conda_in_path

def test_conda_command(conda_exe):
    """Conda 명령어 테스트"""
    print("\n[INFO] Conda 명령어 테스트 중...")
    
    if not os.path.exists(conda_exe):
        print("[ERROR] conda.exe가 없습니다.")
        return False
    
    try:
        result = subprocess.run(
            [conda_exe, "--version"],
            capture_output=True, text=True, check=True
        )
        print(f"[OK] Conda 명령어 실행 성공: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Conda 명령어 실행 실패: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Conda 명령어 실행 오류: {e}")
        return False

def check_conda_environments(conda_exe):
    """Conda 환경 목록 확인"""
    print("\n[INFO] Conda 환경 목록 확인 중...")
    
    if not os.path.exists(conda_exe):
        return
    
    try:
        result = subprocess.run(
            [conda_exe, "env", "list"],
            capture_output=True, text=True, check=True
        )
        print("[OK] Conda 환경 목록:")
        print(result.stdout)
    except:
        print("[WARNING] Conda 환경 목록 확인 실패")

def create_setup_scripts(anaconda_path):
    """설정 스크립트 생성"""
    print("\n[INFO] 설정 스크립트 생성 중...")
    
    # PATH 추가 스크립트
    add_path_script = f'''@echo off
chcp 65001 >nul
REM Anaconda PATH 추가 스크립트

echo ========================================
echo Anaconda PATH 설정
echo ========================================

set ANACONDA_PATH={anaconda_path}
set CONDA_SCRIPTS=%ANACONDA_PATH%\\Scripts
set CONDA_BIN=%ANACONDA_PATH%\\Library\\bin

echo [INFO] Anaconda 경로: %ANACONDA_PATH%

REM PATH에 추가
set PATH=%CONDA_SCRIPTS%;%CONDA_BIN%;%PATH%

echo [INFO] PATH에 Anaconda 경로가 추가되었습니다.
echo.
echo 테스트:
echo 1. conda --version
echo 2. python --version
echo.
echo 주의: 이 스크립트는 현재 세션에만 적용됩니다.
echo 영구적으로 적용하려면 시스템 환경 변수를 설정하세요.
echo.

pause
'''
    
    with open("add_anaconda_to_path.bat", "w", encoding="utf-8") as f:
        f.write(add_path_script)
    print("[OK] PATH 추가 스크립트 생성 완료: add_anaconda_to_path.bat")
    
    # Anaconda 초기화 스크립트
    init_script = f'''@echo off
chcp 65001 >nul
REM Anaconda 초기화 스크립트

echo ========================================
echo Anaconda 초기화
echo ========================================

set ANACONDA_PATH={anaconda_path}

if not exist "%ANACONDA_PATH%" (
    echo [ERROR] Anaconda가 설치되지 않았습니다.
    echo 설치 경로: %ANACONDA_PATH%
    pause
    exit /b 1
)

echo [INFO] Anaconda 초기화 중...

REM Conda 초기화
call "%ANACONDA_PATH%\\Scripts\\conda.exe" init cmd.exe

if %errorlevel% equ 0 (
    echo [OK] Anaconda 초기화 완료
    echo.
    echo 다음 단계:
    echo 1. 새 명령 프롬프트를 열어주세요.
    echo 2. conda --version 명령으로 확인
    echo 3. conda activate base 로 기본 환경 활성화
) else (
    echo [ERROR] Anaconda 초기화 실패
)

echo.
pause
'''
    
    with open("init_anaconda.bat", "w", encoding="utf-8") as f:
        f.write(init_script)
    print("[OK] Anaconda 초기화 스크립트 생성 완료: init_anaconda.bat")

def main():
    """메인 함수"""
    # Anaconda 설치 확인
    found, anaconda_path = check_anaconda_installation()
    if not found:
        return 1
    
    # Anaconda 파일 확인
    conda_exe, python_exe = check_anaconda_files(anaconda_path)
    
    # PATH 설정 확인
    anaconda_in_path, conda_in_path = check_path_settings()
    
    # Conda 명령어 테스트
    conda_works = test_conda_command(conda_exe)
    
    # Conda 환경 확인
    if conda_works:
        check_conda_environments(conda_exe)
    
    # 설정 스크립트 생성
    create_setup_scripts(anaconda_path)
    
    print("\n" + "=" * 60)
    print("확인 완료")
    print("=" * 60)
    
    print("\n생성된 스크립트:")
    print("  1. add_anaconda_to_path.bat - PATH 임시 추가")
    print("  2. init_anaconda.bat - Anaconda 초기화")
    
    print("\n현재 상태:")
    if anaconda_in_path and conda_in_path and conda_works:
        print("  [OK] Anaconda가 올바르게 설정되었습니다.")
        print("  다음 명령어를 테스트하세요:")
        print("    conda --version")
        print("    conda env list")
    else:
        print("  [WARNING] Anaconda 설정에 문제가 있습니다.")
        
        if not anaconda_in_path:
            print("  - PATH에 Anaconda 경로가 없습니다.")
        
        if not conda_in_path:
            print("  - PATH에 Conda 경로가 없습니다.")
        
        if not conda_works:
            print("  - Conda 명령어가 작동하지 않습니다.")
        
        print("\n  해결 방법:")
        print("  1. init_anaconda.bat 실행 (관리자 권한 권장)")
        print("  2. 새 명령 프롬프트 열기")
        print("  3. conda --version 명령 테스트")
    
    print("\nCreon DataReader를 위한 환경 생성:")
    print("  conda create -n creon_datareader python=3.8")
    print("  conda activate creon_datareader")
    print("  pip install pywin32 pandas numpy pyyaml sqlalchemy")
    
    print("\n" + "=" * 60)
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n[INFO] 작업이 취소되었습니다.")
        sys.exit(1)