#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.8.10 32비트 설치 스크립트
"""

import os
import sys
import subprocess
import urllib.request
import tempfile

def download_python_installer():
    """Python 설치 프로그램 다운로드"""
    url = "https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe"
    local_filename = "python-3.8.10-32bit.exe"
    
    print(f"[INFO] Python 3.8.10 32비트 다운로드 중...")
    print(f"  URL: {url}")
    
    try:
        urllib.request.urlretrieve(url, local_filename)
        print(f"[OK] 다운로드 완료: {local_filename}")
        return local_filename
    except Exception as e:
        print(f"[ERROR] 다운로드 실패: {e}")
        return None

def check_existing_installation():
    """기존 설치 확인"""
    install_path = r"C:\Python38-32"
    if os.path.exists(os.path.join(install_path, "python.exe")):
        print(f"[INFO] 기존 설치 발견: {install_path}")
        
        # 아키텍처 확인
        try:
            result = subprocess.run(
                [os.path.join(install_path, "python.exe"), "-c", "import struct; print(struct.calcsize('P')*8)"],
                capture_output=True, text=True, check=True
            )
            arch = int(result.stdout.strip())
            print(f"[INFO] 설치된 Python 아키텍처: {arch}비트")
            return install_path, arch == 32
        except:
            pass
    
    return None, False

def install_python(installer_path):
    """Python 설치"""
    install_path = r"C:\Python38-32"
    
    print(f"\n[INFO] Python 3.8.10 32비트 설치 중...")
    print(f"  설치 경로: {install_path}")
    print(f"  설치 프로그램: {installer_path}")
    
    # 설치 명령어
    # /quiet: 자동 설치
    # InstallAllUsers=1: 모든 사용자용
    # PrependPath=1: PATH에 추가
    # TargetDir: 설치 경로
    # Include_test=0: 테스트 제외
    
    cmd = [
        installer_path,
        "/quiet",
        "InstallAllUsers=1",
        "PrependPath=1",
        f'TargetDir="{install_path}"',
        "Include_test=0",
        "SimpleInstall=1"
    ]
    
    print(f"  설치 명령: {' '.join(cmd)}")
    print(f"  설치 중... (잠시만 기다려주세요)")
    
    try:
        # 관리자 권한으로 실행 필요
        # UAC 요청을 위해 shell=True 사용
        subprocess.run(cmd, shell=True, check=True)
        print("[OK] Python 설치 완료")
        return install_path
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 설치 실패: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] 설치 중 오류: {e}")
        return None

def verify_installation(install_path):
    """설치 확인"""
    python_exe = os.path.join(install_path, "python.exe")
    
    if not os.path.exists(python_exe):
        print(f"[ERROR] Python 실행 파일을 찾을 수 없습니다: {python_exe}")
        return False
    
    print(f"[OK] Python 실행 파일: {python_exe}")
    
    # 아키텍처 확인
    try:
        result = subprocess.run(
            [python_exe, "-c", "import struct; print(struct.calcsize('P')*8)"],
            capture_output=True, text=True, check=True
        )
        arch = int(result.stdout.strip())
        print(f"[INFO] Python 아키텍처: {arch}비트")
        
        if arch == 32:
            print("[OK] 32비트 Python 설치 확인 완료")
            return True
        else:
            print(f"[WARNING] 32비트 Python이 아닙니다: {arch}비트")
            return False
            
    except Exception as e:
        print(f"[ERROR] Python 실행 테스트 실패: {e}")
        return False

def create_setup_script():
    """32비트 환경 설정 스크립트 생성"""
    script_content = '''@echo off
chcp 65001 >nul
REM Creon DataReader 32비트 환경 설정

echo ========================================
echo Creon DataReader 32비트 환경 설정
echo ========================================

REM 32비트 Python 경로
set PYTHON32=C:\\Python38-32\\python.exe

if not exist "%PYTHON32%" (
    echo [ERROR] 32비트 Python을 찾을 수 없습니다: %PYTHON32%
    echo Python 3.8.10 32비트를 설치하세요.
    pause
    exit /b 1
)

echo [INFO] 32비트 Python 확인: %PYTHON32%

REM 아키텍처 확인
echo [INFO] Python 아키텍처 확인...
"%PYTHON32%" -c "import struct; arch=struct.calcsize('P')*8; print(f'아키텍처: {arch}비트')"

if %errorlevel% neq 0 (
    echo [ERROR] Python 실행 실패
    pause
    exit /b 1
)

REM 가상환경 생성
echo.
echo [INFO] 32비트 가상환경 생성 중...
set VENV_NAME=venv_creon_32bit

if exist %VENV_NAME% (
    echo [INFO] 기존 가상환경이 존재합니다: %VENV_NAME%
    set /p CHOICE=재생성하시겠습니까? (y/n): 
    if /i "%CHOICE%"=="y" (
        rmdir /s /q %VENV_NAME%
        echo [OK] 기존 가상환경 삭제 완료
    ) else (
        echo [INFO] 기존 가상환경을 사용합니다.
        goto ACTIVATE_VENV
    )
)

"%PYTHON32%" -m venv %VENV_NAME%
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 생성 실패
    pause
    exit /b 1
)

echo [OK] 가상환경 생성 완료: %VENV_NAME%

:ACTIVATE_VENV
echo.
echo [INFO] 가상환경 활성화 및 패키지 설치 중...

call %VENV_NAME%\\Scripts\\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 활성화 실패
    pause
    exit /b 1
)

echo [OK] 가상환경 활성화 완료

REM 필수 패키지 설치
echo [INFO] 필수 패키지 설치 중...

set PACKAGES=pywin32 pandas numpy pyyaml sqlalchemy pymysql psycopg2-binary

for %%p in (%PACKAGES%) do (
    echo   - %%p 설치 중...
    pip install %%p --quiet
    if !errorlevel! equ 0 (
        echo     [OK] %%p 설치 완료
    ) else (
        echo     [WARNING] %%p 설치 실패
    )
)

REM 프로젝트 의존성 설치
if exist requirements.txt (
    echo [INFO] requirements.txt 설치 중...
    pip install -r requirements.txt
    echo [OK] requirements.txt 설치 완료
) else (
    echo [INFO] requirements.txt 파일이 없습니다.
)

REM 테스트
echo.
echo [INFO] 환경 테스트 중...
python -c "import struct; print('가상환경 아키텍처:', struct.calcsize('P')*8, '비트')"

echo.
echo ========================================
echo 32비트 환경 설정 완료!
echo ========================================
echo.
echo 다음 단계:
echo 1. Creon Plus 프로그램 실행 및 로그인
echo 2. Creon API 테스트: python test_creon_api.py
echo 3. 데이터베이스 테스트: python test_database_simple.py
echo 4. Creon DataReader 실행: python creon_main.py -h
echo.
echo 환경 비활성화: deactivate
echo.

pause
'''
    
    script_file = "setup_creon_32bit.bat"
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(script_content)
    
    print(f"[OK] 32비트 환경 설정 스크립트 생성 완료: {script_file}")
    return script_file

def main():
    """메인 함수"""
    print("=" * 60)
    print("Python 3.8.10 32비트 설치")
    print("=" * 60)
    
    # 1. 기존 설치 확인
    print("\n[1/4] 기존 설치 확인 중...")
    existing_path, is_32bit = check_existing_installation()
    
    if existing_path and is_32bit:
        print("[OK] 32비트 Python이 이미 설치되어 있습니다.")
        print(f"  경로: {existing_path}")
        
        # 설정 스크립트 생성
        create_setup_script()
        
        print("\n" + "=" * 60)
        print("설치 완료!")
        print("=" * 60)
        print("\n다음 단계:")
        print("1. 32비트 환경 설정: setup_creon_32bit.bat 실행")
        print("2. Creon Plus 프로그램 실행")
        print("3. Creon API 테스트")
        return 0
    
    # 2. 설치 프로그램 다운로드
    print("\n[2/4] 설치 프로그램 다운로드 중...")
    installer_path = "python-3.8.10-32bit.exe"
    
    if not os.path.exists(installer_path):
        installer_path = download_python_installer()
        if not installer_path:
            print("[ERROR] 설치 프로그램을 다운로드할 수 없습니다.")
            return 1
    
    # 3. Python 설치
    print("\n[3/4] Python 설치 중...")
    print("[주의] 관리자 권한이 필요할 수 있습니다.")
    print("UAC 창이 나타나면 허용해주세요.")
    
    input("\n엔터 키를 눌러 설치를 시작하세요...")
    
    install_path = install_python(installer_path)
    if not install_path:
        print("[ERROR] Python 설치에 실패했습니다.")
        return 1
    
    # 4. 설치 확인
    print("\n[4/4] 설치 확인 중...")
    if not verify_installation(install_path):
        print("[WARNING] 설치 확인에 문제가 있습니다.")
    
    # 5. 설정 스크립트 생성
    create_setup_script()
    
    print("\n" + "=" * 60)
    print("설치 완료!")
    print("=" * 60)
    print("\n다음 단계:")
    print("1. 새 명령 프롬프트를 열어 PATH 업데이트 확인")
    print("2. 32비트 환경 설정: setup_creon_32bit.bat 실행")
    print("3. Creon Plus 프로그램 실행")
    print("4. Creon API 테스트")
    print("\n참고: 설치 후 시스템 재시작이 필요할 수 있습니다.")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n[INFO] 설치가 취소되었습니다.")
        sys.exit(1)