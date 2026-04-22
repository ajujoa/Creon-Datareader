@echo off
chcp 65001 >nul
REM Creon DataReader 간단 환경 설정 배치 파일

echo ========================================
echo Creon DataReader 환경 설정
echo ========================================

REM 현재 Python 아키텍처 확인
echo [1/4] Python 아키텍처 확인 중...
python -c "import struct; arch=struct.calcsize('P')*8; print(f'아키텍처: {arch}비트')"
if %errorlevel% neq 0 (
    echo [ERROR] Python이 설치되지 않았거나 실행할 수 없습니다.
    pause
    exit /b 1
)

REM 가상환경 생성
echo.
echo [2/4] 가상환경 생성 중...
set VENV_NAME=venv_creon

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

echo [INFO] 가상환경 생성: %VENV_NAME%
python -m venv %VENV_NAME%
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 생성 실패
    echo python -m venv 명령이 실패했습니다.
    pause
    exit /b 1
)

echo [OK] 가상환경 생성 완료

:ACTIVATE_VENV
echo.
echo [3/4] 가상환경 활성화 및 패키지 설치 중...

REM 가상환경 활성화
call %VENV_NAME%\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 활성화 실패
    pause
    exit /b 1
)

echo [OK] 가상환경 활성화 완료

REM Python 아키텍처 다시 확인
python -c "import struct; arch=struct.calcsize('P')*8; print(f'가상환경 아키텍처: {arch}비트')"

REM 필수 패키지 설치
echo [INFO] 필수 패키지 설치 중...

REM 패키지 목록
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

echo.
echo [4/4] 환경 테스트 중...

REM 환경 테스트 스크립트 생성
echo import sys > test_env.py
echo import struct >> test_env.py
echo import pandas as pd >> test_env.py
echo import numpy as np >> test_env.py
echo import yaml >> test_env.py
echo import sqlalchemy >> test_env.py
echo. >> test_env.py
echo print("=" * 60) >> test_env.py
echo print("Creon DataReader 환경 테스트") >> test_env.py
echo print("=" * 60) >> test_env.py
echo. >> test_env.py
echo print(f"Python 버전: {sys.version}") >> test_env.py
echo print(f"아키텍처: {struct.calcsize('P') * 8}비트") >> test_env.py
echo. >> test_env.py
echo print("필수 패키지 버전:") >> test_env.py
echo print(f"  pandas: {pd.__version__}") >> test_env.py
echo print(f"  numpy: {np.__version__}") >> test_env.py
echo print(f"  PyYAML: {yaml.__version__}") >> test_env.py
echo print(f"  SQLAlchemy: {sqlalchemy.__version__}") >> test_env.py
echo. >> test_env.py
echo print("=" * 60) >> test_env.py

python test_env.py
del test_env.py

REM 활성화 스크립트 생성
echo @echo off > activate_creon.bat
echo REM Creon DataReader 가상환경 활성화 스크립트 >> activate_creon.bat
echo. >> activate_creon.bat
echo echo ======================================== >> activate_creon.bat
echo echo Creon DataReader 환경 활성화 >> activate_creon.bat
echo echo ======================================== >> activate_creon.bat
echo. >> activate_creon.bat
echo call %VENV_NAME%\Scripts\activate.bat >> activate_creon.bat
echo. >> activate_creon.bat
echo if %%errorlevel%% equ 0 ( >> activate_creon.bat
echo   echo [OK] 환경 활성화 완료 >> activate_creon.bat
echo   echo. >> activate_creon.bat
echo   echo 사용 가능한 명령어: >> activate_creon.bat
echo   echo   python creon_main.py -h          도움말 보기 >> activate_creon.bat
echo   echo   python creon_main.py collect     데이터 수집 >> activate_creon.bat
echo   echo   python creon_main.py merge       데이터 병합 >> activate_creon.bat
echo   echo   python test_database_simple.py   데이터베이스 테스트 >> activate_creon.bat
echo   echo. >> activate_creon.bat
echo   echo 환경 비활성화: deactivate >> activate_creon.bat
echo ) else ( >> activate_creon.bat
echo   echo [ERROR] 환경 활성화 실패 >> activate_creon.bat
echo   echo 가상환경이 올바르게 생성되었는지 확인하세요. >> activate_creon.bat
echo ) >> activate_creon.bat

echo [OK] 활성화 스크립트 생성 완료: activate_creon.bat

echo.
echo ========================================
echo 환경 설정 완료!
echo ========================================
echo.
echo 다음 단계:
echo 1. 환경 활성화: activate_creon.bat
echo 2. Creon Plus 프로그램 실행 및 로그인
echo 3. 데이터베이스 테스트: python test_database_simple.py
echo 4. Creon API 테스트: python test_creon_api.py
echo 5. Creon DataReader 실행: python creon_main.py -h
echo.
echo 참고: Creon API 테스트를 위해 Creon Plus가 실행 중이어야 합니다.
echo.

pause