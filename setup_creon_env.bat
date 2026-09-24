@echo off
chcp 65001 >nul
REM ==========================================
REM Creon DataReader MH 환경 설정 스크립트
REM 기반: https://github.com/gyusu/Creon-Datareader
REM ==========================================

echo ========================================
echo Creon DataReader MH - 32bit 환경 설정
echo ========================================
echo.

REM 1. Python 32bit 확인
echo [1/4] Python 환경 확인 중...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python이 설치되어 있지 않습니다.
    echo 32bit Python 3.8 이상을 설치해주세요.
    echo 다운로드: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 32bit 아키텍처 확인
python -c "import struct; assert struct.calcsize('P')==4, '64bit Python detected'; print('OK: 32bit Python')"
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] 32bit Python이 필요합니다. 현재 64bit입니다.
    echo.
    echo 해결 방법:
    echo   1. 32bit Python 별도 설치: https://www.python.org/downloads/windows/
    echo      (Windows x86 executable installer 선택)
    echo   2. 또는 Anaconda 32bit 환경 구성:
    echo      set CONDA_FORCE_32BIT=1
    echo      conda create -n creon32 python=3.8
    pause
    exit /b 1
)

echo [OK] 32bit Python 환경 확인됨
echo.

REM 2. 가상환경 생성
echo [2/4] 가상환경 설정 중...
set VENV_DIR=venv_creon32

if exist "%VENV_DIR%\Scripts\activate.bat" (
    echo [INFO] 기존 가상환경 발견: %VENV_DIR%
    choice /c yn /m "가상환경을 재생성하시겠습니까?"
    if !errorlevel! equ 1 (
        echo [INFO] 기존 가상환경 삭제 중...
        rmdir /s /q "%VENV_DIR%"
    ) else (
        goto :activate_venv
    )
)

echo [INFO] 가상환경 생성 중...
python -m venv %VENV_DIR%
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 생성 실패
    pause
    exit /b 1
)
echo [OK] 가상환경 생성 완료

:activate_venv
echo [INFO] 가상환경 활성화 중...
call %VENV_DIR%\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 활성화 실패
    pause
    exit /b 1
)
echo [OK] 가상환경 활성화 완료
echo.

REM 3. 패키지 설치
echo [3/4] 필수 패키지 설치 중...
echo.
echo 설치할 패키지:
echo   - pywin32     : Creon Plus API 통신
echo   - pandas      : 데이터 처리
echo   - numpy       : 수치 계산
echo   - pykrx       : 한국 주식 데이터
echo   - exchange-calendars : 거래일 캘린더
echo   - finance-datareader : 종목 리스트
echo   - tqdm        : 진행률 표시
echo   - matplotlib  : 차트 표시 (옵션)
echo.
echo pip install 중...

pip install --upgrade pip
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] 패키지 설치 실패
    echo 수동 설치: pip install pywin32 pandas numpy pykrx exchange-calendars finance-datareader tqdm matplotlib openpyxl
    pause
    exit /b 1
)
echo [OK] 패키지 설치 완료
echo.

REM 4. 설치 확인
echo [4/4] 설치 확인 중...
echo.

python -c "import win32com.client; print('[OK] pywin32 - Creon API')" 2>nul || echo "[FAIL] pywin32"
python -c "import pandas; print('[OK] pandas', pandas.__version__)" 2>nul || echo "[FAIL] pandas"
python -c "import numpy; print('[OK] numpy', numpy.__version__)" 2>nul || echo "[FAIL] numpy"
python -c "import pykrx; print('[OK] pykrx')" 2>nul || echo "[FAIL] pykrx"
python -c "import exchange_calendars; print('[OK] exchange_calendars')" 2>nul || echo "[FAIL] exchange_calendars"
python -c "import FinanceDataReader; print('[OK] FinanceDataReader')" 2>nul || echo "[FAIL] FinanceDataReader"
python -c "import tqdm; print('[OK] tqdm')" 2>nul || echo "[FAIL] tqdm"
python -c "import matplotlib; print('[OK] matplotlib')" 2>nul || echo "[FAIL] matplotlib"

echo.
echo ========================================
echo 환경 설정 완료!
echo ========================================
echo.
echo 실행 방법:
echo   1. Creon Plus 프로그램 실행 및 로그인
echo   2. 가상환경 활성화: venv_creon32\Scripts\activate
echo   3. 데이터 수집 실행: python MH_creon_datareader_down_20260106.py
echo.
echo 설정 변경은 MH_creon_datareader_down_20260106.py 의 self.settings 에서 수정:
echo   - tick_unit: '일봉' 또는 '분봉'
echo   - tick_range: 분봉 간격 (1,3,5,10,15,30,60)
echo   - count: 요청할 데이터 개수
echo   - ohlcv_only: OHLCV만 수집 여부
echo   - max_stocks: 최대 종목 수 (0=전체)
echo.
echo 데이터 저장 경로: json_data/ (실행 시점별 자동 생성)
echo.
echo [주의] Creon API는 Windows 32bit Python에서만 동작합니다.
echo [주의] Creon Plus HTS가 실행 중이고 로그인되어 있어야 합니다.
echo [주의] API 요청 제한: 시간당 약 300회 (1분당 60회)
echo.
pause
