@echo off
REM Creon DataReader를 위한 32비트 Python 환경 설정 스크립트
REM Anaconda를 사용하여 Creon API 호환 환경 생성

echo ========================================
echo Creon DataReader 환경 설정
echo ========================================

REM Anaconda 설치 경로 확인
set ANACONDA_PATH=C:\ProgramData\Anaconda3
if not exist "%ANACONDA_PATH%" (
    echo [ERROR] Anaconda가 설치되지 않았습니다.
    echo Anaconda 설치 경로: %ANACONDA_PATH%
    pause
    exit /b 1
)

echo [INFO] Anaconda 경로: %ANACONDA_PATH%

REM 환경 이름 설정
set ENV_NAME=creon_datareader
set PYTHON_VERSION=3.8  # Creon API와 호환되는 Python 버전

echo [INFO] 환경 생성: %ENV_NAME% (Python %PYTHON_VERSION%)

REM Conda 환경 생성 (32비트 Python)
call "%ANACONDA_PATH%\Scripts\conda.exe" create -n %ENV_NAME% python=%PYTHON_VERSION% -y
if %errorlevel% neq 0 (
    echo [ERROR] Conda 환경 생성 실패
    pause
    exit /b 1
)

echo [OK] Conda 환경 생성 완료: %ENV_NAME%

REM 환경 활성화 및 패키지 설치
echo [INFO] 필수 패키지 설치 중...

REM Creon DataReader에 필요한 패키지
call "%ANACONDA_PATH%\Scripts\conda.exe" activate %ENV_NAME%
if %errorlevel% neq 0 (
    echo [ERROR] 환경 활성화 실패
    pause
    exit /b 1
)

REM 기본 패키지 설치
pip install pywin32
pip install pandas
pip install numpy
pip install pyyaml
pip install sqlalchemy

echo [OK] 기본 패키지 설치 완료

REM 프로젝트 의존성 설치
echo [INFO] 프로젝트 의존성 설치 중...
pip install -r requirements.txt 2>nul
if %errorlevel% neq 0 (
    echo [WARNING] requirements.txt 파일이 없습니다. 기본 패키지로 진행합니다.
)

echo [OK] 프로젝트 의존성 설치 완료

REM 환경 정보 저장
echo [INFO] 환경 정보 저장 중...
echo # Creon DataReader 환경 설정 > environment.yml
call "%ANACONDA_PATH%\Scripts\conda.exe" env export -n %ENV_NAME% >> environment.yml

echo [OK] 환경 정보 저장 완료: environment.yml

REM 환경 테스트
echo [INFO] 환경 테스트 중...
python -c "import sys; print('Python 버전:', sys.version)"
python -c "import struct; print('아키텍처:', struct.calcsize('P') * 8, '비트')"
python -c "import pandas; print('Pandas 버전:', pandas.__version__)"
python -c "import numpy; print('NumPy 버전:', numpy.__version__)"

echo.
echo ========================================
echo 환경 설정 완료!
echo ========================================
echo.
echo 사용 방법:
echo 1. 환경 활성화: conda activate %ENV_NAME%
echo 2. 프로젝트 실행: python creon_main.py
echo 3. 환경 비활성화: conda deactivate
echo.
echo 환경 정보 파일: environment.yml
echo.

pause