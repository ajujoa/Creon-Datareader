@echo off
chcp 65001 >nul
REM Creon DataReader 32비트 환경 설정 스크립트

echo ========================================
echo Creon DataReader 32비트 환경 설정
echo ========================================

echo [INFO] 가상환경 확인 중...
if not exist "venv_creon32\Scripts\activate.bat" (
    echo [ERROR] 가상환경을 찾을 수 없습니다.
    echo 가상환경 생성: python -m venv venv_creon32
    pause
    exit /b 1
)

echo [INFO] 가상환경 활성화 중...
call venv_creon32\Scripts\activate.bat

if %errorlevel% neq 0 (
    echo [ERROR] 가상환경 활성화 실패
    pause
    exit /b 1
)

echo [OK] 가상환경 활성화 완료

echo [INFO] Python 정보 확인:
python --version
python -c "import struct; print('Python 아키텍처:', struct.calcsize('P') * 8, '비트')"

echo.
echo [INFO] 필수 패키지 설치 중...
echo 1. pywin32 (Creon API 필수)
echo 2. pandas (데이터 처리)
echo 3. numpy (수치 계산)
echo 4. pyyaml (설정 파일)
echo 5. sqlalchemy (데이터베이스)

pip install pywin32 pandas numpy pyyaml sqlalchemy

if %errorlevel% equ 0 (
    echo [OK] 패키지 설치 완료
) else (
    echo [ERROR] 패키지 설치 실패
    pause
    exit /b 1
)

echo.
echo [INFO] 설치된 패키지 확인:
pip list

echo.
echo [INFO] Creon DataReader 모듈 테스트 준비 중...

REM 테스트 스크립트 실행
if exist "test_config.py" (
    echo [INFO] 설정 모듈 테스트:
    python test_config.py
) else (
    echo [WARNING] test_config.py를 찾을 수 없습니다.
)

if exist "test_database_simple.py" (
    echo.
    echo [INFO] 데이터베이스 모듈 테스트:
    python test_database_simple.py
) else (
    echo [WARNING] test_database_simple.py를 찾을 수 없습니다.
)

echo.
echo ========================================
echo 환경 설정 완료
echo ========================================

echo.
echo 다음 단계:
echo 1. Creon Plus 프로그램 실행
echo 2. Creon API 로그인
echo 3. test_creon_api.py 실행으로 API 연결 테스트
echo 4. creon_main.py 실행으로 전체 시스템 테스트

echo.
echo 주의: Creon API는 32비트 Python에서만 작동합니다.
echo 현재 환경이 32비트인지 확인하세요.

pause