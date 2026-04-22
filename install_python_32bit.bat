@echo off
chcp 65001 >nul
REM Python 3.8.10 32비트 설치 스크립트

echo ========================================
echo Python 3.8.10 32비트 설치
echo ========================================

REM 설치 파일 확인
if not exist "python-3.8.10-32bit.exe" (
    echo [ERROR] 설치 파일이 없습니다: python-3.8.10-32bit.exe
    echo 다음 링크에서 다운로드하세요:
    echo https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe
    pause
    exit /b 1
)

echo [INFO] Python 3.8.10 32비트 설치를 시작합니다.
echo.
echo 설치 옵션:
echo 1. 설치 경로: C:\Python38-32
echo 2. Add Python to PATH 체크
echo 3. pip 설치 체크
echo 4. 모든 사용자용 설치
echo.

set /p CONFIRM=설치를 진행하시겠습니까? (y/n): 
if /i not "%CONFIRM%"=="y" (
    echo [INFO] 설치가 취소되었습니다.
    pause
    exit /b 0
)

echo [INFO] 설치 중... (잠시만 기다려주세요)
echo.

REM Python 설치 실행
REM 참고: 자동 설치를 위해 다음 옵션 사용
REM /quiet: 자동 설치
REM InstallAllUsers=1: 모든 사용자용
REM PrependPath=1: PATH에 추가
REM TargetDir=C:\Python38-32: 설치 경로

start /wait python-3.8.10-32bit.exe /quiet InstallAllUsers=1 PrependPath=1 TargetDir="C:\Python38-32" Include_test=0

if %errorlevel% neq 0 (
    echo [ERROR] Python 설치 실패
    pause
    exit /b 1
)

echo [OK] Python 3.8.10 32비트 설치 완료
echo.

REM 설치 확인
echo [INFO] 설치 확인 중...
if exist "C:\Python38-32\python.exe" (
    echo [OK] Python 설치 경로: C:\Python38-32\python.exe
    
    REM Python 아키텍처 확인
    echo [INFO] Python 아키텍처 확인...
    "C:\Python38-32\python.exe" -c "import struct; arch=struct.calcsize('P')*8; print(f'아키텍처: {arch}비트')"
    
    if %errorlevel% equ 0 (
        echo [OK] 32비트 Python 설치 확인 완료
    ) else (
        echo [WARNING] Python 실행에 문제가 있습니다.
    )
) else (
    echo [ERROR] Python이 지정된 경로에 설치되지 않았습니다.
)

echo.
echo ========================================
echo 설치 완료!
echo ========================================
echo.
echo 다음 단계:
echo 1. 새 명령 프롬프트를 열어 PATH 업데이트 확인
echo 2. 32비트 Python 확인: python -c "import struct; print(struct.calcsize('P')*8)"
echo 3. 가상환경 생성: setup_creon_32bit.bat 실행
echo.

REM 32비트 환경 설정 스크립트 생성
echo @echo off > setup_creon_32bit.bat
echo chcp 65001 ^>nul >> setup_creon_32bit.bat
echo REM Creon DataReader 32비트 환경 설정 >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo echo ======================================== >> setup_creon_32bit.bat
echo echo Creon DataReader 32비트 환경 설정 >> setup_creon_32bit.bat
echo echo ======================================== >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo REM 32비트 Python 사용 >> setup_creon_32bit.bat
echo set PYTHON32=C:\Python38-32\python.exe >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo REM 가상환경 생성 >> setup_creon_32bit.bat
echo echo [INFO] 32비트 가상환경 생성 중... >> setup_creon_32bit.bat
echo "%PYTHON32%" -m venv venv_creon_32bit >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo REM 가상환경 활성화 및 패키지 설치 >> setup_creon_32bit.bat
echo call venv_creon_32bit\Scripts\activate.bat >> setup_creon_32bit.bat
echo pip install pywin32 pandas numpy pyyaml sqlalchemy pymysql psycopg2-binary >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo REM 테스트 >> setup_creon_32bit.bat
echo python -c "import struct; print('아키텍처:', struct.calcsize('P')*8, '비트')" >> setup_creon_32bit.bat
echo python test_creon_api.py >> setup_creon_32bit.bat
echo. >> setup_creon_32bit.bat
echo echo [OK] 32비트 환경 설정 완료 >> setup_creon_32bit.bat
echo pause >> setup_creon_32bit.bat

echo [OK] 32비트 환경 설정 스크립트 생성 완료: setup_creon_32bit.bat
echo.

pause