@echo off
chcp 65001 >nul
REM 32비트 Python 3.8.10 자동 설치 스크립트

echo ========================================
echo 32비트 Python 3.8.10 설치
echo ========================================

set PYTHON_INSTALLER=python-3.8.10-32bit.exe
set INSTALL_PATH=C:\Python38-32bit

echo [INFO] 설치 프로그램 확인 중...
if not exist "%PYTHON_INSTALLER%" (
    echo [ERROR] 설치 프로그램을 찾을 수 없습니다: %PYTHON_INSTALLER%
    pause
    exit /b 1
)

echo [INFO] 설치 경로: %INSTALL_PATH%
echo [INFO] 설치 중... (잠시 기다려주세요)

REM Python 3.8.10 자동 설치 옵션:
REM /quiet - 자동 설치
REM InstallAllUsers=1 - 모든 사용자용 설치
REM PrependPath=1 - PATH에 추가
REM TargetDir=설치경로 - 설치 경로 지정
REM AssociateFiles=0 - 파일 연결 안함
REM Shortcuts=0 - 바로가기 생성 안함

"%PYTHON_INSTALLER%" /quiet InstallAllUsers=1 PrependPath=1 TargetDir="%INSTALL_PATH%" AssociateFiles=0 Shortcuts=0

if %errorlevel% equ 0 (
    echo [OK] Python 3.8.10 32비트 설치 완료
    echo.
    echo 설치 경로: %INSTALL_PATH%
    echo.
    echo 다음 단계:
    echo 1. 새 명령 프롬프트 열기
    echo 2. python --version 명령으로 확인
    echo 3. python -c "import struct; print(struct.calcsize('P') * 8)" 로 아키텍처 확인
) else (
    echo [ERROR] Python 설치 실패 (오류 코드: %errorlevel%)
)

echo.
echo ========================================
echo 설치 완료
echo ========================================

REM 설치 후 PATH 확인
echo.
echo [INFO] PATH 확인:
echo %PATH% | findstr /i "python38-32bit"

pause