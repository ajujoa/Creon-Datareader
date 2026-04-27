@echo off
chcp 65001 >nul
REM Anaconda 초기화 스크립트

echo ========================================
echo Anaconda 초기화
echo ========================================

set ANACONDA_PATH=C:\ProgramData\Anaconda3

if not exist "%ANACONDA_PATH%" (
    echo [ERROR] Anaconda가 설치되지 않았습니다.
    echo 설치 경로: %ANACONDA_PATH%
    pause
    exit /b 1
)

echo [INFO] Anaconda 초기화 중...

REM Conda 초기화
call "%ANACONDA_PATH%\Scripts\conda.exe" init cmd.exe

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
