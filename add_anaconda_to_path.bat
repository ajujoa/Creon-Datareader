@echo off
chcp 65001 >nul
REM Anaconda PATH 추가 스크립트

echo ========================================
echo Anaconda PATH 설정
echo ========================================

set ANACONDA_PATH=C:\ProgramData\Anaconda3
set CONDA_SCRIPTS=%ANACONDA_PATH%\Scripts
set CONDA_BIN=%ANACONDA_PATH%\Library\bin

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
