@echo off
cd /d %~dp0
title Creon Datareader v1.0
cls

echo ============================================
echo   Creon Datareader v1.0
echo   %date% %time%
echo ============================================
echo.
echo [1/2] Creon Plus 접속 확인 및로그인...
echo.

echo.
echo [2/2] 데이터 수집 중... (로그: logs\)
echo.

REM Use embedded 32bit Python
set PATH=%~dp0python32;%~dp0python32\Scripts;%PATH%

python32\python.exe creon_chart_down.py

echo.
echo 완료.
echo.

pause
