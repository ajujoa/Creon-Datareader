@echo off
REM Creon DataReader 가상환경 활성화 스크립트

echo ========================================
echo Creon DataReader 환경 활성화
echo ========================================

call venv_creon\Scripts\activate.bat

if %errorlevel% equ 0 (
    echo [OK] 환경 활성화 완료
    echo.
    echo 사용 가능한 명령어:
    echo   python creon_main.py -h          도움말 보기
    echo   python creon_main.py collect     데이터 수집
    echo   python creon_main.py merge       데이터 병합
    echo   python test_database_simple.py   데이터베이스 테스트
    echo   python test_creon_api.py         Creon API 테스트
    echo.
    echo 환경 비활성화: deactivate
) else (
    echo [ERROR] 환경 활성화 실패
    echo 가상환경이 올바르게 생성되었는지 확인하세요.
)
