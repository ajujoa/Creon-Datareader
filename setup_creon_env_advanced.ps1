# Creon DataReader를 위한 32비트 Python 환경 설정 PowerShell 스크립트
# Anaconda를 사용하여 Creon API 호환 환경 생성

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Creon DataReader 환경 설정" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Anaconda 설치 경로 확인
$AnacondaPath = "C:\ProgramData\Anaconda3"
if (-not (Test-Path $AnacondaPath)) {
    Write-Host "[ERROR] Anaconda가 설치되지 않았습니다." -ForegroundColor Red
    Write-Host "Anaconda 설치 경로: $AnacondaPath" -ForegroundColor Yellow
    pause
    exit 1
}

Write-Host "[INFO] Anaconda 경로: $AnacondaPath" -ForegroundColor Green

# Conda 실행 파일 경로
$CondaExe = Join-Path $AnacondaPath "Scripts\conda.exe"

# 환경 이름 설정
$EnvName = "creon_datareader"
$PythonVersion = "3.8"  # Creon API와 호환되는 Python 버전

Write-Host "[INFO] 환경 생성: $EnvName (Python $PythonVersion)" -ForegroundColor Green

# 기존 환경 확인
Write-Host "[INFO] 기존 환경 확인 중..." -ForegroundColor Yellow
$envList = & $CondaExe env list
if ($envList -match $EnvName) {
    Write-Host "[WARNING] '$EnvName' 환경이 이미 존재합니다." -ForegroundColor Yellow
    $choice = Read-Host "재생성하시겠습니까? (y/n)"
    if ($choice -eq 'y') {
        Write-Host "[INFO] 기존 환경 삭제 중..." -ForegroundColor Yellow
        & $CondaExe env remove -n $EnvName -y
    } else {
        Write-Host "[INFO] 기존 환경을 사용합니다." -ForegroundColor Green
        # 환경 활성화 및 업데이트
        & $CondaExe activate $EnvName
        goto INSTALL_PACKAGES
    }
}

# 32비트 Python을 위한 채널 설정
# Anaconda는 기본적으로 64비트이므로, 32비트 패키지를 찾기 위해 채널 추가가 필요할 수 있습니다.
Write-Host "[INFO] 32비트 Python 환경 생성 중..." -ForegroundColor Yellow

# Conda 환경 생성 (가능하면 32비트 Python)
# 참고: conda에서 명시적으로 32비트를 지정하는 방법은 제한적입니다.
# 대신 Python 3.8을 설치하고 필요한 32비트 패키지는 수동으로 설치합니다.
& $CondaExe create -n $EnvName python=$PythonVersion -y
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Conda 환경 생성 실패" -ForegroundColor Red
    pause
    exit 1
}

Write-Host "[OK] Conda 환경 생성 완료: $EnvName" -ForegroundColor Green

:INSTALL_PACKAGES
# 환경 활성화
Write-Host "[INFO] 환경 활성화 중..." -ForegroundColor Yellow
& $CondaExe activate $EnvName

# 필수 패키지 설치
Write-Host "[INFO] 필수 패키지 설치 중..." -ForegroundColor Yellow

# Creon API에 필요한 패키지
$packages = @(
    "pywin32",      # Windows API 접근
    "pandas",       # 데이터 분석
    "numpy",        # 수치 계산
    "pyyaml",       # 설정 파일
    "sqlalchemy",   # 데이터베이스 ORM
    "psycopg2-binary", # PostgreSQL 연결 (선택적)
    "pymysql",      # MySQL/MariaDB 연결 (선택적)
    "python-dateutil", # 날짜 처리
    "tzdata"        # 시간대 데이터
)

foreach ($package in $packages) {
    Write-Host "  - $package 설치 중..." -ForegroundColor Gray
    pip install $package --quiet
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] $package 설치 완료" -ForegroundColor Green
    } else {
        Write-Host "  [WARNING] $package 설치 실패" -ForegroundColor Yellow
    }
}

# 프로젝트 의존성 설치
Write-Host "[INFO] 프로젝트 의존성 설치 중..." -ForegroundColor Yellow
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt
    Write-Host "[OK] requirements.txt 설치 완료" -ForegroundColor Green
} else {
    Write-Host "[INFO] requirements.txt 파일이 없습니다." -ForegroundColor Yellow
}

# 환경 정보 저장
Write-Host "[INFO] 환경 정보 저장 중..." -ForegroundColor Yellow
& $CondaExe env export -n $EnvName > environment.yml
Write-Host "[OK] 환경 정보 저장 완료: environment.yml" -ForegroundColor Green

# 환경 테스트
Write-Host "[INFO] 환경 테스트 중..." -ForegroundColor Yellow

# Python 정보 확인
$pythonTest = @'
import sys
import struct
import platform

print("Python 정보:")
print(f"  버전: {sys.version}")
print(f"  아키텍처: {struct.calcsize('P') * 8}비트")
print(f"  플랫폼: {platform.platform()}")
print(f"  실행 파일: {sys.executable}")

# 필수 패키지 확인
try:
    import pandas
    print(f"  Pandas: {pandas.__version__}")
except ImportError:
    print("  Pandas: 설치되지 않음")

try:
    import numpy
    print(f"  NumPy: {numpy.__version__}")
except ImportError:
    print("  NumPy: 설치되지 않음")

try:
    import win32com
    print("  pywin32: 설치됨")
except ImportError:
    print("  pywin32: 설치되지 않음")
'@

$pythonTest | python

# 활성화 스크립트 생성
Write-Host "[INFO] 활성화 스크립트 생성 중..." -ForegroundColor Yellow

$activateScript = @'
@echo off
REM Creon DataReader 환경 활성화 스크립트

echo ========================================
echo Creon DataReader 환경 활성화
echo ========================================

call conda activate creon_datareader

if %errorlevel% equ 0 (
    echo [OK] 환경 활성화 완료
    echo.
    echo 사용 가능한 명령어:
    echo   python creon_main.py -h    도움말 보기
    echo   python creon_main.py collect --daily    일봉 데이터 수집
    echo   python creon_main.py collect --minute   분봉 데이터 수집
    echo.
    echo 환경 비활성화: conda deactivate
) else (
    echo [ERROR] 환경 활성화 실패
    echo 환경 생성 확인: conda env list
)
'@

$activateScript | Out-File -FilePath "activate_creon.bat" -Encoding ASCII

Write-Host "[OK] 활성화 스크립트 생성 완료: activate_creon.bat" -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "환경 설정 완료!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "사용 방법:" -ForegroundColor Yellow
Write-Host "1. 환경 활성화: .\activate_creon.bat" -ForegroundColor White
Write-Host "2. 또는: conda activate creon_datareader" -ForegroundColor White
Write-Host "3. 프로젝트 실행: python creon_main.py" -ForegroundColor White
Write-Host "4. 환경 비활성화: conda deactivate" -ForegroundColor White
Write-Host ""
Write-Host "생성된 파일:" -ForegroundColor Yellow
Write-Host "  - environment.yml: 환경 설정 파일" -ForegroundColor White
Write-Host "  - activate_creon.bat: 환경 활성화 스크립트" -ForegroundColor White
Write-Host ""
Write-Host "참고: Creon API는 32비트 Python이 필요합니다." -ForegroundColor Magenta
Write-Host "현재 환경이 32비트인지 확인하세요." -ForegroundColor Magenta
Write-Host "32비트 Python이 필요한 경우 별도로 설치해야 할 수 있습니다." -ForegroundColor Magenta
Write-Host ""

pause