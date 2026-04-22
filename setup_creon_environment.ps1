# Creon DataReader 통합 환경 설정 스크립트
# 32비트 Python 환경 설정 및 테스트

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Creon DataReader 통합 환경 설정" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 현재 디렉토리 저장
$currentDir = Get-Location
Write-Host "[INFO] 작업 디렉토리: $currentDir" -ForegroundColor Green

# 1. Python 아키텍처 확인
Write-Host "`n[1/5] Python 아키텍처 확인 중..." -ForegroundColor Yellow

function Test-PythonArchitecture {
    try {
        $output = python -c "import struct; print(struct.calcsize('P') * 8)" 2>$null
        if ($output) {
            return [int]$output
        }
    } catch {
        # Python이 실행되지 않음
    }
    return $null
}

$pythonArch = Test-PythonArchitecture
if ($pythonArch -eq 32) {
    Write-Host "[OK] 32비트 Python이 감지되었습니다." -ForegroundColor Green
    $need32bit = $false
} elseif ($pythonArch -eq 64) {
    Write-Host "[WARNING] 64비트 Python이 감지되었습니다." -ForegroundColor Yellow
    Write-Host "  Creon API는 32비트 Python이 필요합니다." -ForegroundColor Yellow
    $need32bit = $true
} else {
    Write-Host "[ERROR] Python이 설치되지 않았거나 실행할 수 없습니다." -ForegroundColor Red
    $need32bit = $true
}

# 2. 32비트 Python 필요 시 처리
if ($need32bit) {
    Write-Host "`n[2/5] 32비트 Python 설정 중..." -ForegroundColor Yellow
    
    Write-Host "[INFO] 다음 중 하나를 선택하세요:" -ForegroundColor White
    Write-Host "  1. 공식 Python 3.8.10 32비트 설치" -ForegroundColor White
    Write-Host "  2. 기존 32비트 Python 경로 지정" -ForegroundColor White
    Write-Host "  3. 스크립트 종료 (수동 설치)" -ForegroundColor White
    
    $choice = Read-Host "선택 (1-3)"
    
    switch ($choice) {
        "1" {
            Write-Host "[INFO] Python 3.8.10 32비트 다운로드 링크:" -ForegroundColor Blue
            Write-Host "  https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe" -ForegroundColor Blue
            Write-Host "설치 후 스크립트를 다시 실행하세요." -ForegroundColor Yellow
            pause
            exit 0
        }
        "2" {
            $pythonPath = Read-Host "32비트 Python 경로 입력 (예: C:\Python38-32\python.exe)"
            if (Test-Path $pythonPath) {
                # Python 경로를 환경 변수에 임시 추가
                $pythonDir = Split-Path $pythonPath -Parent
                $env:Path = "$pythonDir;$env:Path"
                Write-Host "[OK] Python 경로가 임시로 추가되었습니다." -ForegroundColor Green
                
                # 다시 아키텍처 확인
                $pythonArch = Test-PythonArchitecture
                if ($pythonArch -eq 32) {
                    Write-Host "[OK] 32비트 Python 확인 완료" -ForegroundColor Green
                    $need32bit = $false
                } else {
                    Write-Host "[ERROR] 지정된 경로가 32비트 Python이 아닙니다." -ForegroundColor Red
                    $need32bit = $true
                }
            } else {
                Write-Host "[ERROR] 지정된 경로가 존재하지 않습니다." -ForegroundColor Red
                $need32bit = $true
            }
        }
        default {
            Write-Host "[INFO] 스크립트를 종료합니다." -ForegroundColor Yellow
            Write-Host "수동으로 32비트 Python을 설치한 후 다시 실행하세요." -ForegroundColor White
            pause
            exit 0
        }
    }
}

# 3. 가상환경 생성
Write-Host "`n[3/5] 가상환경 생성 중..." -ForegroundColor Yellow

$venvName = "venv_creon"
if (Test-Path $venvName) {
    Write-Host "[INFO] 기존 가상환경이 존재합니다: $venvName" -ForegroundColor Yellow
    $choice = Read-Host "재생성하시겠습니까? (y/n)"
    if ($choice -eq 'y') {
        Remove-Item -Path $venvName -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "[OK] 기존 가상환경 삭제 완료" -ForegroundColor Green
    } else {
        Write-Host "[INFO] 기존 가상환경을 사용합니다." -ForegroundColor Green
        goto ACTIVATE_VENV
    }
}

# 가상환경 생성
Write-Host "[INFO] 가상환경 생성: $venvName" -ForegroundColor White
python -m venv $venvName
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] 가상환경 생성 실패" -ForegroundColor Red
    Write-Host "  python -m venv 명령이 실패했습니다." -ForegroundColor Yellow
    Write-Host "  Python이 올바르게 설치되었는지 확인하세요." -ForegroundColor Yellow
    pause
    exit 1
}

Write-Host "[OK] 가상환경 생성 완료" -ForegroundColor Green

:ACTIVATE_VENV
# 4. 가상환경 활성화 및 패키지 설치
Write-Host "`n[4/5] 가상환경 활성화 및 패키지 설치 중..." -ForegroundColor Yellow

# 가상환경 활성화
$activateScript = Join-Path $venvName "Scripts\Activate.ps1"
if (Test-Path $activateScript) {
    & $activateScript
    Write-Host "[OK] 가상환경 활성화 완료" -ForegroundColor Green
} else {
    Write-Host "[ERROR] 가상환경 활성화 스크립트를 찾을 수 없습니다." -ForegroundColor Red
    Write-Host "  가상환경이 올바르게 생성되었는지 확인하세요." -ForegroundColor Yellow
    pause
    exit 1
}

# Python 아키텍처 다시 확인 (가상환경 내)
$venvPythonArch = Test-PythonArchitecture
Write-Host "[INFO] 가상환경 Python 아키텍처: $venvPythonArch비트" -ForegroundColor White

if ($venvPythonArch -ne 32) {
    Write-Host "[WARNING] 가상환경이 32비트 Python을 사용하지 않습니다." -ForegroundColor Yellow
    Write-Host "  Creon API 호환성에 문제가 있을 수 있습니다." -ForegroundColor Yellow
}

# 필수 패키지 설치
Write-Host "[INFO] 필수 패키지 설치 중..." -ForegroundColor White

$packages = @(
    @{Name="pywin32"; Description="Windows API 접근"},
    @{Name="pandas"; Description="데이터 분석"},
    @{Name="numpy"; Description="수치 계산"},
    @{Name="pyyaml"; Description="YAML 설정 파일"},
    @{Name="sqlalchemy"; Description="데이터베이스 ORM"},
    @{Name="pymysql"; Description="MySQL/MariaDB 연결"},
    @{Name="psycopg2-binary"; Description="PostgreSQL 연결"}
)

foreach ($pkg in $packages) {
    Write-Host "  - $($pkg.Name) 설치 중... ($($pkg.Description))" -ForegroundColor Gray
    pip install $pkg.Name --quiet
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] $($pkg.Name) 설치 완료" -ForegroundColor Green
    } else {
        Write-Host "    [WARNING] $($pkg.Name) 설치 실패" -ForegroundColor Yellow
    }
}

# 프로젝트 의존성 설치
Write-Host "[INFO] 프로젝트 의존성 설치 중..." -ForegroundColor White
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt
    Write-Host "[OK] requirements.txt 설치 완료" -ForegroundColor Green
} else {
    Write-Host "[INFO] requirements.txt 파일이 없습니다." -ForegroundColor Yellow
}

# 5. 환경 테스트
Write-Host "`n[5/5] 환경 테스트 중..." -ForegroundColor Yellow

# 테스트 스크립트 실행
$testScript = @'
import sys
import struct
import pandas as pd
import numpy as np
import yaml
import sqlalchemy
import pymysql
try:
    import psycopg2
    psycopg2_installed = True
except ImportError:
    psycopg2_installed = False

print("=" * 60)
print("Creon DataReader 환경 테스트")
print("=" * 60)

print(f"Python 버전: {sys.version}")
print(f"아키텍처: {struct.calcsize('P') * 8}비트")

print("\n필수 패키지 버전:")
print(f"  pandas: {pd.__version__}")
print(f"  numpy: {np.__version__}")
print(f"  PyYAML: {yaml.__version__}")
print(f"  SQLAlchemy: {sqlalchemy.__version__}")
print(f"  PyMySQL: {pymysql.__version__}")
print(f"  psycopg2: {'설치됨' if psycopg2_installed else '설치되지 않음'}")

# Creon API 관련 테스트
try:
    import win32com.client
    print(f"  pywin32: 설치됨 (Creon API 호환)")
    
    # Creon 객체 생성 시도
    try:
        cp = win32com.client.Dispatch("CpUtil.CpCybos")
        print(f"  Creon Cybos: 연결 가능")
    except Exception as e:
        print(f"  Creon Cybos: 연결 실패 - {str(e)}")
        
except ImportError:
    print(f"  pywin32: 설치되지 않음 (Creon API 사용 불가)")

print("\n" + "=" * 60)
print("테스트 완료")
print("=" * 60)
'@

# 테스트 스크립트를 파일로 저장하고 실행
$testScript | Out-File -FilePath "test_environment.py" -Encoding UTF8
python test_environment.py

# 테스트 파일 정리
Remove-Item -Path "test_environment.py" -ErrorAction SilentlyContinue

# 활성화 스크립트 생성
Write-Host "`n[INFO] 활성화 스크립트 생성 중..." -ForegroundColor Yellow

$activateBat = @'
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
    echo.
    echo 환경 비활성화: deactivate
) else (
    echo [ERROR] 환경 활성화 실패
    echo 가상환경이 올바르게 생성되었는지 확인하세요.
)
'@

$activateBat | Out-File -FilePath "activate_creon.bat" -Encoding ASCII

Write-Host "[OK] 활성화 스크립트 생성 완료: activate_creon.bat" -ForegroundColor Green

# 환경 설정 파일 생성
Write-Host "[INFO] 환경 설정 파일 생성 중..." -ForegroundColor Yellow

$envConfig = @'
# Creon DataReader 환경 설정
# 생성일: $(Get-Date -Format "yyyy-MM-dd")

## Python 정보
- 아키텍처: $venvPythonArch비트
- 가상환경: $venvName
- 생성 경로: $currentDir\$venvName

## 설치된 패키지
$(foreach ($pkg in $packages) {
    "- $($pkg.Name): $($pkg.Description)"
})

## 사용 방법
1. 환경 활성화: .\activate_creon.bat
2. Creon DataReader 실행: python creon_main.py
3. 데이터베이스 테스트: python test_database_simple.py
4. 환경 비활성화: deactivate

## 주의사항
- Creon API는 32비트 Python이 필요합니다.
- Creon Plus 프로그램이 설치되어 있어야 합니다.
- Windows 운영체제에서만 동작합니다.
'@

$envConfig | Out-File -FilePath "ENVIRONMENT_SETUP.md" -Encoding UTF8

Write-Host "[OK] 환경 설정 파일 생성 완료: ENVIRONMENT_SETUP.md" -ForegroundColor Green

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "환경 설정 완료!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "다음 단계:" -ForegroundColor Yellow
Write-Host "1. 환경 활성화: .\activate_creon.bat" -ForegroundColor White
Write-Host "2. Creon Plus 프로그램 실행 및 로그인" -ForegroundColor White
Write-Host "3. 데이터베이스 테스트: python test_database_simple.py" -ForegroundColor White
Write-Host "4. Creon DataReader 실행: python creon_main.py -h" -ForegroundColor White
Write-Host ""
Write-Host "생성된 파일:" -ForegroundColor Yellow
Write-Host "  - activate_creon.bat: 환경 활성화 스크립트" -ForegroundColor White
Write-Host "  - ENVIRONMENT_SETUP.md: 환경 설정 문서" -ForegroundColor White
Write-Host "  - venv_creon/: 가상환경 디렉토리" -ForegroundColor White
Write-Host ""
Write-Host "참고: Creon API 테스트를 위해 Creon Plus가 실행 중이어야 합니다." -ForegroundColor Magenta
Write-Host ""

pause