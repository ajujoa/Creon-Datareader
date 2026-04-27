# Anaconda 설치 및 PATH 설정 확인 스크립트

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Anaconda 설치 및 PATH 설정 확인" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Anaconda 설치 경로 확인
$anacondaPaths = @(
    "C:\ProgramData\Anaconda3",
    "C:\Users\$env:USERNAME\Anaconda3",
    "C:\Program Files\Anaconda3",
    "C:\Program Files (x86)\Anaconda3"
)

$foundAnaconda = $false
$anacondaPath = ""

foreach ($path in $anacondaPaths) {
    if (Test-Path $path) {
        $foundAnaconda = $true
        $anacondaPath = $path
        Write-Host "[OK] Anaconda 발견: $path" -ForegroundColor Green
        break
    }
}

if (-not $foundAnaconda) {
    Write-Host "[ERROR] Anaconda를 찾을 수 없습니다." -ForegroundColor Red
    Write-Host "설치된 경로를 확인하세요." -ForegroundColor Yellow
    exit 1
}

# Anaconda 실행 파일 확인
Write-Host "`n[INFO] Anaconda 실행 파일 확인 중..." -ForegroundColor Yellow

$condaExe = Join-Path $anacondaPath "Scripts\conda.exe"
$pythonExe = Join-Path $anacondaPath "python.exe"

if (Test-Path $condaExe) {
    Write-Host "[OK] conda.exe 발견: $condaExe" -ForegroundColor Green
} else {
    Write-Host "[ERROR] conda.exe를 찾을 수 없습니다." -ForegroundColor Red
}

if (Test-Path $pythonExe) {
    Write-Host "[OK] python.exe 발견: $pythonExe" -ForegroundColor Green
    
    # Python 아키텍처 확인
    try {
        $archOutput = & $pythonExe -c "import struct; print(struct.calcsize('P') * 8)"
        Write-Host "[INFO] Anaconda Python 아키텍처: $archOutput비트" -ForegroundColor White
    } catch {
        Write-Host "[WARNING] Python 아키텍처 확인 실패" -ForegroundColor Yellow
    }
} else {
    Write-Host "[ERROR] python.exe를 찾을 수 없습니다." -ForegroundColor Red
}

# PATH 설정 확인
Write-Host "`n[INFO] PATH 설정 확인 중..." -ForegroundColor Yellow

$pathEntries = $env:PATH -split ';'
$anacondaInPath = $false
$condaInPath = $false

foreach ($entry in $pathEntries) {
    if ($entry -like "*Anaconda3*" -or $entry -like "*anaconda3*") {
        $anacondaInPath = $true
        Write-Host "[OK] PATH에 Anaconda 경로 있음: $entry" -ForegroundColor Green
    }
    if ($entry -like "*conda*") {
        $condaInPath = $true
        Write-Host "[OK] PATH에 Conda 경로 있음: $entry" -ForegroundColor Green
    }
}

if (-not $anacondaInPath) {
    Write-Host "[WARNING] PATH에 Anaconda 경로가 없습니다." -ForegroundColor Yellow
}

if (-not $condaInPath) {
    Write-Host "[WARNING] PATH에 Conda 경로가 없습니다." -ForegroundColor Yellow
}

# Conda 명령어 테스트
Write-Host "`n[INFO] Conda 명령어 테스트 중..." -ForegroundColor Yellow

try {
    $condaVersion = & $condaExe --version 2>&1
    Write-Host "[OK] Conda 명령어 실행 성공: $condaVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Conda 명령어 실행 실패" -ForegroundColor Red
}

# 환경 목록 확인
Write-Host "`n[INFO] Conda 환경 목록 확인 중..." -ForegroundColor Yellow

try {
    $envList = & $condaExe env list 2>&1
    Write-Host "[OK] Conda 환경 목록:" -ForegroundColor Green
    Write-Host $envList -ForegroundColor White
} catch {
    Write-Host "[WARNING] Conda 환경 목록 확인 실패" -ForegroundColor Yellow
}

# PATH 추가 스크립트 생성
Write-Host "`n[INFO] PATH 추가 스크립트 생성 중..." -ForegroundColor Yellow

$addPathScript = @'
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
'@

$addPathScript | Out-File -FilePath "add_anaconda_to_path.bat" -Encoding ASCII
Write-Host "[OK] PATH 추가 스크립트 생성 완료: add_anaconda_to_path.bat" -ForegroundColor Green

# Anaconda 초기화 스크립트 생성
Write-Host "`n[INFO] Anaconda 초기화 스크립트 생성 중..." -ForegroundColor Yellow

$initScript = @'
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
'@

$initScript | Out-File -FilePath "init_anaconda.bat" -Encoding ASCII
Write-Host "[OK] Anaconda 초기화 스크립트 생성 완료: init_anaconda.bat" -ForegroundColor Green

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "확인 완료" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n생성된 스크립트:" -ForegroundColor Yellow
Write-Host "  1. add_anaconda_to_path.bat - PATH 임시 추가" -ForegroundColor White
Write-Host "  2. init_anaconda.bat - Anaconda 초기화" -ForegroundColor White

Write-Host "`n권장 작업 순서:" -ForegroundColor Magenta
Write-Host "  1. init_anaconda.bat 실행 (관리자 권한 권장)" -ForegroundColor White
Write-Host "  2. 새 명령 프롬프트 열기" -ForegroundColor White
Write-Host "  3. conda --version 명령 테스트" -ForegroundColor White
Write-Host "  4. conda activate base 로 기본 환경 활성화" -ForegroundColor White
Write-Host "  5. Creon 환경 생성: conda create -n creon python=3.8" -ForegroundColor White

Write-Host "`n참고: Anaconda 초기화 후에는 새 터미널을 열어야 변경사항이 적용됩니다." -ForegroundColor Yellow

pause