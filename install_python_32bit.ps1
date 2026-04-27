# 32비트 Python 3.8.10 설치 스크립트

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "32비트 Python 3.8.10 설치" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$pythonInstaller = "python-3.8.10-32bit.exe"
$installPath = "C:\Python38-32bit"

Write-Host "[INFO] 설치 프로그램 확인 중..." -ForegroundColor Yellow
if (-not (Test-Path $pythonInstaller)) {
    Write-Host "[ERROR] 설치 프로그램을 찾을 수 없습니다: $pythonInstaller" -ForegroundColor Red
    Read-Host "계속하려면 Enter 키를 누르세요"
    exit 1
}

Write-Host "[INFO] 설치 경로: $installPath" -ForegroundColor White
Write-Host "[INFO] 설치 중... (잠시 기다려주세요)" -ForegroundColor Yellow

# Python 3.8.10 자동 설치 옵션:
# /quiet - 자동 설치
# InstallAllUsers=1 - 모든 사용자용 설치
# PrependPath=1 - PATH에 추가
# TargetDir=설치경로 - 설치 경로 지정
# AssociateFiles=0 - 파일 연결 안함
# Shortcuts=0 - 바로가기 생성 안함

$installArgs = @(
    "/quiet",
    "InstallAllUsers=1",
    "PrependPath=1",
    "TargetDir=`"$installPath`"",
    "AssociateFiles=0",
    "Shortcuts=0"
)

$process = Start-Process -FilePath $pythonInstaller -ArgumentList $installArgs -Wait -PassThru

if ($process.ExitCode -eq 0) {
    Write-Host "[OK] Python 3.8.10 32비트 설치 완료" -ForegroundColor Green
    Write-Host "" -ForegroundColor White
    Write-Host "설치 경로: $installPath" -ForegroundColor White
    Write-Host "" -ForegroundColor White
    Write-Host "다음 단계:" -ForegroundColor Magenta
    Write-Host "1. 새 명령 프롬프트 열기" -ForegroundColor White
    Write-Host "2. python --version 명령으로 확인" -ForegroundColor White
    Write-Host "3. python -c `"import struct; print(struct.calcsize('P') * 8)`" 로 아키텍처 확인" -ForegroundColor White
} else {
    Write-Host "[ERROR] Python 설치 실패 (오류 코드: $($process.ExitCode))" -ForegroundColor Red
}

Write-Host "" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "설치 완료" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 설치 후 PATH 확인
Write-Host "" -ForegroundColor White
Write-Host "[INFO] PATH 확인:" -ForegroundColor Yellow
$env:PATH -split ';' | Where-Object { $_ -like "*python38*" } | ForEach-Object {
    Write-Host "  $_" -ForegroundColor White
}

Write-Host "" -ForegroundColor White
Write-Host "Creon DataReader를 위한 환경 설정:" -ForegroundColor Magenta
Write-Host "1. 가상환경 생성: python -m venv venv_creon32" -ForegroundColor White
Write-Host "2. 가상환경 활성화: venv_creon32\Scripts\activate" -ForegroundColor White
Write-Host "3. 필수 패키지 설치: pip install pywin32 pandas numpy pyyaml sqlalchemy" -ForegroundColor White

Read-Host "계속하려면 Enter 키를 누르세요"