# Creon API를 위한 32비트 Python 설치 스크립트

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "32비트 Python 설치 스크립트" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "[INFO] Creon API는 32비트 Python이 필요합니다." -ForegroundColor Yellow

# 옵션 1: 공식 Python 웹사이트에서 32비트 Python 설치
Write-Host "`n[OPTION 1] 공식 Python 32비트 설치" -ForegroundColor Green
Write-Host "다음 링크에서 Python 3.8.10 32비트 설치:" -ForegroundColor White
Write-Host "  https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe" -ForegroundColor Blue
Write-Host "또는 Python 3.7.9 32비트:" -ForegroundColor White
Write-Host "  https://www.python.org/ftp/python/3.7.9/python-3.7.9.exe" -ForegroundColor Blue

# 옵션 2: Anaconda에서 32비트 환경 생성 (제한적)
Write-Host "`n[OPTION 2] Anaconda를 통한 32비트 환경 생성" -ForegroundColor Green
Write-Host "Anaconda에서 32비트 Python을 명시적으로 지원하지 않을 수 있습니다." -ForegroundColor Yellow
Write-Host "대신 다음 방법을 시도해보세요:" -ForegroundColor White
Write-Host "  1. Anaconda Prompt 관리자 권한으로 실행" -ForegroundColor White
Write-Host "  2. 다음 명령 실행:" -ForegroundColor White
Write-Host "     conda create -n creon_32bit python=3.8" -ForegroundColor Blue
Write-Host "  3. 생성된 환경이 32비트인지 확인" -ForegroundColor White

# 옵션 3: 가상환경 사용
Write-Host "`n[OPTION 3] venv를 사용한 가상환경" -ForegroundColor Green
Write-Host "32비트 Python이 시스템에 설치된 경우:" -ForegroundColor White
Write-Host "  1. 32비트 Python 설치 (옵션 1 참조)" -ForegroundColor White
Write-Host "  2. 32비트 Python으로 가상환경 생성:" -ForegroundColor White
Write-Host "     C:\Python38-32\python.exe -m venv venv_creon" -ForegroundColor Blue
Write-Host "  3. 가상환경 활성화:" -ForegroundColor White
Write-Host "     .\venv_creon\Scripts\activate" -ForegroundColor Blue

# 현재 시스템 확인
Write-Host "`n[INFO] 현재 시스템 정보:" -ForegroundColor Yellow
$arch = [Environment]::Is64BitOperatingSystem
if ($arch) {
    Write-Host "  운영체제: 64비트" -ForegroundColor White
} else {
    Write-Host "  운영체제: 32비트" -ForegroundColor White
}

$pythonArch = python -c "import struct; print(struct.calcsize('P') * 8)" 2>$null
if ($pythonArch) {
    Write-Host "  현재 Python: $pythonArch비트" -ForegroundColor White
} else {
    Write-Host "  Python이 설치되지 않았거나 PATH에 없습니다." -ForegroundColor Red
}

# 권장 사항
Write-Host "`n[RECOMMENDATION] 권장 설치 방법:" -ForegroundColor Magenta
Write-Host "  1. Python 3.8.10 32비트 설치 (공식 웹사이트)" -ForegroundColor White
Write-Host "  2. 설치 시 'Add Python to PATH' 체크" -ForegroundColor White
Write-Host "  3. 별도 디렉토리에 설치 (예: C:\Python38-32)" -ForegroundColor White
Write-Host "  4. 해당 Python으로 가상환경 생성" -ForegroundColor White

# 설치 스크립트 예시
Write-Host "`n[EXAMPLE] 설치 후 실행할 스크립트 예시:" -ForegroundColor Green
$exampleScript = @'
# 32비트 Python으로 가상환경 생성
C:\Python38-32\python.exe -m venv venv_creon

# 가상환경 활성화
.\venv_creon\Scripts\activate

# 필수 패키지 설치
pip install pywin32 pandas numpy pyyaml sqlalchemy

# Creon DataReader 프로젝트 의존성 설치
pip install -e .
'@

Write-Host $exampleScript -ForegroundColor Gray

Write-Host "`n[NOTE] 참고사항:" -ForegroundColor Yellow
Write-Host "  - Creon Plus 프로그램이 설치되어 있어야 합니다." -ForegroundColor White
Write-Host "  - Creon API는 Windows에서만 동작합니다." -ForegroundColor White
Write-Host "  - 32비트 Python과 32비트 COM 객체가 필요합니다." -ForegroundColor White

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "설치 완료 후 다음 단계:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "1. 32비트 Python 설치 확인" -ForegroundColor White
Write-Host "2. 가상환경 생성 및 활성화" -ForegroundColor White
Write-Host "3. 필수 패키지 설치" -ForegroundColor White
Write-Host "4. creon_main.py 테스트 실행" -ForegroundColor White

pause