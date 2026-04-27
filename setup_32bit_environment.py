#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
32비트 Python 환경 설정 스크립트
기존에 설치된 32비트 Python을 사용하거나 수동 설치 안내
"""

import os
import sys
import subprocess
import shutil

def find_32bit_python():
    """32비트 Python 찾기"""
    possible_paths = [
        r"C:\Python38-32\python.exe",
        r"C:\Python38\python.exe",
        r"C:\Program Files (x86)\Python38-32\python.exe",
        r"C:\Program Files\Python38\python.exe",
        r"C:\Users\{}\AppData\Local\Programs\Python\Python38-32\python.exe".format(os.getenv("USERNAME")),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            # 아키텍처 확인
            try:
                result = subprocess.run(
                    [path, "-c", "import struct; print(struct.calcsize('P')*8)"],
                    capture_output=True, text=True, check=True
                )
                arch = int(result.stdout.strip())
                if arch == 32:
                    print(f"[OK] 32비트 Python 발견: {path}")
                    return path
                else:
                    print(f"[INFO] {path}는 {arch}비트입니다.")
            except:
                continue
    
    return None

def check_current_python():
    """현재 Python 아키텍처 확인"""
    try:
        import struct
        arch = struct.calcsize('P') * 8
        print(f"현재 Python 아키텍처: {arch}비트")
        return arch
    except:
        print("[ERROR] Python 아키텍처 확인 실패")
        return None

def create_32bit_virtualenv(python_32bit_path):
    """32비트 Python으로 가상환경 생성"""
    venv_name = "venv_creon_32bit"
    
    print(f"\n[INFO] 32비트 가상환경 생성 중: {venv_name}")
    print(f"  사용 Python: {python_32bit_path}")
    
    # 기존 가상환경 확인
    if os.path.exists(venv_name):
        print(f"  [INFO] 기존 가상환경이 존재합니다: {venv_name}")
        choice = input("  재생성하시겠습니까? (y/n): ").lower()
        if choice == 'y':
            shutil.rmtree(venv_name, ignore_errors=True)
            print("  [OK] 기존 가상환경 삭제 완료")
        else:
            print("  [INFO] 기존 가상환경을 사용합니다.")
            return venv_name
    
    # 가상환경 생성
    try:
        subprocess.run([python_32bit_path, "-m", "venv", venv_name], check=True)
        print("  [OK] 가상환경 생성 완료")
        return venv_name
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] 가상환경 생성 실패: {e}")
        return None

def install_packages(venv_name):
    """필수 패키지 설치"""
    print(f"\n[INFO] 필수 패키지 설치 중...")
    
    # 가상환경 pip 경로
    if sys.platform == "win32":
        pip_path = os.path.join(venv_name, "Scripts", "pip.exe")
        activate_path = os.path.join(venv_name, "Scripts", "activate.bat")
    else:
        pip_path = os.path.join(venv_name, "bin", "pip")
        activate_path = os.path.join(venv_name, "bin", "activate")
    
    # 패키지 목록
    packages = [
        "pywin32",      # Windows API
        "pandas",       # 데이터 분석
        "numpy",        # 수치 계산
        "pyyaml",       # YAML 설정
        "sqlalchemy",   # 데이터베이스 ORM
        "pymysql",      # MySQL/MariaDB
        "psycopg2-binary", # PostgreSQL
    ]
    
    for package in packages:
        print(f"  - {package} 설치 중...")
        try:
            subprocess.run([pip_path, "install", package, "--quiet"], check=False)
            print(f"    [OK] {package} 설치 완료")
        except:
            print(f"    [WARNING] {package} 설치 실패")
    
    # requirements.txt 설치
    if os.path.exists("requirements.txt"):
        print("  - requirements.txt 설치 중...")
        try:
            subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=False)
            print("    [OK] requirements.txt 설치 완료")
        except:
            print("    [WARNING] requirements.txt 설치 실패")
    else:
        print("  [INFO] requirements.txt 파일이 없습니다.")
    
    print("  [OK] 패키지 설치 완료")
    return activate_path

def create_activation_script(venv_name, python_32bit_path):
    """활성화 스크립트 생성"""
    print(f"\n[INFO] 활성화 스크립트 생성 중...")
    
    script_content = f'''@echo off
chcp 65001 >nul
REM Creon DataReader 32비트 환경 활성화 스크립트

echo ========================================
echo Creon DataReader 32비트 환경 활성화
echo ========================================

echo [INFO] 32비트 Python: {python_32bit_path}

REM 가상환경 활성화
call {venv_name}\\Scripts\\activate.bat

if %errorlevel% equ 0 (
    echo [OK] 환경 활성화 완료
    
    REM 아키텍처 확인
    python -c "import struct; print('가상환경 아키텍처:', struct.calcsize('P')*8, '비트')"
    
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

pause
'''
    
    script_file = "activate_creon_32bit.bat"
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(script_content)
    
    print(f"  [OK] 활성화 스크립트 생성 완료: {script_file}")
    return script_file

def test_environment(venv_name):
    """환경 테스트"""
    print(f"\n[INFO] 환경 테스트 중...")
    
    # 가상환경 Python 경로
    if sys.platform == "win32":
        python_path = os.path.join(venv_name, "Scripts", "python.exe")
    else:
        python_path = os.path.join(venv_name, "bin", "python")
    
    # 아키텍처 확인
    try:
        result = subprocess.run(
            [python_path, "-c", "import struct; print(struct.calcsize('P')*8)"],
            capture_output=True, text=True, check=True
        )
        arch = int(result.stdout.strip())
        print(f"  가상환경 아키텍처: {arch}비트")
        
        if arch == 32:
            print("  [OK] 32비트 가상환경 확인 완료")
            return True
        else:
            print(f"  [WARNING] 가상환경이 32비트가 아닙니다: {arch}비트")
            return False
    except Exception as e:
        print(f"  [ERROR] 환경 테스트 실패: {e}")
        return False

def provide_installation_guide():
    """32비트 Python 설치 안내"""
    print("\n" + "=" * 60)
    print("32비트 Python 설치 안내")
    print("=" * 60)
    
    print("\nCreon API를 사용하려면 32비트 Python이 필요합니다.")
    print("\n설치 방법:")
    print("1. Python 3.8.10 32비트 다운로드:")
    print("   https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe")
    print("\n2. 설치 시 주의사항:")
    print("   - 설치 경로: C:\\Python38-32 (권장)")
    print("   - 'Add Python to PATH' 체크")
    print("   - 'Install for all users' 선택")
    print("\n3. 설치 후 이 스크립트를 다시 실행하세요.")
    
    print("\n또는 다음 명령으로 다운로드:")
    print("powershell -Command \"Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.8.10/python-3.8.10.exe' -OutFile 'python-3.8.10-32bit.exe'\"")
    
    print("\n" + "=" * 60)

def main():
    """메인 함수"""
    print("=" * 60)
    print("Creon DataReader 32비트 환경 설정")
    print("=" * 60)
    
    # 현재 Python 아키텍처 확인
    print("\n[1/5] 현재 Python 아키텍처 확인 중...")
    current_arch = check_current_python()
    
    if current_arch == 32:
        print("[OK] 현재 Python이 32비트입니다.")
        python_32bit_path = sys.executable
    else:
        print("[INFO] 32비트 Python 검색 중...")
        python_32bit_path = find_32bit_python()
    
    # 32비트 Python이 없으면 설치 안내
    if not python_32bit_path:
        print("\n[ERROR] 32비트 Python을 찾을 수 없습니다.")
        provide_installation_guide()
        return 1
    
    # 32비트 가상환경 생성
    print("\n[2/5] 32비트 가상환경 생성 중...")
    venv_name = create_32bit_virtualenv(python_32bit_path)
    if not venv_name:
        print("[ERROR] 가상환경 생성 실패")
        return 1
    
    # 패키지 설치
    print("\n[3/5] 필수 패키지 설치 중...")
    activate_path = install_packages(venv_name)
    
    # 활성화 스크립트 생성
    print("\n[4/5] 활성화 스크립트 생성 중...")
    script_file = create_activation_script(venv_name, python_32bit_path)
    
    # 환경 테스트
    print("\n[5/5] 환경 테스트 중...")
    test_passed = test_environment(venv_name)
    
    print("\n" + "=" * 60)
    print("32비트 환경 설정 완료!")
    print("=" * 60)
    
    print(f"\n생성된 환경:")
    print(f"  가상환경: {venv_name}")
    print(f"  Python: {python_32bit_path}")
    
    print(f"\n사용 방법:")
    print(f"  1. 환경 활성화: {script_file}")
    print(f"  2. Creon Plus 프로그램 실행 및 로그인")
    print(f"  3. Creon API 테스트: python test_creon_api.py")
    print(f"  4. 데이터베이스 테스트: python test_database_simple.py")
    
    if not test_passed:
        print(f"\n[WARNING] 환경 테스트에 문제가 있습니다.")
        print(f"  가상환경이 32비트 Python을 사용하는지 확인하세요.")
    
    print(f"\n참고: Creon API 테스트를 위해 Creon Plus가 실행 중이어야 합니다.")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n[INFO] 작업이 취소되었습니다.")
        sys.exit(1)