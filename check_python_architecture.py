#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 아키텍처 확인 스크립트
"""

import sys
import struct
import platform
import os

print("=" * 60)
print("Python 아키텍처 확인")
print("=" * 60)

print(f"Python 버전: {sys.version}")
print(f"아키텍처: {struct.calcsize('P') * 8}비트")
print(f"플랫폼: {platform.platform()}")
print(f"실행 파일: {sys.executable}")
print(f"실행 파일 크기: {os.path.getsize(sys.executable) if os.path.exists(sys.executable) else 'N/A'} bytes")

# 추가 정보
print(f"\n시스템 정보:")
print(f"  시스템: {platform.system()}")
print(f"  릴리스: {platform.release()}")
print(f"  버전: {platform.version()}")
print(f"  머신: {platform.machine()}")
print(f"  프로세서: {platform.processor()}")

# 환경 변수 확인
print(f"\n관련 환경 변수:")
env_vars = ['PATH', 'PYTHONPATH', 'CONDA_PREFIX', 'CONDA_DEFAULT_ENV']
for var in env_vars:
    value = os.environ.get(var, '설정되지 않음')
    if var == 'PATH':
        # PATH는 너무 길어서 첫 몇 개만 표시
        paths = value.split(os.pathsep)
        print(f"  {var}: {len(paths)}개 경로 (첫 3개: {paths[:3]})")
    else:
        print(f"  {var}: {value}")

print("\n" + "=" * 60)
print("결론:")
arch = struct.calcsize('P') * 8
if arch == 32:
    print("[OK] 32비트 Python입니다. Creon API와 호환됩니다.")
elif arch == 64:
    print("[WARNING] 64비트 Python입니다. Creon API는 32비트 Python이 필요할 수 있습니다.")
else:
    print(f"[UNKNOWN] 알 수 없는 아키텍처: {arch}비트")

print("=" * 60)