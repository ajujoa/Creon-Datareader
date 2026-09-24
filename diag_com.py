# coding=utf-8
"""Creon Plus COM 연결 진단"""
import sys
import os
import struct

print("=== Creon COM 진단 ===")
print(f"Python: {sys.version}")
print(f"Arch: {struct.calcsize('P')*8}bit")

# 1. pywin32 확인
try:
    import win32com
    import win32com.client
    print(f"pywin32: installed ({win32com.__file__})")
except:
    print("pywin32: NOT INSTALLED")
    sys.exit(1)


# 2. COM 객체 생성 테스트
print("\n--- COM Dispatch 테스트 ---")
com_tests = [
    ('CpUtil.CpCybos', 'Cybos 연결상태'),
    ('CpUtil.CpCodeMgr', '종목코드 관리자'),
    ('CpSysDib.StockChart', '차트 데이터'),
    ('DsCbo1.StockMst', '종목 마스터'),
]

for prog_id, desc in com_tests:
    try:
        obj = win32com.client.Dispatch(prog_id)
        print(f"  OK  {prog_id} ({desc})")
    except Exception as e:
        print(f"  FAIL {prog_id} ({desc}): {e}")

# 3. Cybos 연결 상세 확인
print("\n--- Cybos 상세 ---")
try:
    obj = win32com.client.Dispatch('CpUtil.CpCybos')
    print(f"  IsConnect: {obj.IsConnect}")
    print(f"  ServerType: {obj.ServerType}")
    print(f"  LimitRequestRemainTime: {obj.LimitRequestRemainTime}")
except Exception as e:
    print(f"  Error: {e}")

# 4. 종목코드 테스트
print("\n--- 종목코드 테스트 ---")
try:
    obj = win32com.client.Dispatch('CpUtil.CpCodeMgr')
    codes = obj.GetStockListByMarket(1)
    print(f"  KOSPI 종목수: {len(codes)}")
    if codes:
        print(f"  샘플: {codes[0]} -> {obj.CodeToName(codes[0])}")
except Exception as e:
    print(f"  Error: {e}")

# 5. DCOM 권한 확인
print("\n--- 레지스트리 확인 ---")
import winreg
for cls_id in ['CpUtil.CpCybos', 'CpUtil.CpCodeMgr']:
    try:
        key = winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, f"{cls_id}\\CLSID")
        clsid = winreg.QueryValue(key, '')
        winreg.CloseKey(key)
        key2 = winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, f"CLSID\\{clsid}")
        print(f"  {cls_id} -> CLSID: {clsid}")
        winreg.CloseKey(key2)
    except Exception as e:
        print(f"  {cls_id} -> NOT FOUND: {e}")

print("\n진단 완료")
