# coding: utf-8
"""Creon으로 A000020 3분봉 최근 데이터의 raw date/time 조회 — day_block 매핑 확정용."""
import sys, time
import win32com.client

def get_recent_3min(code, n=10):
    chart = win32com.client.Dispatch("CpSysDib.StockChart")
    chart.SetInputValue(0, code)          # 종목코드
    chart.SetInputValue(1, ord('m'))      # 분봉
    chart.SetInputValue(2, 3)             # 3분
    chart.SetInputValue(3, n)             # 최근 n개
    chart.SetInputValue(4, 0)             # 0: 요청개수
    chart.SetInputValue(5, 0)             # 0: 수신개수
    chart.SetInputValue(6, ord('D'))      # 수정주가
    chart.SetInputValue(9, 1)             # ohlcv
    chart.BlockRequest()
    cnt = chart.GetHeaderValue(3)
    print(f"받은 봉 수: {cnt}")
    for i in range(cnt):
        d = chart.GetDataValue(0, i)      # date (Creon raw)
        t = chart.GetDataValue(1, i)      # time (HHMM)
        print(f"  i={i} raw_date={d} raw_time={t}  datetime_div10000={int(d) // 10000 if isinstance(d, (int, float)) else '?'}")

if __name__ == "__main__":
    try:
        cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        print("Creon 연결:", cybos.IsConnect)
        if not cybos.IsConnect:
            print("Creon 미연결 — 종료")
            sys.exit(1)
        get_recent_3min("A005930", 12)
    except Exception as e:
        print(f"오류: {e}")
