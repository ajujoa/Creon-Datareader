# coding=utf-8
"""삼성전자 데이터 범위 테스트 - 원본 Creon-Datareader 패턴"""
import win32com.client
import time

# Creon 연결 확인
obj = win32com.client.Dispatch('CpUtil.CpCybos')
if obj.IsConnect == 0:
    print("Creon Plus 연결 안됨 (IsConnect=0)")
    exit(1)
print("Creon 연결 OK")

code = 'A005930'
chart = win32com.client.Dispatch('CpSysDib.StockChart')

def fetch_chart(setup_fn, col_names, max_count):
    """원본 패턴: Continue 플래그 끝날때까지 BlockRequest 반복"""
    setup_fn()
    data = {col: [] for col in col_names}
    got = 0
    while max_count > got:
        chart.BlockRequest()
        st = chart.GetDibStatus()
        if st != 0:
            print(f"  통신오류 status={st}")
            break
        batch = chart.GetHeaderValue(3)
        batch = min(batch, max_count - got)
        for i in range(batch):
            for ci, col in enumerate(col_names):
                data[col].append(chart.GetDataValue(ci, i))
        if len(data[col_names[0]]) == 0:
            return None, 0
        got += batch
        if not chart.Continue:
            break
        time.sleep(0.25)
    return data, got

# ── 일봉 ──
print("\n=== 일봉 ===")
for count in [2000, 5000, 10000, 20000]:
    def setup():
        chart.SetInputValue(0, code)
        chart.SetInputValue(1, ord('2'))
        chart.SetInputValue(4, count)
        chart.SetInputValue(5, [0, 2, 3, 4, 5, 8])
        chart.SetInputValue(6, ord('D'))
        chart.SetInputValue(9, ord('1'))
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    d, n = fetch_chart(setup, cols, count)
    if d and n > 0:
        print(f"  요청={count:,}, 실제={n:,}, 최신={d['date'][0]}, 최고과거={d['date'][-1]}", end="")
        if n < count:
            print(f" ← 서버 최대")
        else:
            print()
    else:
        print(f"  요청={count:,}, 데이터없음")
        break

# ── 1분봉 ──
print("\n=== 1분봉 ===")
for count in [5000, 20000, 50000, 100000, 195000, 200000]:
    def setup():
        chart.SetInputValue(0, code)
        chart.SetInputValue(1, ord('2'))
        chart.SetInputValue(4, count)
        chart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])
        chart.SetInputValue(6, ord('m'))
        chart.SetInputValue(7, 1)
        chart.SetInputValue(9, ord('1'))
    cols = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']
    d, n = fetch_chart(setup, cols, count)
    if d and n > 0:
        # date+time 합치기
        dt = f"{d['date'][0]}{d['time'][0]:04d} / {d['date'][-1]}{d['time'][-1]:04d}"
        print(f"  요청={count:,}, 실제={n:,}, 범위={dt}", end="")
        if n < count:
            print(f" ← 서버 최대")
        else:
            print()
    else:
        print(f"  요청={count:,}, 데이터없음")
        break

# ── 3분봉 ──
print("\n=== 3분봉 ===")
for count in [5000, 20000, 50000, 65000, 70000]:
    def setup():
        chart.SetInputValue(0, code)
        chart.SetInputValue(1, ord('2'))
        chart.SetInputValue(4, count)
        chart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])
        chart.SetInputValue(6, ord('m'))
        chart.SetInputValue(7, 3)
        chart.SetInputValue(9, ord('1'))
    cols = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']
    d, n = fetch_chart(setup, cols, count)
    if d and n > 0:
        # date+time 합치기
        dt = f"{d['date'][0]}{d['time'][0]:04d} / {d['date'][-1]}{d['time'][-1]:04d}"
        print(f"  요청={count:,}, 실제={n:,}, 범위={dt}", end="")
        if n < count:
            print(f" ← 서버 최대")
        else:
            print()
    else:
        print(f"  요청={count:,}, 데이터없음")
        break

print("\n완료")
