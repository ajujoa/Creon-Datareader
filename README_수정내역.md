# Creon Datareader — 수정 내역

> 프로그램: `creon_chart_down_20260605.bat` + `creon_chart_down_20260605.py`  
> Win11 경로: `C:\code\Creon-Datareader\`

---

## 1. CLI 한글 깨짐 수정

**원인:** 배치파일이 UTF-8로 저장됨. Windows cmd.exe는 CP949(EUC-KR)를 사용하므로 한글이 깨져 보임.

**해결:** 배치파일을 **CP949 인코딩 + CRLF**로 저장.
- UTF-8 → CP949 변환: `iconv -f utf-8 -t cp949`
- Python에서 직접 생성 시: `line.encode('cp949')` 후 `b'\r\n'` 추가
- 수정툴(`patch`/`write_file`) 사용 시 UTF-8로 저장되므로 주의. Python 스크립트로 CP949 직접 쓰는 것이 안정적.

**관련 파일:** `creon_chart_down_20260605.bat`

---

## 2. Creon Plus 중복 실행 방지

**원인:** 배치파일이 AHK 스크립트를 항상 실행 → CreonPlus가 이미 떠있어도 중복 실행 시도.

**1차 시도 (실패):** `tasklist`로 `coStarter.exe` 체크. 문제는 `coStarter.exe`는 로그인 후 종료됨.  
**2차 시도 (실패):** `dicagyw.exe` 체크. 프로세스명이 정확하지 않음.  
**최종:** **AHK 호출 자체를 배치파일에서 제거.** Python `creon_launcher.ensure_connected()`가 Creon 연결 상태를 `CpCybos.IsConnect` COM API로 정확히 체크, 미연결 시에만 실행+로그인.

**변경사항:**
- 배치파일에서 `call AHK\run_creon_login.bat` 제거
- 단계 표시 `[1/3][2/3][3/3]` → `[1/2][2/2]`로 축소
- Python `main()`의 `time.sleep(30)` (AHK 대기용) 제거

---

## 3. 로그인 ID/PW 입력 안 됨 (Creon Plus 업데이트 대응)

**원인:** Creon Plus 업데이트 후 로그인 다이얼로그에 **Edit 컨트롤이 2개에서 3개로 증가**.

**다이얼로그 구조 (덤프 결과):**
```
class=#32770 text='CREON Starter'
  Afx:... id=999  (로고/이미지)
  Edit id=6977    ← ID 필드
  Edit id=6978    ← PW 필드
  Edit id=6979    ← 추가 필드 (cert PW?), visible=False
  Afx:... id=998  (우측 패널)
  Afx:... id=997  (기타), visible=False
```

### 시도한 접근법:

| 방법 | 결과 | 이유 |
|------|------|------|
| `FindWindowEx("Edit")` + `WM_SETTEXT` | ID만 입력됨 | utf-16-le 인코딩 문제? |
| `FindWindowEx("Edit")` + `WM_CHAR` | 입력 안 됨 | 포커스 없이 메시지만 보내서 |
| `SetFocus` + `WM_CHAR` | 입력 안 됨 | 여전히 컨트롤이 WM_CHAR 무시 |
| `SendKeys` COM (WScript.Shell) | **크래시** | 콘솔 Python에서 COM SendKeys 불안정 |
| **`EnumChildWindows` + 컨트롤 ID 직접 지정 + `EM_SETSEL` + `EM_REPLACESEL`** | ✅ **성공** | 6977=ID, 6978=PW로 직접 지정, 전체선택 후 덮어쓰기 |

**최종 해결:** `EnumChildWindows`로 컨트롤 ID(6977, 6978, 6979)를 직접 찾고 `EM_SETSEL(전체선택)` + `EM_REPLACESEL(덮어쓰기)`로 입력.

**관련 파일:** `creon_launcher.py` — `_fill_login_dialog()` 함수

---

## 4. 속도 최적화

| 문제 | 영향 | 해결 |
|------|------|------|
| `time.sleep(0.3)` 2중 (RequestMT내부 + for문) | 종목당 0.6s → 2,894종목 = 29분 손실 | 중복 0.3s 제거 |
| 모든 종목 `SELECT MAX(datetime)` DB 쿼리 | Tailscale 경유 0.5~2s = 24~96분 손실 | JSON의 `from_date`로 대체, 쿼리 생략 |

---

## 5. 로깅 통일

**문제:** `creon_launcher.py`가 `logging.getLogger(__name__)`로 별도 로거 사용 → 메시지가 메인 로그 파일에 안 남음.

**해결:** `logging.getLogger('creon_chart')`로 통일 (메인 스크립트와 동일한 로거).

---

## 6. 핵심 파일 목록

| 파일 | 역할 |
|------|------|
| `creon_chart_down_20260605.bat` | 실행 배치파일 (CP949, CRLF) |
| `creon_chart_down_20260605.py` | 메인 Python 스크립트 |
| `creon_launcher.py` | Creon Plus 실행/로그인 모듈 |
| `creonAPI.py` | Creon API (차트 데이터 요청) |
| `creon_config.ini` | 설정 파일 (DB 접속정보 등) |
| `AHK/creon_auto_login.ahk` | AHK 로그인 스크립트 (현재 미사용) |

---

## 7. 주의사항

- **배치파일 인코딩:** 반드시 **CP949 + CRLF**. Linux에서 수정 시 Python으로 CP949 직접 쓰기 권장.
- **Creon Plus 업데이트:** 로그인 다이얼로그 구조가 바뀌면 `_fill_login_dialog()` 수정 필요. 최신 구조는 로그 파일의 `--- dialog children ---` 섹션에서 확인.
- **SSH 제약:** Win32 GUI 함수(`FindWindow`, `EnumWindows`)는 SSH 세션(session 0)에서 사용자 데스크탑(session 1)의 윈도우를 볼 수 없음. 배치파일 더블클릭으로 실행해야 GUI 접근 가능.

---

## 8. JSON 제거, DB 전환, from_date 버그 수정 (2026-07-13)

**배경:** error.txt에 `utf-8 codec can't decode byte` JSON 파싱 에러 + `Parameter session received as 74356` date 변환 에러 다량 발생.

### JSON 완전 제거
- `save_to_json()` 메서드 삭제 (더 이상 JSON 파일 생성/읽기 안 함)
- 메뉴 선택 제거 (`DB 모드` vs `기존 모드` 선택화면 삭제, 바로 DB 모드)
- `import json` 제거
- `_menu_with_timeout()` 함수 제거
- DB만 단일 진실 공급원으로 사용

### `_build_date_map()` — JSON → DB 기반으로 재작성
```
- A005930.json 읽어서 day_serial → YYYYMMDD 매핑하던 방식 폐기
+ DB SELECT DISTINCT datetime DIV 100, date FROM {table} WHERE date > 0
```
DB에 기존 저장된 date 정보로 매핑 재구성. date_map이 없으면 date=0으로 저장.

### `from_date` 변환 버그 수정
`from_date`가 Creon raw serial(예: 743608618)인데 `// 10000`으로 YYYYMMDD 추출 시도 → 74360 (유효하지 않은 연도).
```
- date_only = from_date // 10000
- prev_session = krx_calendar.previous_session(str(date_only))  # year 74356 폭발
+ ds = from_date // 100                    # day_serial 추출
+ date_ymd = date_map.get(ds)              # day_serial → YYYYMMDD
+ prev_session = krx_calendar.previous_session(str(date_ymd))
```
date_map에 없으면 BY_DATE 모드 skip → 전량 다운로드로 fallback.
