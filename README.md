# Creon DataReader MH

대신증권 Creon Plus API를 사용하여 주가 차트 데이터(분봉/일봉)를 수집하는 프로그램입니다.
기반: [gyusu/Creon-Datareader](https://github.com/gyusu/Creon-Datareader)

## 실행 환경

| 요구사항 | 내용 |
|----------|------|
| OS | Windows 10/11 |
| Python | 32bit Python 3.8+ (**64bit 불가**) |
| Creon Plus | HTS 설치 및 로그인 필수 |
| 패키지 | `requirements.txt` 참조 |

## 빠른 시작

```bash
# 1. Creon Plus 실행 + 로그인 (필수)
# 2. 가상환경 활성화
venv_creon32\Scripts\activate

# 3. 데이터 수집 실행 (방법 A)
python creon_chart_down_20260605.py

# 또는 (방법 B) .bat 파일 더블클릭
creon_chart_down_20260605.bat
```

데이터는 `json_data/YYYYMMDD_HHMMSS/` 디렉토리에 JSON 파일로 저장됩니다.
로그는 `logs/creon_chart_YYYYMMDD_HHMMSS.log` 파일에 DEBUG 레벨로 상세 기록됩니다.

## 프로젝트 구조

```
Creon-Datareader/
├── creon_chart_down_20260605.py           ← 메인 실행 파일
├── creon_chart_down_20260605.bat           ← 더블클릭 실행
├── creonAPI.py                            ← Creon Plus API 래퍼 (win32com)
├── decorators.py                          ← 데코레이터
├── utils.py                               ← 유틸리티 (장 시간 체크 등)
├── requirements.txt                       ← Python 패키지 의존성
├── json_data/                             ← 수집 데이터 저장 (자동 생성)
├── old/                                   ← 이전 버전 파일 보관
└── README.md                              ← 본 문서
```

## 설정 변경

실행 전 `creon_chart_down_20260605.py` 파일 내 `self.settings` 수정:

```python
self.settings = {
    'tick_unit': '일봉',     # '일봉' 또는 '분봉'
    'tick_range': 5,         # 분봉 간격 (1,3,5,10,15,30,60)
    'count': 2000,           # 요청할 데이터 개수
    'ohlcv_only': True,      # True: OHLCV만, False: 추가지표 포함
    'price_min': 1000,       # 최소 가격 필터
    'price_max': 1000000,    # 최대 가격 필터
    'max_stocks': 0          # 최대 종목 수 (0=전체)
}
```

## Creon API 시세 조회 제한

| 항목 | 제한 |
|------|------|
| **시세 조회** | 15초당 최대 60건 |
| **초과 시** | 첫 요청으로부터 15초 경과까지 내부 대기 (blocking) |

프로그램은 BlockRequest 간 `0.3초`, 종목 간 `0.3초` delay로 제한을 준수합니다.
→ 15초당 최대 50건 (제한 60건 대비 여유 10건)

## 분봉 데이터 서버 보유 기간

| 분봉 | 서버 보유 기간 | 하루 생성 | 최대 조회 |
|------|--------------|----------|----------|
| 1분봉 | 2년 (~500일) | 390개 | ~195,000개 |
| 3분봉 | 2년 (1분봉 기반) | 130개 | ~65,000개 |
| 5분봉 | 5년 (~1,250일) | 78개 | ~97,500개 |
| 10분봉 | 5년 | 39개 | ~48,750개 |
| 15분봉 | 5년 | 26개 | ~32,500개 |
| 30분봉 | 5년 | 13개 | ~16,250개 |
| 60분봉 | 5년 | 6.5개 | ~8,125개 |
| 틱 | 20일 | - | - |

※ 3분봉은 1분봉에서 생성되므로 보유 기간 동일 (2년).
※ 5의 배수(5,10,15,30,60)만 진짜 N분봉 데이터.
※ 출처: Creon Plus 담당자 확인 (2026-06-13)

## 의존성 패키지

```bash
pip install -r requirements.txt
```

주요 패키지: `pywin32`, `pandas`, `numpy`, `pykrx`, `exchange-calendars`, `finance-datareader`, `tqdm`, `matplotlib`, `openpyxl`

## 이전 버전 문서 (참고용)

`old/` 폴더에 다음 v2.0 설계 문서들이 보관되어 있습니다:
- `CREON_DATAREADER_V2_SUMMARY.md` - v2.0 개발 완료 요약
- `CREON_DATAREADER_V2_MENU_SUMMARY.md` - v2.0 메뉴 시스템
- `creon_database_schema.md` - v2.0 DB 스키마 설계
- `creon_datareader_v2_plan.md` - v2.0 개발 계획
- `creon_environment_summary.md` - v2.0 환경설정 요약
- `creon_merge_logic.md` - v2.0 병합 로직 설계

## 주의사항

- Creon Plus API는 **Windows 32bit Python**에서만 동작
- Creon Plus HTS가 **실행 중 + 로그인** 상태여야 함
- 장중에는 데이터 수집을 권장하지 않음 (수정주가 변동 가능성)
- 수정주가 기반이므로 과거 데이터 재수집 시 값이 달라질 수 있음

## 업데이트 이력

| 날짜 | 내용 |
|------|------|
| 2026-06-04 | 문서 통합, API 제한 문서화, delay 0.3초 통일, 프로젝트 정리 |
