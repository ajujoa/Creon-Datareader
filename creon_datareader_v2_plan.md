# Creon DataReader v2.0 개발 계획

## 1. 개요
기존 Creon-Datareader 프로그램을 개선한 v2.0 버전 개발 계획

## 2. 주요 개선사항

### 2.1 파일 구조 개선
- 새로 생성할 파일 앞에 "creon_" 접두어 추가
- 기존 파일과 구분하여 관리 용이성 향상

### 2.2 저장 방식 다중화
- **파일 저장**: JSON 형식 유지
- **데이터베이스 저장**: SQLite/PostgreSQL 지원
- **이중 저장 옵션**: 파일과 DB 동시 저장 가능

### 2.3 데이터 구조 설계
- **일봉 데이터**: 별도 테이블/파일 구조
- **분봉 데이터**: 1분, 5분, 30분, 60분 등 다양한 간격 지원
- **계층적 구조**: 종목 → 차트타입 → 데이터

### 2.4 이어받기(병합) 기능
- 최근 3일간 데이터 존재 여부 확인
- 누락된 기간만 수집하여 기존 데이터와 병합
- 중복 데이터 방지 및 무결성 유지

## 3. 파일 구조

### 3.1 새 모듈 파일 (creon_ 접두어)
```
creon_api.py          # Creon API 래퍼 (기존 creonAPI.py 대체)
creon_datareader.py   # 메인 데이터 수집 클래스
creon_database.py     # 데이터베이스 관리 클래스
creon_filemanager.py  # 파일 저장 관리 클래스
creon_merger.py       # 데이터 병합(이어받기) 클래스
creon_config.py       # 설정 관리 클래스
creon_utils.py        # 유틸리티 함수
```

### 3.2 데이터 저장 구조
```
data/
├── daily/           # 일봉 데이터
│   ├── json/       # JSON 파일
│   └── csv/        # CSV 파일 (옵션)
├── minute/         # 분봉 데이터
│   ├── 1min/      # 1분봉
│   ├── 5min/      # 5분봉
│   ├── 30min/     # 30분봉
│   └── 60min/     # 60분봉
└── database/       # SQLite 데이터베이스
    └── creon_data.db
```

## 4. 데이터베이스 스키마 설계

### 4.1 메타 테이블
```sql
-- 종목 메타정보
CREATE TABLE stock_metadata (
    stock_code VARCHAR(10) PRIMARY KEY,
    stock_name VARCHAR(100),
    market_type VARCHAR(10),  -- 'KOSPI', 'KOSDAQ'
    listing_date DATE,
    delisting_date DATE NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 데이터 수집 이력
CREATE TABLE collection_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10),
    chart_type VARCHAR(20),  -- 'daily', '1min', '5min', '30min', '60min'
    start_date INTEGER,
    end_date INTEGER,
    data_count INTEGER,
    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);
```

### 4.2 일봉 데이터 테이블
```sql
CREATE TABLE daily_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10),
    date INTEGER,  -- YYYYMMDD
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume BIGINT,
    amount BIGINT,  -- 거래대금
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, date),
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);

-- 인덱스 추가
CREATE INDEX idx_daily_stock_date ON daily_data(stock_code, date);
```

### 4.3 분봉 데이터 테이블
```sql
-- 분봉 데이터 (공통 구조)
CREATE TABLE minute_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10),
    datetime INTEGER,  -- YYYYMMDDHHMM
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume BIGINT,
    amount BIGINT,
    interval_minutes INTEGER,  -- 1, 5, 30, 60
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, datetime, interval_minutes),
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);

-- 인덱스 추가
CREATE INDEX idx_minute_stock_datetime ON minute_data(stock_code, datetime);
CREATE INDEX idx_minute_interval ON minute_data(interval_minutes);
```

## 5. 이어받기(병합) 로직

### 5.1 데이터 존재 여부 확인
```python
def check_existing_data(stock_code, chart_type, interval=None):
    """
    기존 데이터 존재 여부 확인
    Returns: (has_data, latest_date, earliest_date, gap_days)
    """
    # 파일 시스템 확인
    # 데이터베이스 확인
    # 최근 3일간 데이터 확인
```

### 5.2 병합 알고리즘
1. **데이터 확인**: 최근 3일간 데이터 존재 여부 확인
2. **기간 계산**: 누락된 기간 식별
3. **수집 범위 결정**: 
   - 데이터 없음 → 전체 기간 수집
   - 데이터 있음 → 최신 데이터부터 3일 전까지 수집
4. **데이터 수집**: Creon API로 데이터 가져오기
5. **병합 처리**:
   - 중복 데이터 제거
   - 시간순 정렬
   - 무결성 검증
6. **저장**: 파일과 DB에 저장

### 5.3 병합 예시
```
기존 데이터: 20240101 ~ 20240125
현재 날짜: 20240128

수집 범위: 20240126 ~ 20240128 (3일간)
병합 결과: 20240101 ~ 20240128
```

## 6. 클래스 설계

### 6.1 CreonDataCollector (메인 클래스)
```python
class CreonDataCollector:
    def __init__(self, config):
        self.api = CreonAPI()
        self.db = CreonDatabase()
        self.file_mgr = CreonFileManager()
        self.merger = CreonDataMerger()
        
    def collect_data(self, stock_codes, chart_type, interval=None, 
                    start_date=None, end_date=None, merge=True):
        # 데이터 수집 메인 로직
        pass
        
    def collect_with_merge(self, stock_code, chart_type, interval=None):
        # 이어받기 기능 포함한 수집
        pass
```

### 6.2 CreonDataMerger (병합 클래스)
```python
class CreonDataMerger:
    def __init__(self, db_manager, file_manager):
        self.db = db_manager
        self.files = file_manager
        
    def find_data_gaps(self, stock_code, chart_type, interval=None):
        # 데이터 간격 찾기
        pass
        
    def merge_data(self, existing_data, new_data):
        # 데이터 병합
        pass
        
    def validate_merge(self, merged_data):
        # 병합 데이터 검증
        pass
```

## 7. 설정 관리

### 7.1 설정 파일 (config.yaml)
```yaml
storage:
  file:
    enabled: true
    format: json  # json, csv, parquet
    base_path: ./data
  database:
    enabled: true
    type: sqlite  # sqlite, postgresql
    path: ./data/database/creon_data.db
    
collection:
  default_count: 1000
  max_retries: 3
  retry_delay: 1.0
  request_delay: 0.25
  
merge:
  enabled: true
  lookback_days: 3
  auto_merge: true
  
filters:
  price_min: 10000
  price_max: 30000
  exclude_etf: true
  exclude_keywords:
    - KODEX
    - TIGER
    - ACE
    - 액티브
```

## 8. 개발 일정

### Phase 1: 기본 구조 구현 (1주)
- 파일 구조 설계 및 생성
- 기본 클래스 틀 구현
- 설정 관리 시스템

### Phase 2: 데이터베이스 구현 (1주)
- SQLite 데이터베이스 설계
- CRUD 작업 구현
- 인덱스 및 최적화

### Phase 3: 이어받기 로직 (1주)
- 데이터 존재 여부 확인
- 병합 알고리즘 구현
- 검증 로직 추가

### Phase 4: 통합 및 테스트 (1주)
- 전체 시스템 통합
- 단위 테스트 작성
- 성능 테스트 및 최적화

## 9. 테스트 계획

### 9.1 단위 테스트
- API 연결 테스트
- 데이터 수집 테스트
- 병합 로직 테스트
- 파일 저장 테스트
- DB 저장 테스트

### 9.2 통합 테스트
- 종목별 데이터 수집 테스트
- 이어받기 기능 테스트
- 대량 데이터 처리 테스트
- 에러 복구 테스트

### 9.3 성능 테스트
- 데이터 수집 속도
- 병합 처리 속도
- 메모리 사용량
- 디스크 I/O

## 10. 향후 확장성

### 10.1 지원 가능한 추가 기능
- 실시간 데이터 스트리밍
- 기술적 지표 계산
- 백테스팅 엔진 통합
- 웹 대시보드
- API 서버 구축

### 10.2 클라우드 지원
- AWS S3 파일 저장
- RDS 데이터베이스
- Lambda 함수로 스케줄링
- Docker 컨테이너화

---

**시작일**: 2026년 4월 22일  
**예상 완료일**: 2026년 5월 20일  
**담당자**: 개발팀