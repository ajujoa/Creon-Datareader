# Creon DataReader v2.0 데이터베이스 스키마 설계

## 1. 데이터베이스 선택
- **주 데이터베이스**: SQLite (경량, 파일 기반, 배포 용이)
- **확장 옵션**: PostgreSQL (대규모 데이터, 동시 접속)

## 2. 전체 스키마 구조

### 2.1 메타데이터 테이블
```sql
-- 시스템 설정 테이블
CREATE TABLE system_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value TEXT,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 종목 메타정보 테이블
CREATE TABLE stock_metadata (
    stock_code VARCHAR(10) PRIMARY KEY,
    stock_name VARCHAR(100) NOT NULL,
    market_type VARCHAR(10) NOT NULL,  -- 'KOSPI', 'KOSDAQ', 'KONEX'
    sector_code VARCHAR(10),
    sector_name VARCHAR(50),
    listing_date INTEGER,  -- YYYYMMDD
    delisting_date INTEGER DEFAULT NULL,
    is_etf BOOLEAN DEFAULT FALSE,
    is_etn BOOLEAN DEFAULT FALSE,
    is_delisted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (market_type IN ('KOSPI', 'KOSDAQ', 'KONEX'))
);

-- 데이터 수집 설정 테이블
CREATE TABLE collection_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_name VARCHAR(50) UNIQUE NOT NULL,
    chart_type VARCHAR(20) NOT NULL,  -- 'daily', 'minute'
    interval_minutes INTEGER DEFAULT NULL,  -- NULL for daily, 1/5/30/60 for minute
    data_count INTEGER DEFAULT 1000,
    ohlcv_only BOOLEAN DEFAULT TRUE,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 수집 작업 이력 테이블
CREATE TABLE collection_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_id INTEGER,
    job_type VARCHAR(20) NOT NULL,  -- 'full', 'incremental', 'merge'
    status VARCHAR(20) NOT NULL,  -- 'pending', 'running', 'completed', 'failed'
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP DEFAULT NULL,
    total_stocks INTEGER DEFAULT 0,
    processed_stocks INTEGER DEFAULT 0,
    failed_stocks INTEGER DEFAULT 0,
    error_message TEXT DEFAULT NULL,
    FOREIGN KEY (config_id) REFERENCES collection_config(id)
);
```

### 2.2 일봉 데이터 테이블
```sql
-- 일봉 OHLCV 데이터
CREATE TABLE daily_ohlcv (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    date INTEGER NOT NULL,  -- YYYYMMDD
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,  -- 거래대금
    change REAL,  -- 전일대비 등락
    change_rate REAL,  -- 전일대비 등락률
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, date),
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code),
    CHECK (date BETWEEN 19000101 AND 21000101),
    CHECK (high >= low),
    CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0)
);

-- 일봉 추가 지표 (옵션)
CREATE TABLE daily_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    date INTEGER NOT NULL,
    market_cap BIGINT,  -- 시가총액
    listed_shares BIGINT,  -- 상장주식수
    foreign_limit BIGINT,  -- 외국인주문한도수량
    foreign_holdings BIGINT,  -- 외국인현보유수량
    foreign_ratio REAL,  -- 외국인현보유비율
    institution_net BIGINT,  -- 기관순매수
    institution_cumulative BIGINT,  -- 기관누적순매수
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, date),
    FOREIGN KEY (stock_code, date) REFERENCES daily_ohlcv(stock_code, date)
);
```

### 2.3 분봉 데이터 테이블
```sql
-- 분봉 데이터 (공통 테이블)
CREATE TABLE minute_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    datetime INTEGER NOT NULL,  -- YYYYMMDDHHMM
    date INTEGER NOT NULL,  -- YYYYMMDD (파티셔닝용)
    interval_minutes INTEGER NOT NULL,  -- 1, 5, 30, 60
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,  -- 거래대금
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, datetime, interval_minutes),
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code),
    CHECK (interval_minutes IN (1, 5, 30, 60)),
    CHECK (datetime BETWEEN 190001010000 AND 210001010000),
    CHECK (high >= low),
    CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0)
);

-- 분봉별 파티션 인덱스 (성능 최적화)
CREATE TABLE minute_data_partition_index (
    stock_code VARCHAR(10) NOT NULL,
    interval_minutes INTEGER NOT NULL,
    min_datetime INTEGER NOT NULL,
    max_datetime INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    PRIMARY KEY (stock_code, interval_minutes)
);
```

### 2.4 데이터 품질 관리 테이블
```sql
-- 데이터 검증 결과
CREATE TABLE data_validation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    chart_type VARCHAR(20) NOT NULL,
    interval_minutes INTEGER DEFAULT NULL,
    validation_date INTEGER NOT NULL,  -- YYYYMMDD
    check_type VARCHAR(30) NOT NULL,  -- 'completeness', 'consistency', 'accuracy'
    status VARCHAR(20) NOT NULL,  -- 'pass', 'fail', 'warning'
    issues_found INTEGER DEFAULT 0,
    details TEXT,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);

-- 데이터 갭(누락) 정보
CREATE TABLE data_gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    chart_type VARCHAR(20) NOT NULL,
    interval_minutes INTEGER DEFAULT NULL,
    gap_start INTEGER NOT NULL,  -- YYYYMMDD or YYYYMMDDHHMM
    gap_end INTEGER NOT NULL,
    gap_size INTEGER NOT NULL,  -- 누락된 데이터 포인트 수
    priority INTEGER DEFAULT 1,  -- 1: 높음, 2: 중간, 3: 낮음
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filled_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);
```

### 2.5 병합 이력 테이블
```sql
-- 데이터 병합 이력
CREATE TABLE merge_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    chart_type VARCHAR(20) NOT NULL,
    interval_minutes INTEGER DEFAULT NULL,
    merge_type VARCHAR(20) NOT NULL,  -- 'incremental', 'backfill', 'correction'
    before_merge_start INTEGER,  -- 병합 전 데이터 시작점
    before_merge_end INTEGER,    -- 병합 전 데이터 종료점
    after_merge_start INTEGER,   -- 병합 후 데이터 시작점
    after_merge_end INTEGER,     -- 병합 후 데이터 종료점
    added_count INTEGER DEFAULT 0,
    updated_count INTEGER DEFAULT 0,
    removed_count INTEGER DEFAULT 0,
    merge_date INTEGER NOT NULL,  -- YYYYMMDD
    merged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_code) REFERENCES stock_metadata(stock_code)
);
```

## 3. 인덱스 설계

### 3.1 필수 인덱스
```sql
-- 일봉 데이터 인덱스
CREATE INDEX idx_daily_stock_date ON daily_ohlcv(stock_code, date);
CREATE INDEX idx_daily_date ON daily_ohlcv(date);
CREATE INDEX idx_daily_stock ON daily_ohlcv(stock_code);

-- 분봉 데이터 인덱스
CREATE INDEX idx_minute_stock_datetime ON minute_data(stock_code, datetime);
CREATE INDEX idx_minute_stock_interval ON minute_data(stock_code, interval_minutes);
CREATE INDEX idx_minute_datetime ON minute_data(datetime);
CREATE INDEX idx_minute_date_interval ON minute_data(date, interval_minutes);

-- 메타데이터 인덱스
CREATE INDEX idx_stock_market ON stock_metadata(market_type);
CREATE INDEX idx_stock_sector ON stock_metadata(sector_code);
CREATE INDEX idx_stock_listing ON stock_metadata(listing_date);

-- 데이터 갭 인덱스
CREATE INDEX idx_gaps_stock_type ON data_gaps(stock_code, chart_type, interval_minutes);
CREATE INDEX idx_gaps_priority ON data_gaps(priority, detected_at);
```

### 3.2 파티셔닝 전략 (대용량 데이터)
```sql
-- 일봉 데이터 연도별 파티셔닝 (가상)
-- 실제 구현은 애플리케이션 레벨에서 처리
-- 예: daily_ohlcv_2024, daily_ohlcv_2025

-- 분봉 데이터 월별 파티셔닝
-- 예: minute_data_202401, minute_data_202402
```

## 4. 뷰(View) 설계

### 4.1 데이터 요약 뷰
```sql
-- 종목별 데이터 요약
CREATE VIEW stock_data_summary AS
SELECT 
    sm.stock_code,
    sm.stock_name,
    sm.market_type,
    COUNT(DISTINCT doh.date) as daily_count,
    MIN(doh.date) as daily_first,
    MAX(doh.date) as daily_last,
    COUNT(DISTINCT md.datetime) as minute_count,
    MIN(md.datetime) as minute_first,
    MAX(md.datetime) as minute_last
FROM stock_metadata sm
LEFT JOIN daily_ohlcv doh ON sm.stock_code = doh.stock_code
LEFT JOIN minute_data md ON sm.stock_code = md.stock_code
GROUP BY sm.stock_code, sm.stock_name, sm.market_type;

-- 일별 데이터 통계
CREATE VIEW daily_statistics AS
SELECT 
    date,
    COUNT(DISTINCT stock_code) as stock_count,
    SUM(volume) as total_volume,
    SUM(amount) as total_amount,
    AVG(change_rate) as avg_change_rate
FROM daily_ohlcv
GROUP BY date
ORDER BY date DESC;
```

### 4.2 데이터 품질 모니터링 뷰
```sql
-- 데이터 갭 모니터링
CREATE VIEW gap_monitoring AS
SELECT 
    dg.stock_code,
    sm.stock_name,
    dg.chart_type,
    dg.interval_minutes,
    dg.gap_start,
    dg.gap_end,
    dg.gap_size,
    dg.priority,
    dg.detected_at,
    dg.filled_at,
    CASE 
        WHEN dg.filled_at IS NULL THEN 'pending'
        ELSE 'filled'
    END as status
FROM data_gaps dg
JOIN stock_metadata sm ON dg.stock_code = sm.stock_code
ORDER BY dg.priority, dg.detected_at DESC;
```

## 5. 트리거(Trigger) 설계

### 5.1 자동 업데이트 트리거
```sql
-- daily_ohlcv 업데이트 시 updated_at 자동 설정
CREATE TRIGGER update_daily_ohlcv_timestamp 
AFTER UPDATE ON daily_ohlcv
BEGIN
    UPDATE daily_ohlcv 
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

-- stock_metadata 업데이트 시 updated_at 자동 설정
CREATE TRIGGER update_stock_metadata_timestamp 
AFTER UPDATE ON stock_metadata
BEGIN
    UPDATE stock_metadata 
    SET updated_at = CURRENT_TIMESTAMP
    WHERE stock_code = NEW.stock_code;
END;
```

### 5.2 데이터 무결성 트리거
```sql
-- 분봉 데이터 삽입 시 date 필드 자동 계산
CREATE TRIGGER set_minute_data_date
BEFORE INSERT ON minute_data
BEGIN
    SET NEW.date = CAST(SUBSTR(CAST(NEW.datetime AS TEXT), 1, 8) AS INTEGER);
END;

-- 일봉 데이터 삽입 시 change_rate 계산
CREATE TRIGGER calculate_daily_change
BEFORE INSERT ON daily_ohlcv
FOR EACH ROW
BEGIN
    -- 전일 종가 조회 (간단한 예시, 실제로는 더 복잡한 로직 필요)
    SELECT close INTO @prev_close 
    FROM daily_ohlcv 
    WHERE stock_code = NEW.stock_code 
    AND date = (SELECT MAX(date) FROM daily_ohlcv 
                WHERE stock_code = NEW.stock_code AND date < NEW.date);
    
    IF @prev_close IS NOT NULL AND @prev_close > 0 THEN
        SET NEW.change = NEW.close - @prev_close;
        SET NEW.change_rate = (NEW.change / @prev_close) * 100;
    END IF;
END;
```

## 6. 초기 데이터 설정

### 6.1 시스템 기본 설정
```sql
INSERT INTO system_config (config_key, config_value, description) VALUES
('data_retention_days', '3650', '데이터 보관 기간 (일)'),
('merge_lookback_days', '3', '이어받기 시 확인할 일수'),
('default_collection_count', '1000', '기본 수집 데이터 개수'),
('request_delay_seconds', '0.25', 'API 요청 간 딜레이'),
('max_retry_count', '3', '최대 재시도 횟수'),
('auto_merge_enabled', 'true', '자동 병합 기능 사용 여부'),
('data_validation_enabled', 'true', '데이터 검증 기능 사용 여부');

-- 기본 수집 설정
INSERT INTO collection_config (config_name, chart_type, interval_minutes, data_count, ohlcv_only) VALUES
('daily_full', 'daily', NULL, 10000, TRUE),
('1min_full', 'minute', 1, 5000, TRUE),
('5min_full', 'minute', 5, 5000, TRUE),
('30min_full', 'minute', 30, 5000, TRUE),
('60min_full', 'minute', 60, 5000, TRUE);
```

## 7. 성능 최적화 가이드

### 7.1 인덱스 사용 최적화
- 자주 조회되는 컬럼에 인덱스 생성
- 복합 인덱스는 자주 함께 사용되는 컬럼 순서로 구성
- INSERT 성능을 위해 불필요한 인덱스 최소화

### 7.2 쿼리 최적화
- 범위 쿼리 시 인덱스 활용
- 대량 데이터 조회 시 LIMIT 사용
- 자주 사용되는 집계는 뷰로 미리 계산

### 7.3 데이터 관리
- 오래된 데이터 아카이빙
- 정기적인 VACUUM 실행 (SQLite)
- 데이터베이스 통계 업데이트

## 8. 백업 및 복구 전략

### 8.1 백업 방식
- **전체 백업**: 주기적 전체 데이터베이스 덤프
- **증분 백업**: 변경된 데이터만 백업
- **로그 백업**: 트랜잭션 로그 백업

### 8.2 백업 스케줄
- 일별: 증분 백업
- 주별: 전체 백업
- 월별: 아카이브 백업

### 8.3 복구 절차
1. 최신 전체 백업 복원
2. 증분 백업 적용
3. 트랜잭션 로그 재생
4. 데이터 무결성 검증

---

**데이터베이스 버전**: 2.0  
**호환성**: SQLite 3.35+, PostgreSQL 12+  
**최대 지원 데이터량**: 1억 건 이상  
**예상 디스크 사용량**: 100GB ~ 1TB (데이터량에 따라)