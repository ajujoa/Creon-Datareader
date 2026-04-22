# coding=utf-8
"""
Creon DataReader v2.0 설정 관리 모듈
"""

import os
import yaml
import json
import configparser
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class StorageConfig:
    """저장 설정"""
    file_enabled: bool = True
    file_format: str = "json"  # json, csv, parquet
    file_base_path: str = "./data"
    file_compression: bool = False
    file_encoding: str = "utf-8"
    
    database_enabled: bool = True
    database_type: str = "sqlite"  # sqlite, postgresql, mariadb
    database_path: str = "./data/database/creon_data.db"
    database_host: Optional[str] = None
    database_port: Optional[int] = None
    database_name: Optional[str] = None
    database_user: Optional[str] = None
    database_password: Optional[str] = None


@dataclass
class DatabaseSQLiteConfig:
    """SQLite 데이터베이스 설정"""
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    cache_size: int = -2000
    temp_store: str = "MEMORY"
    mmap_size: int = 268435456
    busy_timeout: int = 5000
    foreign_keys: bool = True
    auto_vacuum: str = "INCREMENTAL"


@dataclass
class DatabasePostgreSQLConfig:
    """PostgreSQL 데이터베이스 설정"""
    host: str = "localhost"
    port: int = 5432
    database: str = "creon_data"
    user: str = "creon_user"
    password: str = ""
    pool_size: int = 10
    pool_recycle: int = 3600
    pool_timeout: int = 30
    charset: str = "utf8"
    sslmode: str = "disable"


@dataclass
class DatabaseMariaDBConfig:
    """MariaDB 데이터베이스 설정"""
    host: str = "localhost"
    port: int = 3306
    database: str = "creon_data"
    user: str = "creon_user"
    password: str = ""
    pool_size: int = 10
    pool_recycle: int = 3600
    pool_timeout: int = 30
    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"
    use_compression: bool = True


@dataclass
class CollectionConfig:
    """수집 설정"""
    default_count: int = 1000
    max_retries: int = 3
    retry_delay: float = 1.0
    request_delay: float = 0.25
    chunk_size: int = 100  # 한 번에 처리할 종목 수
    
    # 데이터 품질 설정
    validate_data: bool = True
    check_ohlcv_integrity: bool = True
    check_timestamp_order: bool = True
    check_for_gaps: bool = True


@dataclass
class MergeConfig:
    """병합 설정"""
    enabled: bool = True
    lookback_days: int = 3
    auto_merge: bool = True
    max_gaps_to_fill: int = 3
    conflict_resolution: str = "newer_source"  # newer_source, higher_quality, manual
    
    # 병합 검증
    validate_before_save: bool = True
    create_backup: bool = True
    backup_retention_days: int = 30


@dataclass
class FilterConfig:
    """필터 설정"""
    price_min: int = 10000
    price_max: int = 30000
    exclude_etf: bool = True
    exclude_etn: bool = True
    exclude_delisted: bool = True
    
    exclude_keywords: List[str] = field(default_factory=lambda: [
        "KODEX", "TIGER", "ACE", "액티브", "KOSEF", "ARIRANG",
        "블룸버그", "합성", "SOL", "스팩", "HANARO", "메리츠"
    ])
    
    market_types: List[str] = field(default_factory=lambda: ["KOSPI"])  # KOSPI, KOSDAQ, KONEX


@dataclass
class MarketTypesConfig:
    """시장 타입 설정"""
    kosdaq: bool = True
    kospi: bool = True
    konex: bool = False


@dataclass
class ChartsConfig:
    """차트 데이터 설정"""
    daily_enabled: bool = True
    minute_1_enabled: bool = True
    minute_5_enabled: bool = True
    minute_30_enabled: bool = True
    minute_60_enabled: bool = True
    
    daily_retention_days: int = 3650
    minute_1_retention_days: int = 30
    minute_5_retention_days: int = 90
    minute_30_retention_days: int = 180
    minute_60_retention_days: int = 365


@dataclass
class APIConfig:
    """API 설정"""
    timeout: int = 30
    max_connections: int = 10
    rate_limit_per_minute: int = 60
    user_agent: str = "CreonDataReader/2.0"


@dataclass
class SecurityConfig:
    """보안 설정"""
    encrypt_database: bool = False
    encryption_key: str = ""
    enable_audit_log: bool = True
    audit_log_path: str = "./logs/audit"
    log_sensitive_operations: bool = False


@dataclass
class MaintenanceConfig:
    """유지보수 설정"""
    auto_vacuum_days: int = 7
    auto_backup_days: int = 1
    cleanup_old_files_days: int = 30
    check_integrity_days: int = 1
    optimize_database_days: int = 30


@dataclass
class NotificationsConfig:
    """알림 설정"""
    enable_email: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    email_to: str = ""
    enable_telegram: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""


@dataclass
class DebugConfig:
    """디버그 설정"""
    enable_debug_mode: bool = False
    log_sql_queries: bool = False
    log_api_calls: bool = False
    log_performance: bool = False
    profiling_enabled: bool = False
    profiling_output: str = "./logs/profiles"


@dataclass
class LoggingConfig:
    """로깅 설정"""
    level: str = "INFO"
    file_enabled: bool = True
    file_path: str = "./logs"
    file_max_size: int = 10 * 1024 * 1024  # 10MB
    file_backup_count: int = 5
    console_enabled: bool = True
    
    # 모니터링
    enable_metrics: bool = True
    metrics_port: int = 9090


@dataclass
class PerformanceConfig:
    """성능 설정"""
    max_workers: int = 4
    use_multiprocessing: bool = False
    memory_limit_mb: int = 1024
    batch_size: int = 1000
    
    # 캐시 설정
    enable_cache: bool = True
    cache_ttl_seconds: int = 300
    cache_max_size: int = 1000


@dataclass
class SystemConfig:
    """시스템 전체 설정"""
    storage: StorageConfig = field(default_factory=StorageConfig)
    collection: CollectionConfig = field(default_factory=CollectionConfig)
    merge: MergeConfig = field(default_factory=MergeConfig)
    filters: FilterConfig = field(default_factory=FilterConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    
    # 데이터베이스 특화 설정
    database_sqlite: DatabaseSQLiteConfig = field(default_factory=DatabaseSQLiteConfig)
    database_postgresql: DatabasePostgreSQLConfig = field(default_factory=DatabasePostgreSQLConfig)
    database_mariadb: DatabaseMariaDBConfig = field(default_factory=DatabaseMariaDBConfig)
    
    # 추가 설정
    market_types: MarketTypesConfig = field(default_factory=MarketTypesConfig)
    charts: ChartsConfig = field(default_factory=ChartsConfig)
    api: APIConfig = field(default_factory=APIConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    maintenance: MaintenanceConfig = field(default_factory=MaintenanceConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    debug: DebugConfig = field(default_factory=DebugConfig)
    
    # 시스템 메타데이터
    version: str = "2.0.0"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())


class ConfigManager:
    """설정 관리 클래스"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._get_default_config_path()
        self.config: Optional[SystemConfig] = None
        self._ensure_config_directory()
        self.load()
    
    def _get_default_config_path(self) -> str:
        """기본 설정 파일 경로 반환"""
        # INI 파일을 우선 사용, 없으면 YAML 사용
        ini_path = "./config/creon_config.ini"
        yaml_path = "./config/creon_config.yaml"
        
        if Path(ini_path).exists():
            return ini_path
        elif Path(yaml_path).exists():
            return yaml_path
        else:
            # 기본값으로 INI 파일 생성
            return ini_path
    
    def _ensure_config_directory(self):
        """설정 디렉토리 생성"""
        config_dir = Path(self.config_path).parent
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # 로그 디렉토리도 생성
        log_dir = Path("./logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 데이터 디렉토리 생성
        data_dir = Path("./data")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        database_dir = Path("./data/database")
        database_dir.mkdir(parents=True, exist_ok=True)
    
    def load(self, config_path: Optional[str] = None):
        """설정 파일 로드"""
        if config_path:
            self.config_path = config_path
        
        config_file = Path(self.config_path)
        
        if config_file.exists():
            try:
                config_dict = {}
                
                if config_file.suffix.lower() in ['.yaml', '.yml']:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config_dict = yaml.safe_load(f)
                elif config_file.suffix.lower() == '.json':
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config_dict = json.load(f)
                elif config_file.suffix.lower() == '.ini':
                    config_dict = self._load_ini_config(config_file)
                else:
                    raise ValueError(f"지원하지 않는 설정 파일 형식: {config_file.suffix}")
                
                # 딕셔너리를 dataclass로 변환
                self.config = self._dict_to_dataclass(config_dict, SystemConfig)
                logger.info(f"설정 파일 로드 완료: {self.config_path}")
                
            except Exception as e:
                logger.error(f"설정 파일 로드 실패: {e}")
                self._create_default_config()
        else:
            logger.warning(f"설정 파일 없음. 기본 설정 생성: {self.config_path}")
            self._create_default_config()
    
    def _load_ini_config(self, config_file: Path) -> Dict:
        """INI 설정 파일 로드"""
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')
        
        config_dict = {}
        
        # 모든 섹션 처리
        for section in config.sections():
            section_dict = {}
            for key, value in config.items(section):
                # 값 변환 (문자열 → 적절한 타입)
                section_dict[key] = self._convert_ini_value(value)
            
            # 섹션 이름을 키로 사용
            config_dict[section] = section_dict
        
        return config_dict
    
    def _convert_ini_value(self, value: str) -> Any:
        """INI 값 변환 (문자열 → 적절한 타입)"""
        # 빈 문자열 처리
        if value == '':
            return ''
        
        # 불리언 값
        if value.lower() in ('true', 'yes', 'on', '1'):
            return True
        if value.lower() in ('false', 'no', 'off', '0'):
            return False
        
        # 정수
        if value.isdigit():
            return int(value)
        
        # 부동소수점
        try:
            return float(value)
        except ValueError:
            pass
        
        # 리스트 (쉼표로 구분)
        if ',' in value:
            items = [item.strip() for item in value.split(',')]
            # 각 항목 변환 시도
            converted_items = []
            for item in items:
                converted_items.append(self._convert_ini_value(item))
            return converted_items
        
        # 기본값: 문자열
        return value
    
    def _create_default_config(self):
        """기본 설정 생성"""
        self.config = SystemConfig()
        self.save()
        logger.info("기본 설정 파일 생성 완료")
    
    def save(self, config_path: Optional[str] = None):
        """설정 파일 저장"""
        if config_path:
            self.config_path = config_path
        
        if self.config is None:
            logger.error("저장할 설정이 없습니다.")
            return
        
        # 업데이트 시간 갱신
        self.config.updated_at = datetime.now().isoformat()
        
        config_file = Path(self.config_path)
        config_dict = self._dataclass_to_dict(self.config)
        
        try:
            if config_file.suffix.lower() in ['.yaml', '.yml']:
                with open(config_file, 'w', encoding='utf-8') as f:
                    yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            elif config_file.suffix.lower() == '.json':
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(config_dict, f, ensure_ascii=False, indent=2)
            elif config_file.suffix.lower() == '.ini':
                self._save_ini_config(config_file, config_dict)
            else:
                # 기본은 INI로 저장
                self._save_ini_config(config_file, config_dict)
            
            logger.info(f"설정 파일 저장 완료: {self.config_path}")
            
        except Exception as e:
            logger.error(f"설정 파일 저장 실패: {e}")
    
    def _save_ini_config(self, config_file: Path, config_dict: Dict):
        """INI 설정 파일 저장"""
        config = configparser.ConfigParser()
        
        # 딕셔너리를 INI 형식으로 변환
        for section, values in config_dict.items():
            if isinstance(values, dict):
                config[section] = {}
                for key, value in values.items():
                    # 값 변환 (타입 → 문자열)
                    config[section][key] = self._convert_to_ini_string(value)
            else:
                # 섹션 값이 딕셔너리가 아닌 경우
                config[section] = {'value': self._convert_to_ini_string(values)}
        
        with open(config_file, 'w', encoding='utf-8') as f:
            config.write(f)
    
    def _convert_to_ini_string(self, value: Any) -> str:
        """값을 INI 문자열로 변환"""
        if isinstance(value, bool):
            return 'true' if value else 'false'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            # 리스트는 쉼표로 구분된 문자열로 변환
            return ','.join([self._convert_to_ini_string(item) for item in value])
        elif isinstance(value, dict):
            # 딕셔너리는 JSON 문자열로 변환
            return json.dumps(value, ensure_ascii=False)
        elif value is None:
            return ''
        else:
            return str(value)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """설정 값 조회 (점 표기법 지원)"""
        if self.config is None:
            return default
        
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                if hasattr(value, key):
                    value = getattr(value, key)
                elif isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            return value
        except (AttributeError, KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any):
        """설정 값 설정 (점 표기법 지원)"""
        if self.config is None:
            logger.error("설정이 초기화되지 않았습니다.")
            return
        
        keys = key_path.split('.')
        target = self.config
        
        try:
            # 마지막 키 직전까지 이동
            for key in keys[:-1]:
                if hasattr(target, key):
                    target = getattr(target, key)
                elif isinstance(target, dict) and key in target:
                    target = target[key]
                else:
                    # 중간 경로가 없으면 생성 (dict로)
                    if isinstance(target, dict):
                        target[key] = {}
                        target = target[key]
                    else:
                        logger.error(f"설정 경로를 생성할 수 없음: {key_path}")
                        return
            
            # 마지막 키 설정
            last_key = keys[-1]
            if hasattr(target, last_key):
                setattr(target, last_key, value)
            elif isinstance(target, dict):
                target[last_key] = value
            else:
                logger.error(f"설정 경로를 설정할 수 없음: {key_path}")
                return
            
            # 설정 파일 자동 저장
            self.save()
            logger.debug(f"설정 업데이트: {key_path} = {value}")
            
        except Exception as e:
            logger.error(f"설정 업데이트 실패: {e}")
    
    def _dataclass_to_dict(self, obj: Any) -> Dict:
        """dataclass 객체를 딕셔너리로 변환"""
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        elif isinstance(obj, list):
            return [self._dataclass_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._dataclass_to_dict(v) for k, v in obj.items()}
        elif hasattr(obj, '__dataclass_fields__'):
            result = {}
            for field_name in obj.__dataclass_fields__:
                field_value = getattr(obj, field_name)
                result[field_name] = self._dataclass_to_dict(field_value)
            return result
        else:
            return obj
    
    def _dict_to_dataclass(self, data: Dict, dataclass_type: type) -> Any:
        """딕셔너리를 dataclass 객체로 변환"""
        if not isinstance(data, dict):
            return data
        
        # dataclass 필드 정보 가져오기
        field_types = {f.name: f.type for f in dataclass_type.__dataclass_fields__.values()}
        
        kwargs = {}
        for field_name, field_type in field_types.items():
            if field_name in data:
                field_value = data[field_name]
                
                # 필드 타입이 dataclass인 경우 재귀적 변환
                if hasattr(field_type, '__dataclass_fields__'):
                    kwargs[field_name] = self._dict_to_dataclass(field_value, field_type)
                # 필드 타입이 List[dataclass]인 경우
                elif (hasattr(field_type, '__origin__') and 
                      field_type.__origin__ == list and
                      len(field_type.__args__) > 0 and
                      hasattr(field_type.__args__[0], '__dataclass_fields__')):
                    item_type = field_type.__args__[0]
                    kwargs[field_name] = [self._dict_to_dataclass(item, item_type) for item in field_value]
                else:
                    kwargs[field_name] = field_value
        
        return dataclass_type(**kwargs)
    
    def validate(self) -> Dict[str, List[str]]:
        """설정 유효성 검증"""
        errors = {}
        
        if self.config is None:
            errors['system'] = ['설정이 초기화되지 않았습니다.']
            return errors
        
        # 저장 설정 검증
        if self.config.storage.file_enabled:
            base_path = Path(self.config.storage.file_base_path)
            if not base_path.parent.exists():
                errors.setdefault('storage', []).append(f'파일 저장 경로 부모 디렉토리가 없음: {base_path.parent}')
        
        if self.config.storage.database_enabled:
            if self.config.storage.database_type == 'sqlite':
                db_path = Path(self.config.storage.database_path)
                if not db_path.parent.exists():
                    errors.setdefault('storage', []).append(f'데이터베이스 경로 부모 디렉토리가 없음: {db_path.parent}')
        
        # 수집 설정 검증
        if self.config.collection.default_count <= 0:
            errors.setdefault('collection', []).append('기본 수집 개수는 0보다 커야 합니다.')
        
        if self.config.collection.request_delay < 0:
            errors.setdefault('collection', []).append('요청 딜레이는 0 이상이어야 합니다.')
        
        # 병합 설정 검증
        if self.config.merge.lookback_days < 0:
            errors.setdefault('merge', []).append('lookback_days는 0 이상이어야 합니다.')
        
        # 필터 설정 검증
        if self.config.filters.price_min > self.config.filters.price_max:
            errors.setdefault('filters', []).append('최소 가격이 최대 가격보다 큽니다.')
        
        # 성능 설정 검증
        if self.config.performance.max_workers <= 0:
            errors.setdefault('performance', []).append('max_workers는 0보다 커야 합니다.')
        
        if self.config.performance.memory_limit_mb <= 0:
            errors.setdefault('performance', []).append('memory_limit_mb는 0보다 커야 합니다.')
        
        return errors
    
    def setup_logging(self):
        """로깅 설정 적용"""
        if self.config is None:
            return
        
        log_config = self.config.logging
        
        # 루트 로거 설정
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_config.level.upper()))
        
        # 기존 핸들러 제거
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 포맷터 설정
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 콘솔 핸들러
        if log_config.console_enabled:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
        
        # 파일 핸들러
        if log_config.file_enabled:
            from logging.handlers import RotatingFileHandler
            
            log_file = Path(log_config.file_path) / "creon_datareader.log"
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=log_config.file_max_size,
                backupCount=log_config.file_backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        
        logger.info(f"로깅 설정 완료 (레벨: {log_config.level})")


# 전역 설정 인스턴스
_config_manager: Optional[ConfigManager] = None


def get_config(config_path: Optional[str] = None) -> ConfigManager:
    """전역 설정 관리자 인스턴스 반환"""
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
        _config_manager.setup_logging()
    
    return _config_manager


def reload_config(config_path: Optional[str] = None):
    """설정 재로드"""
    global _config_manager
    
    if _config_manager is not None:
        _config_manager.load(config_path)
        _config_manager.setup_logging()
    else:
        _config_manager = ConfigManager(config_path)
        _config_manager.setup_logging()


if __name__ == "__main__":
    # 설정 테스트
    config = get_config()
    print("현재 설정:")
    print(json.dumps(config._dataclass_to_dict(config.config), ensure_ascii=False, indent=2))
    
    # 설정 검증
    errors = config.validate()
    if errors:
        print("\n설정 오류:")
        for category, msgs in errors.items():
            for msg in msgs:
                print(f"  {category}: {msg}")
    else:
        print("\n설정 유효성 검증 통과")