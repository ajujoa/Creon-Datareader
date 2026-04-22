# coding=utf-8
"""
Creon DataReader v2.0 - 파일 관리 모듈
"""

import json
import csv
import pickle
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd
import numpy as np

from creon_config import get_config

logger = logging.getLogger(__name__)


class FileManagerError(Exception):
    """파일 관리 관련 에러"""
    pass


class CreonFileManager:
    """Creon 파일 관리 클래스"""
    
    def __init__(self, base_path: Optional[str] = None):
        """초기화"""
        self.config = get_config()
        
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path(self.config.get('storage.file_base_path', './data'))
        
        # 디렉토리 생성
        self._create_directories()
        logger.info(f"파일 관리자 초기화 완료: {self.base_path}")
    
    def _create_directories(self):
        """필요한 디렉토리 생성"""
        directories = [
            self.base_path / "daily" / "json",
            self.base_path / "daily" / "csv",
            self.base_path / "minute" / "1min",
            self.base_path / "minute" / "5min",
            self.base_path / "minute" / "30min",
            self.base_path / "minute" / "60min",
            self.base_path / "backup",
            self.base_path / "logs"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"디렉토리 생성: {directory}")
    
    def get_file_path(self, stock_code: str, chart_type: str, 
                     interval: Optional[int] = None, 
                     file_format: str = "json") -> Path:
        """
        파일 경로 생성
        """
        if chart_type == "daily":
            if file_format == "json":
                return self.base_path / "daily" / "json" / f"{stock_code}_daily.json"
            elif file_format == "csv":
                return self.base_path / "daily" / "csv" / f"{stock_code}_daily.csv"
            else:
                raise FileManagerError(f"지원하지 않는 파일 형식: {file_format}")
        
        elif chart_type == "minute":
            if interval is None:
                raise FileManagerError("분봉 데이터는 interval이 필요합니다.")
            
            interval_dir = f"{interval}min"
            if file_format == "json":
                return self.base_path / "minute" / interval_dir / f"{stock_code}_{interval}min.json"
            elif file_format == "csv":
                return self.base_path / "minute" / interval_dir / f"{stock_code}_{interval}min.csv"
            else:
                raise FileManagerError(f"지원하지 않는 파일 형식: {file_format}")
        
        else:
            raise FileManagerError(f"지원하지 않는 차트 타입: {chart_type}")
    
    def save_data(self, stock_code: str, chart_type: str, 
                 data: Dict[str, Any], interval: Optional[int] = None,
                 file_format: Optional[str] = None) -> bool:
        """
        데이터 저장
        """
        if file_format is None:
            file_format = self.config.get('storage.file_format', 'json')
        
        try:
            file_path = self.get_file_path(stock_code, chart_type, interval, file_format)
            
            # 메타데이터 추가
            data_with_meta = self._add_metadata(data, stock_code, chart_type, interval)
            
            if file_format == "json":
                self._save_json(file_path, data_with_meta)
            elif file_format == "csv":
                self._save_csv(file_path, data_with_meta)
            else:
                raise FileManagerError(f"지원하지 않는 파일 형식: {file_format}")
            
            logger.info(f"데이터 저장 완료: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"데이터 저장 실패: {stock_code}, {chart_type}, {e}")
            return False
    
    def _add_metadata(self, data: Dict[str, Any], stock_code: str, 
                     chart_type: str, interval: Optional[int]) -> Dict[str, Any]:
        """메타데이터 추가"""
        metadata = {
            'meta': {
                'stock_code': stock_code,
                'chart_type': chart_type,
                'interval': interval,
                'created_at': datetime.now().isoformat(),
                'version': '2.0.0',
                'columns': list(data.keys()) if isinstance(data, dict) else []
            },
            'data': data
        }
        return metadata
    
    def _save_json(self, file_path: Path, data: Dict[str, Any]):
        """JSON 파일 저장"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _save_csv(self, file_path: Path, data: Dict[str, Any]):
        """CSV 파일 저장"""
        # 데이터 추출
        if 'data' in data and isinstance(data['data'], dict):
            df = pd.DataFrame(data['data'])
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        else:
            # 데이터가 딕셔너리 형태인 경우
            df = pd.DataFrame(data)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    def load_data(self, stock_code: str, chart_type: str,
                 interval: Optional[int] = None,
                 file_format: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        데이터 로드
        """
        if file_format is None:
            file_format = self.config.get('storage.file_format', 'json')
        
        try:
            file_path = self.get_file_path(stock_code, chart_type, interval, file_format)
            
            if not file_path.exists():
                logger.debug(f"파일 없음: {file_path}")
                return None
            
            if file_format == "json":
                data = self._load_json(file_path)
            elif file_format == "csv":
                data = self._load_csv(file_path)
            else:
                raise FileManagerError(f"지원하지 않는 파일 형식: {file_format}")
            
            logger.debug(f"데이터 로드 완료: {file_path}")
            return data
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {stock_code}, {chart_type}, {e}")
            return None
    
    def _load_json(self, file_path: Path) -> Dict[str, Any]:
        """JSON 파일 로드"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_csv(self, file_path: Path) -> Dict[str, Any]:
        """CSV 파일 로드"""
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 데이터프레임을 딕셔너리로 변환
        data = {}
        for column in df.columns:
            data[column] = df[column].tolist()
        
        return {
            'meta': {
                'created_at': datetime.now().isoformat(),
                'columns': list(df.columns)
            },
            'data': data
        }
    
    def get_data_info(self, stock_code: str, chart_type: str,
                     interval: Optional[int] = None) -> Dict[str, Any]:
        """
        데이터 정보 조회
        """
        info = {
            'exists': False,
            'file_path': None,
            'file_size': 0,
            'created_at': None,
            'modified_at': None,
            'data_range': None,
            'data_count': 0
        }
        
        try:
            # JSON 파일 확인
            json_path = self.get_file_path(stock_code, chart_type, interval, 'json')
            csv_path = self.get_file_path(stock_code, chart_type, interval, 'csv')
            
            file_path = None
            if json_path.exists():
                file_path = json_path
            elif csv_path.exists():
                file_path = csv_path
            
            if file_path and file_path.exists():
                info['exists'] = True
                info['file_path'] = str(file_path)
                info['file_size'] = file_path.stat().st_size
                info['modified_at'] = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                
                # 데이터 로드하여 추가 정보 추출
                data = self.load_data(stock_code, chart_type, interval)
                if data and 'meta' in data:
                    info['created_at'] = data['meta'].get('created_at')
                
                if data and 'data' in data:
                    data_dict = data['data']
                    if isinstance(data_dict, dict) and 'date' in data_dict:
                        dates = data_dict['date']
                        if dates:
                            info['data_count'] = len(dates)
                            info['data_range'] = {
                                'start': min(dates),
                                'end': max(dates)
                            }
        
        except Exception as e:
            logger.error(f"데이터 정보 조회 실패: {stock_code}, {chart_type}, {e}")
        
        return info
    
    def merge_data(self, existing_data: Dict[str, Any], new_data: Dict[str, Any],
                  merge_strategy: str = 'append') -> Dict[str, Any]:
        """
        데이터 병합
        """
        try:
            if merge_strategy == 'replace':
                # 기존 데이터 완전 대체
                merged = new_data
            
            elif merge_strategy == 'append':
                # 기존 데이터에 새 데이터 추가
                merged = self._append_data(existing_data, new_data)
            
            elif merge_strategy == 'update':
                # 기존 데이터 업데이트 (중복 제거)
                merged = self._update_data(existing_data, new_data)
            
            else:
                raise FileManagerError(f"지원하지 않는 병합 전략: {merge_strategy}")
            
            # 메타데이터 업데이트
            if 'meta' in merged:
                merged['meta']['updated_at'] = datetime.now().isoformat()
                merged['meta']['merge_strategy'] = merge_strategy
            
            logger.debug(f"데이터 병합 완료: 전략={merge_strategy}")
            return merged
            
        except Exception as e:
            logger.error(f"데이터 병합 실패: {e}")
            raise FileManagerError(f"데이터 병합 실패: {e}")
    
    def _append_data(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 추가 병합"""
        merged = {}
        
        if 'data' in existing and 'data' in new:
            existing_data = existing['data']
            new_data = new['data']
            
            if isinstance(existing_data, dict) and isinstance(new_data, dict):
                # 딕셔너리 병합
                for key in set(list(existing_data.keys()) + list(new_data.keys())):
                    if key in existing_data and key in new_data:
                        # 두 데이터 모두 있는 경우 연결
                        merged[key] = existing_data[key] + new_data[key]
                    elif key in existing_data:
                        merged[key] = existing_data[key]
                    else:
                        merged[key] = new_data[key]
            else:
                # 다른 형식의 데이터
                merged = new_data
        
        # 메타데이터 병합
        if 'meta' in existing:
            merged_meta = existing['meta'].copy()
            if 'meta' in new:
                merged_meta.update(new['meta'])
            merged['meta'] = merged_meta
        
        return {'data': merged, 'meta': merged.get('meta', {})}
    
    def _update_data(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 업데이트 병합 (중복 제거)"""
        merged = {}
        
        if 'data' in existing and 'data' in new:
            existing_data = existing['data']
            new_data = new['data']
            
            if isinstance(existing_data, dict) and isinstance(new_data, dict):
                # 날짜 기준 중복 제거
                if 'date' in existing_data and 'date' in new_data:
                    existing_dates = set(existing_data['date'])
                    new_dates = set(new_data['date'])
                    
                    # 중복되지 않은 새 데이터만 필터링
                    unique_indices = []
                    for i, date in enumerate(new_data['date']):
                        if date not in existing_dates:
                            unique_indices.append(i)
                    
                    # 데이터 병합
                    for key in set(list(existing_data.keys()) + list(new_data.keys())):
                        if key in existing_data and key in new_data:
                            # 기존 데이터 + 중복되지 않은 새 데이터
                            existing_values = existing_data[key]
                            new_values = [new_data[key][i] for i in unique_indices]
                            merged[key] = existing_values + new_values
                        elif key in existing_data:
                            merged[key] = existing_data[key]
                        elif key in new_data:
                            # 새 데이터만 있는 경우 (중복되지 않은 것만)
                            merged[key] = [new_data[key][i] for i in unique_indices]
                else:
                    # 날짜 정보 없으면 단순 병합
                    merged = self._append_data(existing, new)['data']
            else:
                merged = new_data
        
        # 메타데이터 병합
        if 'meta' in existing:
            merged_meta = existing['meta'].copy()
            if 'meta' in new:
                merged_meta.update(new['meta'])
            merged['meta'] = merged_meta
        
        return {'data': merged, 'meta': merged.get('meta', {})}
    
    def backup_data(self, stock_code: str, chart_type: str,
                   interval: Optional[int] = None) -> Optional[str]:
        """
        데이터 백업
        """
        try:
            # 원본 데이터 로드
            data = self.load_data(stock_code, chart_type, interval)
            if not data:
                logger.warning(f"백업할 데이터 없음: {stock_code}, {chart_type}")
                return None
            
            # 백업 파일명 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = self.base_path / "backup" / f"{stock_code}_{chart_type}"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            if interval:
                backup_file = backup_dir / f"{stock_code}_{chart_type}_{interval}_{timestamp}.json"
            else:
                backup_file = backup_dir / f"{stock_code}_{chart_type}_{timestamp}.json"
            
            # 백업 저장
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"데이터 백업 완료: {backup_file}")
            return str(backup_file)
            
        except Exception as e:
            logger.error(f"데이터 백업 실패: {stock_code}, {chart_type}, {e}")
            return None
    
    def cleanup_old_backups(self, retention_days: int = 30) -> int:
        """
        오래된 백업 파일 정리
        """
        try:
            backup_dir = self.base_path / "backup"
            if not backup_dir.exists():
                return 0
            
            cutoff_time = datetime.now().timestamp() - (retention_days * 24 * 60 * 60)
            deleted_count = 0
            
            for backup_file in backup_dir.rglob("*.json"):
                if backup_file.stat().st_mtime < cutoff_time:
                    backup_file.unlink()
                    deleted_count += 1
                    logger.debug(f"오래된 백업 파일 삭제: {backup_file}")
            
            logger.info(f"오래된 백업 파일 정리 완료: {deleted_count}개")
            return deleted_count
            
        except Exception as e:
            logger.error(f"백업 파일 정리 실패: {e}")
            return 0
    
    def export_to_dataframe(self, stock_code: str, chart_type: str,
                           interval: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        데이터를 DataFrame으로 변환
        """
        try:
            data = self.load_data(stock_code, chart_type, interval)
            if not data or 'data' not in data:
                return None
            
            data_dict = data['data']
            if isinstance(data_dict, dict):
                df = pd.DataFrame(data_dict)
                
                # 날짜 컬럼 변환
                if 'date' in df.columns:
                    if chart_type == 'daily':
                        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
                    elif chart_type == 'minute':
                        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M')
                    df.set_index('date', inplace=True)
                
                return df
            
            return None
            
        except Exception as e:
            logger.error(f"DataFrame 변환 실패: {stock_code}, {chart_type}, {e}")
            return None
    
    def get_all_files(self, chart_type: str, 
                     interval: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        모든 파일 정보 조회
        """
        files = []
        
        try:
            if chart_type == 'daily':
                json_dir = self.base_path / "daily" / "json"
                csv_dir = self.base_path / "daily" / "csv"
                
                # JSON 파일 검색
                for json_file in json_dir.glob("*.json"):
                    stock_code = json_file.stem.replace('_daily', '')
                    file_info = self.get_data_info(stock_code, 'daily')
                    files.append(file_info)
                
                # CSV 파일 검색
                for csv_file in csv_dir.glob("*.csv"):
                    stock_code = csv_file.stem.replace('_daily', '')
                    file_info = self.get_data_info(stock_code, 'daily')
                    files.append(file_info)
            
            elif chart_type == 'minute' and interval:
                interval_dir = f"{interval}min"
                json_dir = self.base_path / "minute" / interval_dir
                csv_dir = self.base_path / "minute" / interval_dir
                
                # JSON 파일 검색
                for json_file in json_dir.glob("*.json"):
                    stock_code = json_file.stem.replace(f'_{interval}min', '')
                    file_info = self.get_data_info(stock_code, 'minute', interval)
                    files.append(file_info)
                
                # CSV 파일 검색
                for csv_file in csv_dir.glob("*.csv"):
                    stock_code = csv_file.stem.replace(f'_{interval}min', '')
                    file_info = self.get_data_info(stock_code, 'minute', interval)
                    files.append(file_info)
        
        except Exception as e:
            logger.error(f"파일 목록 조회 실패: {chart_type}, {e}")
        
        return files


# 전역 파일 관리자 인스턴스
_file_manager: Optional[CreonFileManager] = None


def get_file_manager(base_path: Optional[str] = None) -> CreonFileManager:
    """전역 파일 관리자 인스턴스 반환"""
    global _file_manager
    
    if _file_manager is None:
        _file_manager = CreonFileManager(base_path)
    
    return _file_manager


if __name__ == "__main__":
    # 파일 관리자 테스트
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 파일 관리자 생성
        fm = get_file_manager()
        
        # 테스트 데이터
        test_data = {
            'date': [20240101, 20240102, 20240103],
            'open': [100.0, 101.0, 102.0],
            'high': [105.0, 106.0, 107.0],
            'low': [95.0, 96.0, 97.0],
            'close': [102.0, 103.0, 104.0],
            'volume': [1000000, 1100000, 1200000]
        }
        
        # 데이터 저장 테스트
        print("데이터 저장 테스트...")
        success = fm.save_data('005930', 'daily', test_data)
        if success:
            print("데이터 저장 성공")
        
        # 데이터 로드 테스트
        print("\n데이터 로드 테스트...")
        loaded_data = fm.load_data('005930', 'daily')
        if loaded_data:
            print(f"데이터 로드 성공: {len(loaded_data.get('data', {}).get('date', []))}개")
        
        # 데이터 정보 조회 테스트
        print("\n데이터 정보 조회 테스트...")
        info = fm.get_data_info('005930', 'daily')
        print(f"데이터 존재: {info['exists']}")
        print(f"파일 크기: {info['file_size']} bytes")
        print(f"데이터 개수: {info['data_count']}")
        
        # DataFrame 변환 테스트
        print("\nDataFrame 변환 테스트...")
        df = fm.export_to_dataframe('005930', 'daily')
        if df is not None:
            print(f"DataFrame 생성 성공: {df.shape}")
            print(df.head())
        
        # 백업 테스트
        print("\n백업 테스트...")
        backup_path = fm.backup_data('005930', 'daily')
        if backup_path:
            print(f"백업 생성: {backup_path}")
        
        # 파일 목록 조회 테스트
        print("\n파일 목록 조회 테스트...")
        files = fm.get_all_files('daily')
        print(f"일봉 파일 수: {len(files)}")
        
        print("\n파일 관리자 테스트 완료")
        
    except Exception as e:
        print(f"테스트 실패: {e}")
        import traceback
        traceback.print_exc()