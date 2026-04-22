#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creon DataReader 설정 모듈 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from creon_config import get_config, reload_config
import json

def test_ini_config():
    """INI 설정 파일 테스트"""
    print("=" * 60)
    print("INI 설정 파일 테스트")
    print("=" * 60)
    
    # INI 설정 파일 사용
    config = get_config("./config/creon_config.ini")
    
    # 설정 값 조회 테스트
    print("\n1. 기본 설정 값 조회:")
    print(f"   데이터베이스 타입: {config.get('storage.database_type')}")
    print(f"   파일 저장 활성화: {config.get('storage.file_enabled')}")
    print(f"   병합 활성화: {config.get('merge.enabled')}")
    
    print("\n2. 데이터베이스 특화 설정:")
    print(f"   SQLite journal_mode: {config.get('database_sqlite.journal_mode')}")
    print(f"   PostgreSQL host: {config.get('database_postgresql.host')}")
    print(f"   MariaDB charset: {config.get('database_mariadb.charset')}")
    
    print("\n3. 차트 설정:")
    print(f"   일봉 보관 기간: {config.get('charts.daily_retention_days')}일")
    print(f"   1분봉 보관 기간: {config.get('charts.minute_1_retention_days')}일")
    
    print("\n4. 설정 값 변경 테스트:")
    old_value = config.get('collection.default_count')
    config.set('collection.default_count', 2000)
    new_value = config.get('collection.default_count')
    print(f"   default_count 변경: {old_value} → {new_value}")
    
    # 원래 값으로 복구
    config.set('collection.default_count', old_value)
    
    print("\n5. 설정 유효성 검증:")
    errors = config.validate()
    if errors:
        print("   오류 발견:")
        for category, msgs in errors.items():
            for msg in msgs:
                print(f"     {category}: {msg}")
    else:
        print("   모든 설정이 유효합니다.")
    
    print("\n6. 설정 저장 테스트:")
    # 설정을 JSON으로 저장해보기
    config.save("./config/test_config.json")
    print("   설정이 JSON 파일로 저장되었습니다.")
    
    # 설정을 YAML으로 저장해보기
    config.save("./config/test_config.yaml")
    print("   설정이 YAML 파일로 저장되었습니다.")
    
    print("\n7. 설정 재로드 테스트:")
    reload_config("./config/creon_config.ini")
    print("   설정이 성공적으로 재로드되었습니다.")
    
    return config

def test_yaml_config():
    """YAML 설정 파일 테스트"""
    print("\n" + "=" * 60)
    print("YAML 설정 파일 테스트")
    print("=" * 60)
    
    # YAML 설정 파일 사용 (존재하는 경우)
    yaml_path = "./config/creon_config.yaml"
    if os.path.exists(yaml_path):
        config = get_config(yaml_path)
        print(f"YAML 설정 파일 로드: {yaml_path}")
        print(f"데이터베이스 타입: {config.get('storage.database_type')}")
    else:
        print("YAML 설정 파일이 없습니다.")

def test_config_export():
    """설정 내보내기 테스트"""
    print("\n" + "=" * 60)
    print("설정 내보내기 테스트")
    print("=" * 60)
    
    config = get_config()
    
    # 설정을 딕셔너리로 변환
    config_dict = config._dataclass_to_dict(config.config)
    
    # 중요한 정보 숨기기
    if 'database_postgresql' in config_dict:
        config_dict['database_postgresql']['password'] = '***HIDDEN***'
    if 'database_mariadb' in config_dict:
        config_dict['database_mariadb']['password'] = '***HIDDEN***'
    if 'notifications' in config_dict:
        config_dict['notifications']['smtp_password'] = '***HIDDEN***'
        config_dict['notifications']['telegram_bot_token'] = '***HIDDEN***'
    
    print("현재 설정 (일부 정보 숨김):")
    print(json.dumps(config_dict, ensure_ascii=False, indent=2))

def test_database_configs():
    """데이터베이스 설정 테스트"""
    print("\n" + "=" * 60)
    print("데이터베이스 설정 테스트")
    print("=" * 60)
    
    config = get_config()
    
    # 현재 데이터베이스 타입 확인
    db_type = config.get('storage.database_type', 'sqlite')
    print(f"현재 데이터베이스 타입: {db_type}")
    
    # 각 데이터베이스 타입별 설정 출력
    db_configs = {
        'sqlite': config.get('database_sqlite', {}),
        'postgresql': config.get('database_postgresql', {}),
        'mariadb': config.get('database_mariadb', {})
    }
    
    for db_name, db_config in db_configs.items():
        print(f"\n{db_name.upper()} 설정:")
        if isinstance(db_config, dict):
            for key, value in db_config.items():
                print(f"  {key}: {value}")
        else:
            print(f"  설정을 불러올 수 없습니다: {type(db_config)}")

def main():
    """메인 테스트 함수"""
    print("Creon DataReader 설정 모듈 테스트 시작")
    print("=" * 60)
    
    try:
        # INI 설정 테스트
        config = test_ini_config()
        
        # YAML 설정 테스트
        test_yaml_config()
        
        # 설정 내보내기 테스트
        test_config_export()
        
        # 데이터베이스 설정 테스트
        test_database_configs()
        
        print("\n" + "=" * 60)
        print("모든 테스트 완료!")
        print("=" * 60)
        
        # 테스트 파일 정리
        import os
        test_files = ["./config/test_config.json", "./config/test_config.yaml"]
        for file in test_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"테스트 파일 삭제: {file}")
        
    except Exception as e:
        print(f"\n테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())