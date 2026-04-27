# coding=utf-8
"""
Creon DataReader v2.0 - 메뉴 선택형 메인 실행 스크립트
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from typing import List, Optional, Dict, Any

from creon_config import get_config, reload_config
from creon_datareader import get_data_reader


def setup_logging():
    """로깅 설정"""
    config = get_config()
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.get('logging.level', 'INFO').upper()))
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 포맷터 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 핸들러
    if config.get('logging.console_enabled', True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # 파일 핸들러
    if config.get('logging.file_enabled', True):
        from logging.handlers import RotatingFileHandler
        
        import os
        log_path = config.get('logging.file_path', './logs')
        os.makedirs(log_path, exist_ok=True)
        
        log_file = os.path.join(log_path, 'creon_datareader.log')
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=config.get('logging.file_max_size', 10485760),
            backupCount=config.get('logging.file_backup_count', 5),
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def print_banner():
    """배너 출력"""
    banner = """
    ╔══════════════════════════════════════════════════════════╗
    ║                 Creon DataReader v2.0                    ║
    ║         한국 주식 데이터 수집 및 관리 시스템            ║
    ║                (메뉴 선택형 버전)                        ║
    ╚══════════════════════════════════════════════════════════╝
    """
    print(banner)


def clear_screen():
    """화면 지우기"""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_menu(title: str, options: List[Dict[str, Any]], current_selection: int = 0):
    """메뉴 출력"""
    clear_screen()
    print_banner()
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}\n")
    
    for i, option in enumerate(options):
        prefix = "▶ " if i == current_selection else "  "
        print(f"{prefix}{i+1}. {option['text']}")
    
    print(f"\n{'='*60}")
    print(" ↑↓: 이동, Enter: 선택, ESC: 이전 메뉴, Q: 종료")
    print(f"{'='*60}")


def get_menu_selection(title: str, options: List[Dict[str, Any]]) -> Optional[int]:
    """메뉴 선택 받기"""
    import msvcrt
    
    current_selection = 0
    total_options = len(options)
    
    while True:
        print_menu(title, options, current_selection)
        
        try:
            key = msvcrt.getch()
            
            # ESC 키 (27)
            if key == b'\x1b':
                return None
            
            # Q 키 (종료)
            if key in [b'q', b'Q']:
                print("\n프로그램을 종료합니다.")
                sys.exit(0)
            
            # 엔터 키 (13)
            if key == b'\r':
                return current_selection
            
            # 위쪽 화살표 (224 + 72)
            if key == b'\xe0':
                next_key = msvcrt.getch()
                if next_key == b'H':  # 위쪽
                    current_selection = (current_selection - 1) % total_options
                elif next_key == b'P':  # 아래쪽
                    current_selection = (current_selection + 1) % total_options
            
            # 숫자 키 (1-9)
            if key.isdigit():
                num = int(key) - 1
                if 0 <= num < total_options:
                    return num
        
        except KeyboardInterrupt:
            print("\n\n프로그램을 종료합니다.")
            sys.exit(0)
        except Exception as e:
            print(f"\n입력 오류: {e}")
            continue


def get_numeric_input(prompt: str, default: int = None, min_val: int = None, max_val: int = None) -> int:
    """숫자 입력 받기"""
    while True:
        try:
            input_str = input(f"\n{prompt} (기본값: {default}): ").strip()
            
            if not input_str and default is not None:
                return default
            
            value = int(input_str)
            
            if min_val is not None and value < min_val:
                print(f"최소값 {min_val} 이상 입력해주세요.")
                continue
            
            if max_val is not None and value > max_val:
                print(f"최대값 {max_val} 이하 입력해주세요.")
                continue
            
            return value
        
        except ValueError:
            print("숫자를 입력해주세요.")
        except KeyboardInterrupt:
            print("\n입력이 취소되었습니다.")
            return default if default is not None else 0


def get_chart_type_selection() -> Optional[Dict[str, Any]]:
    """차트 타입 선택"""
    options = [
        {'text': '일봉 데이터 수집', 'type': 'daily', 'interval': None},
        {'text': '1분봉 데이터 수집', 'type': 'minute', 'interval': 1},
        {'text': '3분봉 데이터 수집', 'type': 'minute', 'interval': 3},
        {'text': '5분봉 데이터 수집', 'type': 'minute', 'interval': 5},
        {'text': '10분봉 데이터 수집', 'type': 'minute', 'interval': 10},
        {'text': '15분봉 데이터 수집', 'type': 'minute', 'interval': 15},
        {'text': '이전 메뉴로 돌아가기', 'type': 'back', 'interval': None}
    ]
    
    selection = get_menu_selection("차트 타입 선택", options)
    
    if selection is None or options[selection]['type'] == 'back':
        return None
    
    return options[selection]


def get_max_count_for_interval(interval: int) -> int:
    """분봉별 최대 조회 가능 개수 반환"""
    config = get_config()
    max_counts_str = config.get('charts.max_data_counts', '')
    
    # "1:195000,3:162500,5:97500,10:48750,15:32500" 형식 파싱
    max_counts = {}
    for item in max_counts_str.split(','):
        if ':' in item:
            key, value = item.split(':')
            try:
                max_counts[int(key.strip())] = int(value.strip())
            except:
                continue
    
    return max_counts.get(interval, 100000)  # 기본값 100,000


def _build_filters(config) -> dict:
    """설정에서 필터 딕셔너리 생성 (MH_creon_datareader_down_20260106.py 필터링 로직 기반)"""
    filters_obj = config.get('filters')
    if filters_obj is None:
        return {
            'exclude_etf': True,
            'exclude_etn': True,
            'exclude_delisted': True,
            'exclude_keywords': [],
            'price_min': 0,
            'price_max': 0,
            'amount_min': 0,
            'amount_max': 0
        }
    if hasattr(filters_obj, 'get') and callable(getattr(filters_obj, 'get')):
        return {
            'exclude_etf': filters_obj.get('exclude_etf', True),
            'exclude_etn': filters_obj.get('exclude_etn', True),
            'exclude_delisted': filters_obj.get('exclude_delisted', True),
            'exclude_keywords': filters_obj.get('exclude_keywords', []),
            'price_min': filters_obj.get('price_min', 0),
            'price_max': filters_obj.get('price_max', 0),
            'amount_min': filters_obj.get('amount_min', 0),
            'amount_max': filters_obj.get('amount_max', 0)
        }
    return {
        'exclude_etf': getattr(filters_obj, 'exclude_etf', True),
        'exclude_etn': getattr(filters_obj, 'exclude_etn', True),
        'exclude_delisted': getattr(filters_obj, 'exclude_delisted', True),
        'exclude_keywords': getattr(filters_obj, 'exclude_keywords', []),
        'price_min': getattr(filters_obj, 'price_min', 0),
        'price_max': getattr(filters_obj, 'price_max', 0),
        'amount_min': getattr(filters_obj, 'amount_min', 0),
        'amount_max': getattr(filters_obj, 'amount_max', 0)
    }


def collect_data(chart_type: str, interval: int = None, max_stocks: Optional[int] = None):
    """데이터 수집"""
    config = get_config()
    
    if chart_type == 'daily':
        print(f"\n[일봉 데이터 수집]")
        chart_display = "일봉"
        max_count = 3650
    else:
        print(f"\n[분봉 데이터 수집] 간격: {interval}분")
        chart_display = f"{interval}분봉"
        max_count = get_max_count_for_interval(interval)
    
    print(f"최대 조회 가능 개수: {max_count:,}개")
    
    count = get_numeric_input(
        f"수집할 {chart_display} 데이터 개수 입력",
        default=1000,
        min_val=1,
        max_val=max_count
    )
    
    reader = get_data_reader()
    
    filters = _build_filters(config)
    
    markets = []
    if config.get('market_types.kospi', True):
        markets.append('KOSPI')
    if config.get('market_types.kosdaq', True):
        markets.append('KOSDAQ')
    if config.get('market_types.konex', True):
        markets.append('KONEX')
    
    if not markets:
        markets = ['KOSPI']
    
    total_success = 0
    total_stocks = 0
    total_data = 0
    
    for market in markets:
        print(f"\n[{market}]")
        
        results = reader.collect_market_data(
            market=market,
            chart_type=chart_type,
            interval=interval,
            max_stocks=max_stocks,
            merge=True,
            count=count,
            filters=filters
        )
        
        success_count = sum(1 for r in results if r.success)
        market_stocks = len(results)
        market_data = sum(r.data_count for r in results if r.success)
        
        total_success += success_count
        total_stocks += market_stocks
        total_data += market_data
    
    print(f"\n[완료] 종목 {total_success}/{total_stocks}, 데이터 {total_data:,}개")
    input("\nEnter 키를 누르세요...")


def show_summary():
    """데이터 요약 표시"""
    print(f"\n[데이터 요약]")
    
    reader = get_data_reader()
    config = get_config()
    
    # 모든 마켓 선택
    markets = []
    if config.get('market_types.kospi', True):
        markets.append('KOSPI')
    if config.get('market_types.kosdaq', True):
        markets.append('KOSDAQ')
    if config.get('market_types.konex', True):
        markets.append('KONEX')
    
    if not markets:
        markets = ['KOSPI']
    
    total_stocks = 0
    total_daily_data = 0
    total_minute_data = {}
    
    for market in markets:
        print(f"\n[마켓: {market}]")
        
        summary = reader.get_data_summary(market)
        
        print(f"  총 종목 수: {summary['total_stocks']}")
        print(f"  일봉 데이터: {summary['daily_data']['stocks']}종목, {summary['daily_data']['count']:,}개")
        
        print("  분봉 데이터:")
        for interval, count in summary['minute_data'].items():
            if count > 0:
                print(f"    {interval}: {count}종목")
                if interval not in total_minute_data:
                    total_minute_data[interval] = 0
                total_minute_data[interval] += count
        
        total_stocks += summary['total_stocks']
        total_daily_data += summary['daily_data']['count']
        
        if summary['db_stats']:
            print(f"  데이터베이스: {summary['db_stats'].get('daily_ohlcv_count', 0):,}개 일봉, "
                  f"{summary['db_stats'].get('minute_data_count', 0):,}개 분봉")
            
            if 'file_size_mb' in summary['db_stats']:
                print(f"  DB 파일 크기: {summary['db_stats']['file_size_mb']:.2f} MB")
    
    print(f"\n[전체 통계]")
    print(f"  총 종목: {total_stocks}개")
    print(f"  총 일봉 데이터: {total_daily_data:,}개")
    print("  분봉 데이터 종목 수:")
    for interval, count in sorted(total_minute_data.items()):
        print(f"    {interval}분봉: {count}종목")
    
    input("\n계속하려면 Enter 키를 누르세요...")


def run_cleanup():
    """정리 작업 실행"""
    print("\n[정리 작업 실행]")
    
    reader = get_data_reader()
    reader.cleanup()
    
    print("정리 작업 완료")
    input("\n계속하려면 Enter 키를 누르세요...")


def test_connection():
    """연결 테스트"""
    print("\n[연결 테스트]")
    
    reader = get_data_reader()
    
    if reader.api.check_connection():
        print("Creon PLUS 연결 성공")
        
        config = get_config()
        filters = _build_filters(config)
        
        markets = []
        if config.get('market_types.kospi', True):
            markets.append('KOSPI')
        if config.get('market_types.kosdaq', True):
            markets.append('KOSDAQ')
        if config.get('market_types.konex', True):
            markets.append('KONEX')
        
        total_stocks = 0
        for market in markets:
            try:
                stocks = reader.api.get_market_codes(market, filters)
                print(f"✓ {market} 종목 수: {len(stocks):,}개")
                total_stocks += len(stocks)
                
                if stocks:
                    sample = stocks[0]
                    print(f"  샘플 종목: {sample['code']} - {sample['name']}")
                
            except Exception as e:
                print(f"✗ {market} 종목 코드 조회 실패: {e}")
        
        print(f"\n✓ 전체 종목 수: {total_stocks:,}개")
        
        input("\n계속하려면 Enter 키를 누르세요...")
        return True
        
    else:
        print("✗ Creon PLUS 연결 실패")
        input("\n계속하려면 Enter 키를 누르세요...")
        return False


def show_config():
    """설정 확인"""
    config = get_config()
    print("\n[현재 설정]")
    
    # 주요 설정 출력
    print(f"  파일 저장: {'활성화' if config.get('storage.file_enabled') else '비활성화'}")
    print(f"  DB 저장: {'활성화' if config.get('storage.database_enabled') else '비활성화'}")
    print(f"  병합 기능: {'활성화' if config.get('merge.enabled') else '비활성화'}")
    print(f"  기본 수집 개수: {config.get('collection.default_count')}")
    print(f"  병합 lookback: {config.get('merge.lookback_days')}일")
    
    # 마켓 설정
    print(f"  KOSPI 수집: {'예' if config.get('market_types.kospi') else '아니오'}")
    print(f"  KOSDAQ 수집: {'예' if config.get('market_types.kosdaq') else '아니오'}")
    print(f"  KONEX 수집: {'예' if config.get('market_types.konex') else '아니오'}")
    
    # 필터 설정
    filters = config.get('filters', {})
    print(f"  가격 필터: {filters.get('price_min', 0)} ~ {filters.get('price_max', 0)}")
    print(f"  ETF 제외: {'예' if filters.get('exclude_etf') else '아니오'}")
    print(f"  ETN 제외: {'예' if filters.get('exclude_etn') else '아니오'}")
    
    # 분봉별 최대 조회 개수
    max_counts_str = config.get('charts.max_data_counts', '')
    print("\n  분봉별 최대 조회 가능 개수:")
    for item in max_counts_str.split(','):
        if ':' in item:
            interval, count = item.split(':')
            print(f"    {interval}분봉: {int(count):,}개")
    
    input("\n계속하려면 Enter 키를 누르세요...")


def main_menu():
    """메인 메뉴"""
    while True:
        options = [
            {'text': '데이터 수집', 'action': 'collect'},
            {'text': '데이터 요약', 'action': 'summary'},
            {'text': '정리 작업', 'action': 'cleanup'},
            {'text': '연결 테스트', 'action': 'test'},
            {'text': '설정 확인', 'action': 'config'},
            {'text': '프로그램 종료', 'action': 'exit'}
        ]
        
        selection = get_menu_selection("메인 메뉴", options)
        
        if selection is None:
            continue
        
        action = options[selection]['action']
        
        if action == 'exit':
            print("\n프로그램을 종료합니다.")
            sys.exit(0)
        
        elif action == 'collect':
            chart_selection = get_chart_type_selection()
            if chart_selection and chart_selection['type'] != 'back':
                if test_connection():
                    collect_data(
                        chart_selection['type'],
                        chart_selection['interval']
                    )
        
        elif action == 'summary':
            if test_connection():
                show_summary()
        
        elif action == 'cleanup':
            run_cleanup()
        
        elif action == 'test':
            test_connection()
        
        elif action == 'config':
            show_config()


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='Creon DataReader v2.0 - 메뉴 선택형')
    
    # 옵션 정의
    parser.add_argument('--config', '-c',
                       help='설정 파일 경로')
    
    args = parser.parse_args()
    
    # 설정 파일 로드
    if args.config:
        reload_config(args.config)
    else:
        get_config()
    
    # 로깅 설정
    setup_logging()
    
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\n프로그램을 종료합니다.")
        sys.exit(0)
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
        input("\n계속하려면 Enter 키를 누르세요...")
        sys.exit(1)


if __name__ == "__main__":
    main()