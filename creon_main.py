# coding=utf-8
"""
Creon DataReader v2.0 - 메인 실행 스크립트
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Optional

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
    ╚══════════════════════════════════════════════════════════╝
    """
    print(banner)


def collect_daily_data(market: str = 'KOSPI', max_stocks: Optional[int] = None):
    """일봉 데이터 수집"""
    print(f"\n[일봉 데이터 수집] 마켓: {market}")
    
    reader = get_data_reader()
    
    def progress_callback(message: str, progress: float = None):
        if progress is not None:
            print(f"  [{progress:.1f}%] {message}")
        else:
            print(f"  {message}")
    
    reader.set_progress_callback(progress_callback)
    
    # 데이터 수집
    results = reader.collect_market_data(
        market=market,
        chart_type='daily',
        max_stocks=max_stocks,
        merge=True
    )
    
    # 결과 통계
    success_count = sum(1 for r in results if r.success)
    total_count = len(results)
    total_data = sum(r.data_count for r in results if r.success)
    
    print(f"\n[수집 완료] 성공: {success_count}/{total_count}, 총 데이터: {total_data}개")
    
    # 실패한 종목 출력
    failed_stocks = [r.stock_code for r in results if not r.success]
    if failed_stocks:
        print(f"[실패 종목] {', '.join(failed_stocks[:10])}" + 
              (f" 외 {len(failed_stocks)-10}개" if len(failed_stocks) > 10 else ""))


def collect_minute_data(market: str = 'KOSPI', interval: int = 5, max_stocks: Optional[int] = None):
    """분봉 데이터 수집"""
    print(f"\n[분봉 데이터 수집] 마켓: {market}, 간격: {interval}분")
    
    reader = get_data_reader()
    
    def progress_callback(message: str, progress: float = None):
        if progress is not None:
            print(f"  [{progress:.1f}%] {message}")
        else:
            print(f"  {message}")
    
    reader.set_progress_callback(progress_callback)
    
    # 데이터 수집
    results = reader.collect_market_data(
        market=market,
        chart_type='minute',
        interval=interval,
        max_stocks=max_stocks,
        merge=True
    )
    
    # 결과 통계
    success_count = sum(1 for r in results if r.success)
    total_count = len(results)
    total_data = sum(r.data_count for r in results if r.success)
    
    print(f"\n[수집 완료] 성공: {success_count}/{total_count}, 총 데이터: {total_data}개")


def show_summary(market: str = 'KOSPI'):
    """데이터 요약 표시"""
    print(f"\n[데이터 요약] 마켓: {market}")
    
    reader = get_data_reader()
    summary = reader.get_data_summary(market)
    
    print(f"  총 종목 수: {summary['total_stocks']}")
    print(f"  일봉 데이터: {summary['daily_data']['stocks']}종목, {summary['daily_data']['count']}개")
    
    print("  분봉 데이터:")
    for interval, count in summary['minute_data'].items():
        if count > 0:
            print(f"    {interval}: {count}종목")
    
    if summary['db_stats']:
        print(f"  데이터베이스: {summary['db_stats'].get('daily_ohlcv_count', 0)}개 일봉, "
              f"{summary['db_stats'].get('minute_data_count', 0)}개 분봉")
        
        if 'file_size_mb' in summary['db_stats']:
            print(f"  DB 파일 크기: {summary['db_stats']['file_size_mb']:.2f} MB")


def run_cleanup():
    """정리 작업 실행"""
    print("\n[정리 작업 실행]")
    
    reader = get_data_reader()
    reader.cleanup()
    
    print("정리 작업 완료")


def test_connection():
    """연결 테스트"""
    print("\n[연결 테스트]")
    
    reader = get_data_reader()
    
    if reader.api.check_connection():
        print("✓ Creon PLUS 연결 성공")
        
        # 종목 코드 테스트
        try:
            stocks = reader.api.get_market_codes('KOSPI', {
                'exclude_etf': True,
                'exclude_etn': True,
                'exclude_delisted': True
            })
            print(f"✓ KOSPI 종목 수: {len(stocks)}")
            
            if stocks:
                sample = stocks[0]
                print(f"✓ 샘플 종목: {sample['code']} - {sample['name']}")
            
            return True
            
        except Exception as e:
            print(f"✗ 종목 코드 조회 실패: {e}")
            return False
    else:
        print("✗ Creon PLUS 연결 실패")
        return False


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='Creon DataReader v2.0')
    
    # 명령어 정의
    parser.add_argument('command', nargs='?', default='help',
                       choices=['help', 'daily', 'minute', 'summary', 
                               'cleanup', 'test', 'config'],
                       help='실행할 명령어')
    
    # 옵션 정의
    parser.add_argument('--market', '-m', default='KOSPI',
                       choices=['KOSPI', 'KOSDAQ', 'KONEX'],
                       help='마켓 선택 (기본: KOSPI)')
    
    parser.add_argument('--interval', '-i', type=int, default=5,
                       choices=[1, 5, 30, 60],
                       help='분봉 간격 (기본: 5)')
    
    parser.add_argument('--max-stocks', '-n', type=int,
                       help='처리할 최대 종목 수')
    
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
    
    # 배너 출력
    print_banner()
    
    # 명령어 실행
    if args.command == 'help':
        print("\n사용법:")
        print("  python creon_main.py [명령어] [옵션]")
        print("\n명령어:")
        print("  help      - 도움말 출력")
        print("  daily     - 일봉 데이터 수집")
        print("  minute    - 분봉 데이터 수집")
        print("  summary   - 데이터 요약 표시")
        print("  cleanup   - 오래된 데이터 정리")
        print("  test      - 연결 테스트")
        print("  config    - 설정 확인")
        print("\n옵션:")
        print("  --market, -m     마켓 선택 (KOSPI, KOSDAQ, KONEX)")
        print("  --interval, -i   분봉 간격 (1, 5, 30, 60)")
        print("  --max-stocks, -n 처리할 최대 종목 수")
        print("  --config, -c     설정 파일 경로")
        print("\n예시:")
        print("  python creon_main.py daily --market KOSPI --max-stocks 10")
        print("  python creon_main.py minute --interval 5 --market KOSDAQ")
        print("  python creon_main.py summary --market KOSPI")
        print("  python creon_main.py test")
    
    elif args.command == 'test':
        test_connection()
    
    elif args.command == 'daily':
        if test_connection():
            collect_daily_data(args.market, args.max_stocks)
    
    elif args.command == 'minute':
        if test_connection():
            collect_minute_data(args.market, args.interval, args.max_stocks)
    
    elif args.command == 'summary':
        if test_connection():
            show_summary(args.market)
    
    elif args.command == 'cleanup':
        run_cleanup()
    
    elif args.command == 'config':
        config = get_config()
        print("\n[현재 설정]")
        
        # 주요 설정 출력
        print(f"  파일 저장: {'활성화' if config.get('storage.file_enabled') else '비활성화'}")
        print(f"  DB 저장: {'활성화' if config.get('storage.database_enabled') else '비활성화'}")
        print(f"  병합 기능: {'활성화' if config.get('merge.enabled') else '비활성화'}")
        print(f"  기본 수집 개수: {config.get('collection.default_count')}")
        print(f"  병합 lookback: {config.get('merge.lookback_days')}일")
        
        # 필터 설정
        filters = config.get('filters', {})
        print(f"  가격 필터: {filters.get('price_min', 0)} ~ {filters.get('price_max', 0)}")
        print(f"  ETF 제외: {'예' if filters.get('exclude_etf') else '아니오'}")
        print(f"  ETN 제외: {'예' if filters.get('exclude_etn') else '아니오'}")
    
    print("\n작업 완료")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n작업이 사용자에 의해 중단되었습니다.")
        sys.exit(0)
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)