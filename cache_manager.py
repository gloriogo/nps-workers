#!/usr/bin/env python3
"""
NPS 캐시 관리 유틸리티
- 캐시 통계 조회
- 캐시 정리
- 동기화 상태 확인
- 수동 동기화 실행
"""

import argparse
import sys
from nps_cache import NPSCache
from nps_sync import NPSSync

def show_cache_stats():
    """캐시 통계 정보 출력"""
    print("📊 NPS 캐시 시스템 통계")
    print("=" * 50)
    
    # 로컬 캐시 통계
    cache = NPSCache()
    cache_stats = cache.get_cache_stats()
    
    print(f"🗄️  로컬 SQLite 캐시:")
    print(f"   - API 캐시: {cache_stats['api_cache_count']}개")
    print(f"   - 사업장 캐시: {cache_stats['workplace_cache_count']}개")
    print(f"   - 동기화 대기: {cache_stats['pending_sync_count']}개")
    
    if cache_stats['top_accessed_workplaces']:
        print(f"   - 가장 많이 조회된 사업장:")
        for i, (name, count) in enumerate(cache_stats['top_accessed_workplaces'], 1):
            print(f"     {i}. {name} ({count}회)")
    
    cache.close()
    
    # Supabase 동기화 통계
    try:
        sync = NPSSync()
        sync_stats = sync.get_cache_stats()
        
        print(f"\n🌐 Supabase 동기화:")
        print(f"   - 연결 상태: {'✅ 연결됨' if sync_stats['supabase_connected'] else '❌ 연결 안됨'}")
        
        sync.close()
    except Exception as e:
        print(f"\n🌐 Supabase 동기화: ❌ 연결 실패 - {e}")

def cleanup_cache():
    """캐시 정리"""
    print("🧹 캐시 정리 시작...")
    
    cache = NPSCache()
    cache.cleanup_expired_cache()
    cache.close()
    
    print("✅ 캐시 정리 완료")

def sync_pending():
    """대기 중인 동기화 작업 처리"""
    print("🔄 대기 중인 동기화 작업 처리...")
    
    try:
        sync = NPSSync()
        sync.sync_pending_operations()
        sync.close()
        print("✅ 동기화 작업 완료")
    except Exception as e:
        print(f"❌ 동기화 실패: {e}")

def search_workplace(name):
    """사업장 검색"""
    print(f"🔍 사업장 검색: {name}")
    
    cache = NPSCache()
    results = cache.get_workplace_cache(name)
    
    if results:
        print(f"📦 로컬 캐시에서 {len(results)}개 사업장 발견:")
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['wkpl_nm']}")
            print(f"   - 식별번호: {result['seq']}")
            print(f"   - 주소: {result['wkpl_road_nm_dtl_addr']}")
            print(f"   - 가입자수: {result['jnngp_cnt']}")
            print(f"   - 마지막 접근: {result['last_accessed']}")
            print(f"   - 접근 횟수: {result['access_count']}회")
    else:
        print("❌ 로컬 캐시에서 해당 사업장을 찾을 수 없습니다.")
    
    cache.close()

def clear_all_cache():
    """모든 캐시 삭제"""
    print("⚠️  모든 캐시를 삭제하시겠습니까? (y/N): ", end="")
    confirm = input().strip().lower()
    
    if confirm == 'y':
        import os
        cache_files = ['nps_cache.db']
        
        for file in cache_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️  {file} 삭제 완료")
        
        print("✅ 모든 캐시 삭제 완료")
    else:
        print("❌ 캐시 삭제 취소됨")

def main():
    parser = argparse.ArgumentParser(description='NPS 캐시 관리 유틸리티')
    parser.add_argument('command', choices=[
        'stats', 'cleanup', 'sync', 'search', 'clear'
    ], help='실행할 명령어')
    parser.add_argument('--name', help='검색할 사업장명 (search 명령어 사용시)')
    
    args = parser.parse_args()
    
    if args.command == 'stats':
        show_cache_stats()
    elif args.command == 'cleanup':
        cleanup_cache()
    elif args.command == 'sync':
        sync_pending()
    elif args.command == 'search':
        if not args.name:
            print("❌ --name 옵션을 사용하여 사업장명을 입력해주세요.")
            sys.exit(1)
        search_workplace(args.name)
    elif args.command == 'clear':
        clear_all_cache()

if __name__ == "__main__":
    main()
