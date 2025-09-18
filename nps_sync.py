import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import json
from datetime import datetime
from nps_cache import NPSCache
from typing import Dict, List, Optional

# 환경 변수 로드
load_dotenv('config.env')

class NPSSync:
    """
    SQLite 캐시와 Supabase 동기화 관리 클래스
    - 로컬 캐시 우선 조회
    - 캐시 미스 시 Supabase 조회 후 캐시 저장
    - 백그라운드 동기화 처리
    """
    
    def __init__(self):
        self.cache = NPSCache()
        self.supabase_connection = None
        self.connect_supabase()
    
    def connect_supabase(self):
        """Supabase 연결"""
        try:
            self.supabase_connection = psycopg2.connect(
                host=os.getenv('SUPABASE_DB_HOST'),
                database=os.getenv('SUPABASE_DB_NAME'),
                user=os.getenv('SUPABASE_DB_USER'),
                password=os.getenv('SUPABASE_DB_PASSWORD'),
                port=os.getenv('SUPABASE_DB_PORT', 5432)
            )
            print("✅ Supabase 연결 성공")
        except Exception as e:
            print(f"❌ Supabase 연결 실패: {e}")
            # Supabase 연결 실패 시에도 로컬 캐시는 사용 가능
            self.supabase_connection = None
    
    def get_workplace_data(self, wkpl_nm: str, bzowr_rgst_no: str = None) -> List[Dict]:
        """
        사업장 데이터 조회 (캐시 우선, Supabase 백업)
        1. SQLite 캐시에서 조회
        2. 캐시 미스 시 Supabase에서 조회
        3. Supabase 결과를 캐시에 저장
        """
        # 1단계: 로컬 캐시에서 조회
        cached_data = self.cache.get_workplace_cache(wkpl_nm, bzowr_rgst_no)
        
        if cached_data:
            print(f"📦 로컬 캐시에서 {len(cached_data)}개 사업장 정보 조회")
            return cached_data
        
        # 2단계: 캐시 미스 시 Supabase에서 조회
        if not self.supabase_connection:
            print("❌ Supabase 연결 없음 - 캐시된 데이터만 사용 가능")
            return []
        
        print(f"🌐 Supabase에서 사업장 정보 조회: {wkpl_nm}")
        supabase_data = self._get_workplace_from_supabase(wkpl_nm, bzowr_rgst_no)
        
        if supabase_data:
            # 3단계: Supabase 결과를 로컬 캐시에 저장
            for data in supabase_data:
                self.cache.set_workplace_cache(data, 'insert')
            
            print(f"💾 {len(supabase_data)}개 사업장 정보를 로컬 캐시에 저장")
            return supabase_data
        
        return []
    
    def _get_workplace_from_supabase(self, wkpl_nm: str, bzowr_rgst_no: str = None) -> List[Dict]:
        """Supabase에서 사업장 데이터 조회"""
        try:
            cursor = self.supabase_connection.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT 
                    b.wkpl_nm, b.bzowr_rgst_no, b.seq, b.data_crt_ym, b.wkpl_road_nm_dtl_addr,
                    d.jnngp_cnt, d.crrmm_ntc_amt, d.avg_monthly_salary,
                    m.nw_acqzr_cnt, m.lss_jnngp_cnt
                FROM workplace_base_info b
                LEFT JOIN workplace_detail_info d ON b.seq = d.seq
                LEFT JOIN workplace_monthly_status m ON b.seq = m.seq
                WHERE b.wkpl_nm = %s
            """
            params = [wkpl_nm]
            
            if bzowr_rgst_no:
                query += " AND b.bzowr_rgst_no = %s"
                params.append(bzowr_rgst_no)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                print(f"🌐 Supabase에서 {len(results)}개 사업장 정보 조회")
                return [dict(row) for row in results]
            
            return []
            
        except Exception as e:
            print(f"❌ Supabase 조회 실패: {e}")
            return []
    
    def save_workplace_data(self, workplace_data: Dict, operation: str = 'insert'):
        """
        사업장 데이터 저장 (로컬 캐시 + Supabase 동기화)
        1. 로컬 캐시에 즉시 저장
        2. Supabase 동기화 작업을 백그라운드에서 처리
        """
        # 1단계: 로컬 캐시에 즉시 저장
        self.cache.set_workplace_cache(workplace_data, operation)
        
        # 2단계: Supabase 동기화 (백그라운드)
        if self.supabase_connection:
            try:
                self._sync_to_supabase(workplace_data, operation)
                print(f"✅ Supabase 동기화 완료: {workplace_data.get('wkplNm')}")
            except Exception as e:
                print(f"❌ Supabase 동기화 실패: {e}")
                # 동기화 실패 시 로그에 기록 (나중에 재시도 가능)
        else:
            print("⚠️ Supabase 연결 없음 - 로컬 캐시에만 저장됨")
    
    def _sync_to_supabase(self, workplace_data: Dict, operation: str):
        """Supabase에 데이터 동기화"""
        cursor = self.supabase_connection.cursor()
        
        if operation == 'insert' or operation == 'update':
            # 기본 정보 저장/업데이트
            cursor.execute("""
                INSERT INTO workplace_base_info 
                (wkpl_nm, bzowr_rgst_no, seq, data_crt_ym, wkpl_road_nm_dtl_addr)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (seq) 
                DO UPDATE SET 
                    wkpl_nm = EXCLUDED.wkpl_nm,
                    data_crt_ym = EXCLUDED.data_crt_ym,
                    wkpl_road_nm_dtl_addr = EXCLUDED.wkpl_road_nm_dtl_addr,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                workplace_data.get('wkplNm'),
                workplace_data.get('bzowrRgstNo'),
                workplace_data.get('seq'),
                workplace_data.get('dataCrtYm'),
                workplace_data.get('wkplRoadNmDtlAddr')
            ))
            
            # 상세 정보 저장/업데이트
            if any(workplace_data.get(key) for key in ['jnngpCnt', 'crrmmNtcAmt', 'avgMonthlySalary']):
                cursor.execute("""
                    INSERT INTO workplace_detail_info 
                    (seq, jnngp_cnt, crrmm_ntc_amt, avg_monthly_salary)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (seq) 
                    DO UPDATE SET 
                        jnngp_cnt = EXCLUDED.jnngp_cnt,
                        crrmm_ntc_amt = EXCLUDED.crrmm_ntc_amt,
                        avg_monthly_salary = EXCLUDED.avg_monthly_salary,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    workplace_data.get('seq'),
                    workplace_data.get('jnngpCnt'),
                    workplace_data.get('crrmmNtcAmt'),
                    workplace_data.get('avgMonthlySalary')
                ))
            
            # 월별 현황 저장/업데이트
            if any(workplace_data.get(key) for key in ['nwAcqzrCnt', 'lssJnngpCnt']):
                cursor.execute("""
                    INSERT INTO workplace_monthly_status 
                    (seq, nw_acqzr_cnt, lss_jnngp_cnt)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (seq) 
                    DO UPDATE SET 
                        nw_acqzr_cnt = EXCLUDED.nw_acqzr_cnt,
                        lss_jnngp_cnt = EXCLUDED.lss_jnngp_cnt,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    workplace_data.get('seq'),
                    workplace_data.get('nwAcqzrCnt'),
                    workplace_data.get('lssJnngpCnt')
                ))
        
        elif operation == 'delete':
            # 삭제 작업
            cursor.execute("DELETE FROM workplace_monthly_status WHERE seq = %s", (workplace_data.get('seq'),))
            cursor.execute("DELETE FROM workplace_detail_info WHERE seq = %s", (workplace_data.get('seq'),))
            cursor.execute("DELETE FROM workplace_base_info WHERE seq = %s", (workplace_data.get('seq'),))
        
        self.supabase_connection.commit()
    
    def get_api_data_with_cache(self, api_type: str, params: Dict, api_function, expires_hours: int = 24) -> Optional[Dict]:
        """
        API 데이터 조회 (캐시 우선)
        1. 로컬 캐시에서 조회
        2. 캐시 미스 시 API 호출
        3. API 결과를 캐시에 저장
        """
        # 1단계: 로컬 캐시에서 조회
        cached_data = self.cache.get_api_cache(api_type, params)
        
        if cached_data:
            return cached_data
        
        # 2단계: 캐시 미스 시 API 호출
        print(f"🌐 API 호출: {api_type}")
        api_data = api_function(params)
        
        if api_data:
            # 3단계: API 결과를 캐시에 저장
            self.cache.set_api_cache(api_type, params, api_data, expires_hours)
            return api_data
        
        return None
    
    def sync_pending_operations(self):
        """대기 중인 동기화 작업들을 처리"""
        pending_ops = self.cache.get_pending_sync_operations()
        
        if not pending_ops:
            print("📋 동기화 대기 작업 없음")
            return
        
        if not self.supabase_connection:
            print("❌ Supabase 연결 없음 - 동기화 불가")
            return
        
        print(f"🔄 {len(pending_ops)}개 동기화 작업 처리 시작")
        
        for op in pending_ops:
            try:
                sync_id = op['id']
                table_name = op['table_name']
                operation = op['operation']
                record_id = op['record_id']
                
                if table_name == 'workplace_cache':
                    # 사업장 데이터 동기화
                    if operation == 'delete':
                        workplace_data = {'seq': record_id}
                    else:
                        # data_after에서 데이터 복원
                        workplace_data = json.loads(op['data_after']) if op['data_after'] else {}
                    
                    self._sync_to_supabase(workplace_data, operation)
                    self.cache.mark_sync_completed(sync_id, success=True)
                    print(f"✅ 동기화 완료: {operation} - {record_id}")
                
            except Exception as e:
                print(f"❌ 동기화 실패: {op['id']} - {e}")
                self.cache.mark_sync_completed(sync_id, success=False, error_message=str(e))
        
        print("🔄 동기화 작업 처리 완료")
    
    def get_cache_stats(self) -> Dict:
        """캐시 통계 정보 조회"""
        cache_stats = self.cache.get_cache_stats()
        
        # Supabase 연결 상태 추가
        cache_stats['supabase_connected'] = self.supabase_connection is not None
        
        return cache_stats
    
    def cleanup_cache(self):
        """캐시 정리"""
        self.cache.cleanup_expired_cache()
    
    def close(self):
        """연결 종료"""
        self.cache.close()
        if self.supabase_connection:
            self.supabase_connection.close()
            print("🔌 Supabase 연결 종료")

# 사용 예시
if __name__ == "__main__":
    # 동기화 시스템 테스트
    sync = NPSSync()
    
    # 통계 정보 출력
    stats = sync.get_cache_stats()
    print("📊 동기화 시스템 통계:")
    print(f"  - Supabase 연결: {'✅' if stats['supabase_connected'] else '❌'}")
    print(f"  - API 캐시: {stats['api_cache_count']}개")
    print(f"  - 사업장 캐시: {stats['workplace_cache_count']}개")
    print(f"  - 동기화 대기: {stats['pending_sync_count']}개")
    
    # 대기 중인 동기화 작업 처리
    sync.sync_pending_operations()
    
    sync.close()
