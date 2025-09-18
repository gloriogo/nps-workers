import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Any
import threading

class NPSCache:
    """
    SQLite 로컬 캐시 시스템
    - 로컬에서 빠른 조회
    - Supabase 조회 횟수 최소화
    - SQLite와 Supabase 동기화
    """
    
    def __init__(self, db_path: str = "nps_cache.db"):
        self.db_path = db_path
        self.lock = threading.Lock()  # 동시 접근 방지
        self.init_database()
    
    def init_database(self):
        """SQLite 데이터베이스 초기화 및 테이블 생성"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # API 캐시 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_hash TEXT UNIQUE NOT NULL,
                    api_type TEXT NOT NULL,
                    request_params TEXT NOT NULL,
                    response_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 1
                )
            """)
            
            # 사업장 기본 정보 캐시 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workplace_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq TEXT UNIQUE NOT NULL,
                    wkpl_nm TEXT NOT NULL,
                    bzowr_rgst_no TEXT,
                    data_crt_ym TEXT,
                    wkpl_road_nm_dtl_addr TEXT,
                    jnngp_cnt INTEGER,
                    crrmm_ntc_amt INTEGER,
                    avg_monthly_salary REAL,
                    nw_acqzr_cnt INTEGER,
                    lss_jnngp_cnt INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 1,
                    sync_status TEXT DEFAULT 'pending'  -- pending, synced, error
                )
            """)
            
            # 동기화 로그 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,  -- insert, update, delete
                    record_id TEXT NOT NULL,
                    data_before TEXT,
                    data_after TEXT,
                    sync_status TEXT DEFAULT 'pending',  -- pending, synced, error
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    synced_at TIMESTAMP
                )
            """)
            
            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_cache_hash ON api_cache(request_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_workplace_seq ON workplace_cache(seq)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_workplace_name ON workplace_cache(wkpl_nm)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sync_log_status ON sync_log(sync_status)")
            
            conn.commit()
            conn.close()
            print("✅ SQLite 캐시 데이터베이스 초기화 완료")
    
    def generate_request_hash(self, api_type: str, params: Dict) -> str:
        """API 요청 파라미터로부터 해시 생성"""
        sorted_params = sorted(params.items())
        param_string = f"{api_type}:{json.dumps(sorted_params, sort_keys=True)}"
        return hashlib.sha256(param_string.encode()).hexdigest()
    
    def get_api_cache(self, api_type: str, params: Dict) -> Optional[Dict]:
        """API 캐시에서 데이터 조회"""
        with self.lock:
            request_hash = self.generate_request_hash(api_type, params)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT response_data, expires_at FROM api_cache 
                WHERE request_hash = ? 
                AND (expires_at IS NULL OR expires_at > ?)
            """, (request_hash, datetime.now()))
            
            result = cursor.fetchone()
            
            if result:
                # 접근 횟수 증가 및 마지막 접근 시간 업데이트
                cursor.execute("""
                    UPDATE api_cache 
                    SET access_count = access_count + 1, 
                        last_accessed = CURRENT_TIMESTAMP
                    WHERE request_hash = ?
                """, (request_hash,))
                conn.commit()
                
                print(f"📦 SQLite 캐시에서 API 데이터 조회: {api_type}")
                conn.close()
                return json.loads(result[0])
            
            conn.close()
            return None
    
    def set_api_cache(self, api_type: str, params: Dict, response_data: Dict, expires_hours: int = 24):
        """API 응답 데이터를 캐시에 저장"""
        with self.lock:
            request_hash = self.generate_request_hash(api_type, params)
            expires_at = datetime.now() + timedelta(hours=expires_hours)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO api_cache 
                (request_hash, api_type, request_params, response_data, expires_at, last_accessed, access_count)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
            """, (
                request_hash, 
                api_type, 
                json.dumps(params), 
                json.dumps(response_data), 
                expires_at
            ))
            
            conn.commit()
            conn.close()
            print(f"💾 SQLite 캐시에 API 데이터 저장: {api_type}")
    
    def get_workplace_cache(self, wkpl_nm: str, bzowr_rgst_no: str = None) -> List[Dict]:
        """사업장 정보 캐시에서 조회"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
            cursor = conn.cursor()
            
            if bzowr_rgst_no:
                cursor.execute("""
                    SELECT * FROM workplace_cache 
                    WHERE wkpl_nm = ? AND bzowr_rgst_no = ?
                """, (wkpl_nm, bzowr_rgst_no))
            else:
                cursor.execute("""
                    SELECT * FROM workplace_cache 
                    WHERE wkpl_nm = ?
                """, (wkpl_nm,))
            
            results = cursor.fetchall()
            
            if results:
                # 접근 횟수 증가 및 마지막 접근 시간 업데이트
                for result in results:
                    cursor.execute("""
                        UPDATE workplace_cache 
                        SET access_count = access_count + 1, 
                            last_accessed = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (result['id'],))
                
                conn.commit()
                print(f"📦 SQLite 캐시에서 {len(results)}개 사업장 정보 조회")
                conn.close()
                return [dict(row) for row in results]
            
            conn.close()
            return []
    
    def set_workplace_cache(self, workplace_data: Dict, operation: str = 'insert'):
        """사업장 정보를 캐시에 저장/업데이트"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 기존 데이터 조회 (업데이트/삭제 시 비교용)
            cursor.execute("SELECT * FROM workplace_cache WHERE seq = ?", (workplace_data.get('seq'),))
            existing_data = cursor.fetchone()
            
            if operation == 'insert' or operation == 'update':
                cursor.execute("""
                    INSERT OR REPLACE INTO workplace_cache 
                    (seq, wkpl_nm, bzowr_rgst_no, data_crt_ym, wkpl_road_nm_dtl_addr,
                     jnngp_cnt, crrmm_ntc_amt, avg_monthly_salary, nw_acqzr_cnt, lss_jnngp_cnt,
                     updated_at, sync_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'pending')
                """, (
                    workplace_data.get('seq'),
                    workplace_data.get('wkplNm'),
                    workplace_data.get('bzowrRgstNo'),
                    workplace_data.get('dataCrtYm'),
                    workplace_data.get('wkplRoadNmDtlAddr'),
                    workplace_data.get('jnngpCnt'),
                    workplace_data.get('crrmmNtcAmt'),
                    workplace_data.get('avgMonthlySalary'),
                    workplace_data.get('nwAcqzrCnt'),
                    workplace_data.get('lssJnngpCnt')
                ))
                
                # 동기화 로그 기록
                self._log_sync_operation('workplace_cache', operation, workplace_data.get('seq'), 
                                       existing_data, workplace_data)
                
            elif operation == 'delete':
                cursor.execute("DELETE FROM workplace_cache WHERE seq = ?", (workplace_data.get('seq'),))
                
                # 동기화 로그 기록
                self._log_sync_operation('workplace_cache', operation, workplace_data.get('seq'), 
                                       existing_data, None)
            
            conn.commit()
            conn.close()
            print(f"💾 SQLite 캐시에 사업장 데이터 {operation}: {workplace_data.get('wkplNm')}")
    
    def _log_sync_operation(self, table_name: str, operation: str, record_id: str, 
                          data_before: Any, data_after: Any):
        """동기화 작업 로그 기록"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO sync_log 
            (table_name, operation, record_id, data_before, data_after, sync_status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        """, (
            table_name,
            operation,
            record_id,
            json.dumps(data_before) if data_before else None,
            json.dumps(data_after) if data_after else None
        ))
        
        conn.commit()
        conn.close()
    
    def get_pending_sync_operations(self) -> List[Dict]:
        """동기화 대기 중인 작업들 조회"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM sync_log 
                WHERE sync_status = 'pending' 
                ORDER BY created_at ASC
            """)
            
            results = cursor.fetchall()
            conn.close()
            return [dict(row) for row in results]
    
    def mark_sync_completed(self, sync_id: int, success: bool = True, error_message: str = None):
        """동기화 작업 완료 표시"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if success:
                cursor.execute("""
                    UPDATE sync_log 
                    SET sync_status = 'synced', synced_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (sync_id,))
                
                # workplace_cache의 sync_status도 업데이트
                cursor.execute("""
                    SELECT record_id FROM sync_log WHERE id = ?
                """, (sync_id,))
                result = cursor.fetchone()
                if result:
                    cursor.execute("""
                        UPDATE workplace_cache 
                        SET sync_status = 'synced'
                        WHERE seq = ?
                    """, (result[0],))
            else:
                cursor.execute("""
                    UPDATE sync_log 
                    SET sync_status = 'error', error_message = ?
                    WHERE id = ?
                """, (error_message, sync_id))
            
            conn.commit()
            conn.close()
    
    def cleanup_expired_cache(self):
        """만료된 캐시 데이터 정리"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 만료된 API 캐시 삭제
            cursor.execute("DELETE FROM api_cache WHERE expires_at < ?", (datetime.now(),))
            api_deleted = cursor.rowcount
            
            # 오래된 동기화 로그 정리 (30일 이상)
            old_date = datetime.now() - timedelta(days=30)
            cursor.execute("DELETE FROM sync_log WHERE created_at < ? AND sync_status = 'synced'", (old_date,))
            log_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            if api_deleted > 0 or log_deleted > 0:
                print(f"🧹 캐시 정리 완료: API 캐시 {api_deleted}개, 로그 {log_deleted}개 삭제")
    
    def get_cache_stats(self) -> Dict:
        """캐시 통계 정보 조회"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # API 캐시 통계
            cursor.execute("SELECT COUNT(*) FROM api_cache")
            api_cache_count = cursor.fetchone()[0]
            
            # 사업장 캐시 통계
            cursor.execute("SELECT COUNT(*) FROM workplace_cache")
            workplace_cache_count = cursor.fetchone()[0]
            
            # 동기화 대기 작업 수
            cursor.execute("SELECT COUNT(*) FROM sync_log WHERE sync_status = 'pending'")
            pending_sync_count = cursor.fetchone()[0]
            
            # 가장 많이 접근된 사업장
            cursor.execute("""
                SELECT wkpl_nm, access_count FROM workplace_cache 
                ORDER BY access_count DESC LIMIT 5
            """)
            top_accessed = cursor.fetchall()
            
            conn.close()
            
            return {
                'api_cache_count': api_cache_count,
                'workplace_cache_count': workplace_cache_count,
                'pending_sync_count': pending_sync_count,
                'top_accessed_workplaces': top_accessed
            }
    
    def close(self):
        """데이터베이스 연결 정리"""
        # SQLite는 파일 기반이므로 별도 연결 종료 불필요
        print("🔌 SQLite 캐시 연결 정리 완료")

# 사용 예시
if __name__ == "__main__":
    # 캐시 시스템 테스트
    cache = NPSCache()
    
    # 통계 정보 출력
    stats = cache.get_cache_stats()
    print("📊 캐시 통계:")
    print(f"  - API 캐시: {stats['api_cache_count']}개")
    print(f"  - 사업장 캐시: {stats['workplace_cache_count']}개")
    print(f"  - 동기화 대기: {stats['pending_sync_count']}개")
    
    cache.close()
