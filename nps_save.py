import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta
import hashlib

# 환경 변수 로드
load_dotenv('config.env')

class NPSDatabase:
    def __init__(self):
        """Supabase PostgreSQL 데이터베이스 연결 초기화"""
        self.connection = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('SUPABASE_DB_HOST'),
                database=os.getenv('SUPABASE_DB_NAME'),
                user=os.getenv('SUPABASE_DB_USER'),
                password=os.getenv('SUPABASE_DB_PASSWORD'),
                port=os.getenv('SUPABASE_DB_PORT', 5432)
            )
            print("✅ Supabase 데이터베이스 연결 성공")
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            raise
    
    def create_tables(self):
        """필요한 테이블들 생성"""
        try:
            cursor = self.connection.cursor()
            
            # 사업장 기본 정보 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workplace_base_info (
                    id SERIAL PRIMARY KEY,
                    wkpl_nm VARCHAR(255) NOT NULL,
                    bzowr_rgst_no VARCHAR(20),
                    seq VARCHAR(50) UNIQUE NOT NULL,
                    data_crt_ym VARCHAR(6),
                    wkpl_road_nm_dtl_addr TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 사업장 상세 정보 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workplace_detail_info (
                    id SERIAL PRIMARY KEY,
                    seq VARCHAR(50) REFERENCES workplace_base_info(seq),
                    jnngp_cnt INTEGER,
                    crrmm_ntc_amt BIGINT,
                    avg_monthly_salary DECIMAL(15,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 월별 현황 정보 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workplace_monthly_status (
                    id SERIAL PRIMARY KEY,
                    seq VARCHAR(50) REFERENCES workplace_base_info(seq),
                    nw_acqzr_cnt INTEGER,
                    lss_jnngp_cnt INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # API 요청 캐시 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    id SERIAL PRIMARY KEY,
                    request_hash VARCHAR(64) UNIQUE NOT NULL,
                    api_type VARCHAR(50) NOT NULL,
                    response_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """)
            
            self.connection.commit()
            print("✅ 데이터베이스 테이블 생성 완료")
            
        except Exception as e:
            print(f"❌ 테이블 생성 실패: {e}")
            self.connection.rollback()
            raise
    
    def generate_request_hash(self, api_type, params):
        """API 요청 파라미터로부터 해시 생성"""
        # 파라미터를 정렬하여 일관된 해시 생성
        sorted_params = sorted(params.items())
        param_string = f"{api_type}:{json.dumps(sorted_params, sort_keys=True)}"
        return hashlib.sha256(param_string.encode()).hexdigest()
    
    def get_cached_data(self, api_type, params):
        """캐시된 데이터 조회"""
        try:
            request_hash = self.generate_request_hash(api_type, params)
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT response_data FROM api_cache 
                WHERE request_hash = %s 
                AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            """, (request_hash,))
            
            result = cursor.fetchone()
            if result:
                print(f"📦 캐시에서 데이터 조회: {api_type}")
                return result['response_data']
            
            return None
            
        except Exception as e:
            print(f"❌ 캐시 조회 실패: {e}")
            return None
    
    def save_cached_data(self, api_type, params, response_data, expires_hours=24):
        """API 응답 데이터를 캐시에 저장"""
        try:
            request_hash = self.generate_request_hash(api_type, params)
            cursor = self.connection.cursor()
            
            # 만료 시간 계산
            expires_at = datetime.now() + timedelta(hours=expires_hours)
            
            cursor.execute("""
                INSERT INTO api_cache (request_hash, api_type, response_data, expires_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (request_hash) 
                DO UPDATE SET 
                    response_data = EXCLUDED.response_data,
                    expires_at = EXCLUDED.expires_at,
                    created_at = CURRENT_TIMESTAMP
            """, (request_hash, api_type, json.dumps(response_data), expires_at))
            
            self.connection.commit()
            print(f"💾 캐시에 데이터 저장: {api_type}")
            
        except Exception as e:
            print(f"❌ 캐시 저장 실패: {e}")
            self.connection.rollback()
    
    def save_workplace_data(self, base_info, detail_info=None, monthly_status=None):
        """사업장 데이터를 데이터베이스에 저장"""
        try:
            cursor = self.connection.cursor()
            
            # 기본 정보 저장
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
                base_info.get('wkplNm'),
                base_info.get('bzowrRgstNo'),
                base_info.get('seq'),
                base_info.get('dataCrtYm'),
                base_info.get('wkplRoadNmDtlAddr')
            ))
            
            # 상세 정보 저장
            if detail_info:
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
                    base_info.get('seq'),
                    detail_info.get('jnngpCnt'),
                    detail_info.get('crrmmNtcAmt'),
                    detail_info.get('avgMonthlySalary')
                ))
            
            # 월별 현황 저장
            if monthly_status:
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
                    base_info.get('seq'),
                    monthly_status.get('nwAcqzrCnt'),
                    monthly_status.get('lssJnngpCnt')
                ))
            
            self.connection.commit()
            print(f"💾 사업장 데이터 저장 완료: {base_info.get('wkplNm')}")
            
        except Exception as e:
            print(f"❌ 사업장 데이터 저장 실패: {e}")
            self.connection.rollback()
    
    def get_workplace_data(self, wkpl_nm, bzowr_rgst_no=None):
        """저장된 사업장 데이터 조회"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
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
                print(f"📦 데이터베이스에서 {len(results)}개 사업장 정보 조회")
                return [dict(row) for row in results]
            
            return []
            
        except Exception as e:
            print(f"❌ 사업장 데이터 조회 실패: {e}")
            return []
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("🔌 데이터베이스 연결 종료")

# 사용 예시
if __name__ == "__main__":
    # 데이터베이스 연결 테스트
    db = NPSDatabase()
    db.close()