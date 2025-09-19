#!/usr/bin/env python3
"""
NPS Workers 데이터베이스 CRUD 테스트
- Supabase PostgreSQL 데이터베이스 테스트
- SQLite 캐시 시스템 테스트
- 동기화 기능 테스트
"""

import pytest
import os
import tempfile
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# 테스트 대상 모듈 import
from nps_save import NPSDatabase
from nps_cache import NPSCache
from nps_sync import NPSSync


class TestNPSCache:
    """SQLite 캐시 시스템 테스트"""
    
    @pytest.fixture
    def temp_cache(self):
        """임시 캐시 데이터베이스 생성"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            cache = NPSCache(tmp.name)
            yield cache
            cache.close()
            os.unlink(tmp.name)
    
    def test_cache_initialization(self, temp_cache):
        """캐시 초기화 테스트"""
        assert temp_cache is not None
        stats = temp_cache.get_cache_stats()
        assert stats['api_cache_count'] == 0
        assert stats['workplace_cache_count'] == 0
    
    def test_api_cache_create(self, temp_cache):
        """API 캐시 생성 테스트"""
        api_type = "test_api"
        params = {"param1": "value1", "param2": "value2"}
        response_data = {"result": "success", "data": [1, 2, 3]}
        
        # 캐시 저장
        temp_cache.set_api_cache(api_type, params, response_data)
        
        # 캐시 조회
        cached_data = temp_cache.get_api_cache(api_type, params)
        
        assert cached_data is not None
        assert cached_data == response_data
        
        # 통계 확인
        stats = temp_cache.get_cache_stats()
        assert stats['api_cache_count'] == 1
    
    def test_api_cache_read(self, temp_cache):
        """API 캐시 조회 테스트"""
        api_type = "test_api"
        params = {"param1": "value1"}
        response_data = {"result": "success"}
        
        # 캐시 저장
        temp_cache.set_api_cache(api_type, params, response_data)
        
        # 캐시 조회
        cached_data = temp_cache.get_api_cache(api_type, params)
        assert cached_data == response_data
        
        # 존재하지 않는 캐시 조회
        non_existent = temp_cache.get_api_cache("non_existent", {"param": "value"})
        assert non_existent is None
    
    def test_workplace_cache_create(self, temp_cache):
        """사업장 캐시 생성 테스트"""
        workplace_data = {
            'seq': 'TEST001',
            'wkplNm': '테스트사업장',
            'bzowrRgstNo': '1234567890',
            'dataCrtYm': '202401',
            'wkplRoadNmDtlAddr': '서울시 강남구 테스트로 123',
            'jnngpCnt': 10,
            'crrmmNtcAmt': 1000000,
            'avgMonthlySalary': 500000.0,
            'nwAcqzrCnt': 2,
            'lssJnngpCnt': 1
        }
        
        # 캐시 저장
        temp_cache.set_workplace_cache(workplace_data, 'insert')
        
        # 캐시 조회
        cached_data = temp_cache.get_workplace_cache('테스트사업장')
        
        assert len(cached_data) == 1
        assert cached_data[0]['seq'] == 'TEST001'
        assert cached_data[0]['wkpl_nm'] == '테스트사업장'
        
        # 통계 확인
        stats = temp_cache.get_cache_stats()
        assert stats['workplace_cache_count'] == 1
    
    def test_workplace_cache_update(self, temp_cache):
        """사업장 캐시 업데이트 테스트"""
        workplace_data = {
            'seq': 'TEST001',
            'wkplNm': '테스트사업장',
            'jnngpCnt': 10
        }
        
        # 초기 데이터 저장
        temp_cache.set_workplace_cache(workplace_data, 'insert')
        
        # 업데이트 데이터
        updated_data = {
            'seq': 'TEST001',
            'wkplNm': '테스트사업장',
            'jnngpCnt': 15  # 가입자 수 변경
        }
        
        # 업데이트
        temp_cache.set_workplace_cache(updated_data, 'update')
        
        # 조회하여 업데이트 확인
        cached_data = temp_cache.get_workplace_cache('테스트사업장')
        assert len(cached_data) == 1
        assert cached_data[0]['jnngp_cnt'] == 15
    
    def test_workplace_cache_delete(self, temp_cache):
        """사업장 캐시 삭제 테스트"""
        workplace_data = {
            'seq': 'TEST001',
            'wkplNm': '테스트사업장'
        }
        
        # 데이터 저장
        temp_cache.set_workplace_cache(workplace_data, 'insert')
        
        # 삭제
        temp_cache.set_workplace_cache(workplace_data, 'delete')
        
        # 조회하여 삭제 확인
        cached_data = temp_cache.get_workplace_cache('테스트사업장')
        assert len(cached_data) == 0
        
        # 통계 확인
        stats = temp_cache.get_cache_stats()
        assert stats['workplace_cache_count'] == 0
    
    def test_cache_expiration(self, temp_cache):
        """캐시 만료 테스트"""
        api_type = "test_api"
        params = {"param": "value"}
        response_data = {"result": "success"}
        
        # 1초 후 만료되는 캐시 저장
        temp_cache.set_api_cache(api_type, params, response_data, expires_hours=1/3600)  # 1초
        
        # 즉시 조회 (성공해야 함)
        cached_data = temp_cache.get_api_cache(api_type, params)
        assert cached_data == response_data
        
        # 1초 대기 후 조회 (실패해야 함)
        import time
        time.sleep(1.1)
        expired_data = temp_cache.get_api_cache(api_type, params)
        assert expired_data is None
    
    def test_cache_cleanup(self, temp_cache):
        """캐시 정리 테스트"""
        # 만료된 캐시 생성
        api_type = "expired_api"
        params = {"param": "value"}
        response_data = {"result": "success"}
        
        # 과거 시간으로 만료 설정
        with patch('nps_cache.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.now() - timedelta(hours=25)
            temp_cache.set_api_cache(api_type, params, response_data, expires_hours=24)
        
        # 정리 전 통계
        stats_before = temp_cache.get_cache_stats()
        assert stats_before['api_cache_count'] == 1
        
        # 캐시 정리
        temp_cache.cleanup_expired_cache()
        
        # 정리 후 통계
        stats_after = temp_cache.get_cache_stats()
        assert stats_after['api_cache_count'] == 0


class TestNPSDatabase:
    """Supabase 데이터베이스 테스트 (Mock 사용)"""
    
    @pytest.fixture
    def mock_db(self):
        """Mock 데이터베이스 연결"""
        with patch('nps_save.psycopg2.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Mock 데이터베이스 생성
            db = NPSDatabase()
            db.connection = mock_conn
            yield db
    
    def test_database_connection(self, mock_db):
        """데이터베이스 연결 테스트"""
        assert mock_db.connection is not None
    
    def test_table_creation(self, mock_db):
        """테이블 생성 테스트"""
        # create_tables 메서드 호출
        mock_db.create_tables()
        
        # cursor.execute가 호출되었는지 확인
        assert mock_db.connection.cursor.called
        assert mock_db.connection.commit.called
    
    def test_workplace_data_save(self, mock_db):
        """사업장 데이터 저장 테스트"""
        base_info = {
            'wkplNm': '테스트사업장',
            'bzowrRgstNo': '1234567890',
            'seq': 'TEST001',
            'dataCrtYm': '202401',
            'wkplRoadNmDtlAddr': '서울시 강남구'
        }
        
        detail_info = {
            'jnngpCnt': 10,
            'crrmmNtcAmt': 1000000,
            'avgMonthlySalary': 500000.0
        }
        
        monthly_status = {
            'nwAcqzrCnt': 2,
            'lssJnngpCnt': 1
        }
        
        # 데이터 저장
        mock_db.save_workplace_data(base_info, detail_info, monthly_status)
        
        # cursor.execute가 호출되었는지 확인
        assert mock_db.connection.cursor.called
        assert mock_db.connection.commit.called
    
    def test_workplace_data_read(self, mock_db):
        """사업장 데이터 조회 테스트"""
        # Mock 결과 설정
        mock_cursor = mock_db.connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {
                'wkpl_nm': '테스트사업장',
                'seq': 'TEST001',
                'jnngp_cnt': 10
            }
        ]
        
        # 데이터 조회
        results = mock_db.get_workplace_data('테스트사업장')
        
        assert len(results) == 1
        assert results[0]['wkpl_nm'] == '테스트사업장'
        assert results[0]['seq'] == 'TEST001'


class TestNPSSync:
    """동기화 시스템 테스트"""
    
    @pytest.fixture
    def mock_sync(self):
        """Mock 동기화 시스템"""
        with patch('nps_sync.NPSCache') as mock_cache, \
             patch('nps_sync.psycopg2.connect') as mock_connect:
            
            # Mock 캐시 설정
            mock_cache_instance = MagicMock()
            mock_cache.return_value = mock_cache_instance
            mock_cache_instance.get_workplace_cache.return_value = []
            mock_cache_instance.get_api_cache.return_value = None
            
            # Mock Supabase 연결
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            sync = NPSSync()
            sync.cache = mock_cache_instance
            sync.supabase_connection = mock_conn
            
            yield sync
    
    def test_cache_priority_read(self, mock_sync):
        """캐시 우선 조회 테스트"""
        # 캐시에 데이터가 있는 경우
        mock_sync.cache.get_workplace_cache.return_value = [
            {'wkpl_nm': '테스트사업장', 'seq': 'TEST001'}
        ]
        
        results = mock_sync.get_workplace_data('테스트사업장')
        
        assert len(results) == 1
        assert results[0]['wkpl_nm'] == '테스트사업장'
        
        # Supabase 조회가 호출되지 않았는지 확인
        assert not mock_sync.supabase_connection.cursor.called
    
    def test_supabase_fallback_read(self, mock_sync):
        """Supabase 백업 조회 테스트"""
        # 캐시에 데이터가 없는 경우
        mock_sync.cache.get_workplace_cache.return_value = []
        
        # Mock Supabase 결과
        mock_cursor = mock_sync.supabase_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {'wkpl_nm': '테스트사업장', 'seq': 'TEST001'}
        ]
        
        results = mock_sync.get_workplace_data('테스트사업장')
        
        assert len(results) == 1
        assert results[0]['wkpl_nm'] == '테스트사업장'
        
        # 캐시 저장이 호출되었는지 확인
        assert mock_sync.cache.set_workplace_cache.called
    
    def test_api_cache_integration(self, mock_sync):
        """API 캐시 통합 테스트"""
        # Mock API 함수
        def mock_api_function(params):
            return {"result": "success", "data": params}
        
        # 캐시에 데이터가 없는 경우
        mock_sync.cache.get_api_cache.return_value = None
        
        # API 호출 테스트
        result = mock_sync.get_api_data_with_cache(
            'test_api', 
            {'param': 'value'}, 
            mock_api_function
        )
        
        assert result == {"result": "success", "data": {'param': 'value'}}
        
        # 캐시 저장이 호출되었는지 확인
        assert mock_sync.cache.set_api_cache.called


class TestIntegration:
    """통합 테스트"""
    
    def test_full_workflow(self):
        """전체 워크플로우 테스트"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            try:
                # 캐시 시스템 초기화
                cache = NPSCache(tmp.name)
                
                # 테스트 데이터
                workplace_data = {
                    'seq': 'INTEGRATION_TEST',
                    'wkplNm': '통합테스트사업장',
                    'jnngpCnt': 20,
                    'crrmmNtcAmt': 2000000
                }
                
                # 1. 데이터 저장
                cache.set_workplace_cache(workplace_data, 'insert')
                
                # 2. 데이터 조회
                results = cache.get_workplace_cache('통합테스트사업장')
                assert len(results) == 1
                assert results[0]['seq'] == 'INTEGRATION_TEST'
                
                # 3. 데이터 업데이트
                updated_data = workplace_data.copy()
                updated_data['jnngpCnt'] = 25
                cache.set_workplace_cache(updated_data, 'update')
                
                # 4. 업데이트 확인
                updated_results = cache.get_workplace_cache('통합테스트사업장')
                assert updated_results[0]['jnngp_cnt'] == 25
                
                # 5. 데이터 삭제
                cache.set_workplace_cache(workplace_data, 'delete')
                
                # 6. 삭제 확인
                deleted_results = cache.get_workplace_cache('통합테스트사업장')
                assert len(deleted_results) == 0
                
                cache.close()
                
            finally:
                os.unlink(tmp.name)


if __name__ == "__main__":
    # 테스트 실행
    pytest.main([__file__, "-v", "--tb=short"])
