import requests
import json
import urllib.parse
from nps_sync import NPSSync

def get_service_key():
    """Reads the service key from key.txt."""
    try:
        with open('key.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("Error: 'key.txt' file not found. Please create it and paste your service key inside.")
        return None

def fetch_api_data(url, params):
    """Generic function to make API requests."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def get_base_info(service_key, wkpl_nm, bzowr_rgst_no=None):
    """
    Fetches basic workplace information from the first API.
    Sorts by wkplNm (ascending) and dataCrtYm (descending).
    """
    base_url = 'https://apis.data.go.kr/B552015/NpsBplcInfoInqireServiceV2/getBassInfoSearchV2'
    params = {
        'serviceKey': urllib.parse.unquote(service_key),
        'wkplNm': wkpl_nm,
        'numOfRows': 30,
        'dataType': 'JSON',
        'wkplNmOrdrBy': 'ASC',  # Sort by workplace name ascending
        'dataCrtYmOrdrBy': 'DESC'  # Sort by data creation month descending
    }
    if bzowr_rgst_no:
        params['bzowrRgstNo'] = bzowr_rgst_no
    
    data = fetch_api_data(base_url, params)
    
    if data and data['response']['header']['resultCode'] == '00':
        items = data['response']['body']['items']
        if items and 'item' in items:
            return items['item']
    return []

def get_detail_info(service_key, seq):
    """Fetches detailed workplace information using the sequence number."""
    base_url = 'https://apis.data.go.kr/B552015/NpsBplcInfoInqireServiceV2/getDetailInfoSearchV2'
    params = {
        'serviceKey': urllib.parse.unquote(service_key),
        'seq': seq,
        'dataType': 'JSON'
    }
    data = fetch_api_data(base_url, params)
    
    if data and data['response']['header']['resultCode'] == '00':
        items = data['response']['body']['items']
        if items and 'item' in items:
            return items['item'][0]
    return {}

def get_monthly_status(service_key, seq):
    """Fetches monthly status (new hires and resignations) using the sequence number."""
    base_url = 'https://apis.data.go.kr/B552015/NpsBplcInfoInqireServiceV2/getPdAcctoSttusInfoSearchV2'
    params = {
        'serviceKey': urllib.parse.unquote(service_key),
        'seq': seq,
        'dataType': 'JSON'
    }
    data = fetch_api_data(base_url, params)
    
    if data and data['response']['header']['resultCode'] == '00':
        items = data['response']['body']['items']
        if items and 'item' in items:
            return items['item'][0]
    return {}

def main():
    service_key = get_service_key()
    if not service_key:
        return

    # 동기화 시스템 초기화
    sync = NPSSync()

    wkpl_nm = input("사업장명(wkplNm)을 입력하세요: ")
    bzowr_rgst_no = input("사업자등록번호(bzowrRgstNo)를 입력하세요 (선택 사항): ") or None

    print("\n1. 사업장 정보 조회 중... (캐시 우선)")
    
    # 캐시에서 먼저 조회
    cached_results = sync.get_workplace_data(wkpl_nm, bzowr_rgst_no)
    
    if cached_results:
        print(f"📦 캐시에서 {len(cached_results)}개 사업장 정보 조회 완료")
        final_results = cached_results
    else:
        print("🌐 캐시 미스 - API 호출 시작")
        
        # API 호출
        base_info_list = get_base_info(service_key, wkpl_nm, bzowr_rgst_no)
        
        if not base_info_list:
            print("조회된 사업장 정보가 없습니다. 입력 정보를 다시 확인해주세요.")
            sync.close()
            return

        final_results = []
        print("\n2. 상세 정보 및 월별 가입자 현황 조회 중...")
        
        for info in base_info_list:
            seq = info.get('seq')
            if not seq:
                continue
            
            # API 호출을 캐시와 함께 처리
            def get_detail_info_cached(params):
                return get_detail_info(service_key, seq)
            
            def get_monthly_status_cached(params):
                return get_monthly_status(service_key, seq)
            
            # 2단계: 상세 정보 조회 (캐시 우선)
            detail_info = sync.get_api_data_with_cache(
                'detail_info', 
                {'seq': seq}, 
                get_detail_info_cached
            )
            
            jnngp_cnt = detail_info.get('jnngpCnt') if detail_info else None
            crrmm_ntc_amt = detail_info.get('crrmmNtcAmt') if detail_info else None

            # 월평균 급여 계산
            avg_monthly_salary = None
            if jnngp_cnt and jnngp_cnt > 0 and crrmm_ntc_amt:
                try:
                    # 당월 고지금액 / 0.09 / 가입자 수
                    avg_monthly_salary = int(crrmm_ntc_amt) / 0.09 / int(jnngp_cnt) + 200000 # +20만원 보정(교통비, 식대)
                except (ValueError, ZeroDivisionError):
                    avg_monthly_salary = None            
            
            # 3단계: 월별 현황 조회 (캐시 우선)
            status_info = sync.get_api_data_with_cache(
                'monthly_status', 
                {'seq': seq}, 
                get_monthly_status_cached
            )
            
            nw_acqzr_cnt = status_info.get('nwAcqzrCnt') if status_info else None
            lss_jnngp_cnt = status_info.get('lssJnngpCnt') if status_info else None
            
            # 결과 조합
            combined_data = {
                'wkplNm': info.get('wkplNm'),
                'bzowrRgstNo': info.get('bzowrRgstNo'),
                'dataCrtYm': info.get('dataCrtYm'),
                'seq': info.get('seq'),
                'wkplRoadNmDtlAddr': info.get('wkplRoadNmDtlAddr'),
                'jnngpCnt': jnngp_cnt,
                'crrmmNtcAmt': crrmm_ntc_amt,
                'avgMonthlySalary': avg_monthly_salary,
                'nwAcqzrCnt': nw_acqzr_cnt,
                'lssJnngpCnt': lss_jnngp_cnt
            }
            final_results.append(combined_data)
            
            # 데이터베이스에 저장 (캐시 + Supabase)
            sync.save_workplace_data(combined_data, 'insert')

    print("\n--- 최종 결과 ---")
    if final_results:
        for result in final_results:
            print(f"사업장명: {result.get('wkplNm')}")
            print(f"자료생성년월: {result.get('dataCrtYm')}")
            print(f"식별번호(seq): {result.get('seq')}")
            print(f"주소: {result.get('wkplRoadNmDtlAddr')}")
            print(f"가입자수: {result.get('jnngpCnt')}")
            print(f"당월고지금액: {result.get('crrmmNtcAmt')}")
            print(f"**월평균 급여(추정): {result.get('avgMonthlySalary'):,.0f}원**" if result.get('avgMonthlySalary') else "월평균 급여: 정보 없음")
            print(f"월별 취업자수: {result.get('nwAcqzrCnt')}")
            print(f"월별 퇴직자수: {result.get('lssJnngpCnt')}")
            print("-" * 20)
    else:
        print("조회된 데이터가 없습니다.")
    
    # 캐시 통계 출력
    stats = sync.get_cache_stats()
    print(f"\n📊 캐시 통계:")
    print(f"  - API 캐시: {stats['api_cache_count']}개")
    print(f"  - 사업장 캐시: {stats['workplace_cache_count']}개")
    print(f"  - 동기화 대기: {stats['pending_sync_count']}개")
    print(f"  - Supabase 연결: {'✅' if stats['supabase_connected'] else '❌'}")
    
    # 대기 중인 동기화 작업 처리
    if stats['pending_sync_count'] > 0:
        print(f"\n🔄 {stats['pending_sync_count']}개 동기화 작업 처리 중...")
        sync.sync_pending_operations()
    
    sync.close()

if __name__ == "__main__":
    main()