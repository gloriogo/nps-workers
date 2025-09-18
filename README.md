# NPS Workers - 국민연금 사업장 정보 조회 시스템

국민연금공단 API를 활용한 사업장 정보 조회 시스템으로, SQLite 로컬 캐시와 Supabase 클라우드 데이터베이스를 연동하여 API 호출 횟수를 최소화하고 데이터를 안전하게 저장합니다.

## 🚀 주요 기능

- **로컬 캐시 시스템**: SQLite를 사용한 빠른 로컬 데이터 조회
- **클라우드 동기화**: Supabase PostgreSQL과 실시간 동기화
- **API 호출 최적화**: 캐시된 데이터 우선 조회로 API 호출 횟수 최소화
- **자동 동기화**: 로컬 변경사항을 자동으로 클라우드에 동기화
- **캐시 관리**: 만료된 캐시 자동 정리 및 통계 정보 제공

## 📁 파일 구조

```
nps/
├── nps_workers.py      # 메인 실행 파일
├── nps_cache.py        # SQLite 로컬 캐시 시스템
├── nps_sync.py         # SQLite-Supabase 동기화 관리
├── nps_save.py         # Supabase 데이터베이스 연결
├── cache_manager.py    # 캐시 관리 유틸리티
├── config.env          # 환경 변수 설정
├── key.txt            # API 키 파일
└── requirements.txt   # Python 패키지 의존성
```

## 🛠️ 설치 및 설정

### 1. 가상환경 생성 및 활성화

```bash
# 가상환경 생성
python -m venv .venv

# 가상환경 활성화 (WSL/Linux)
source .venv/bin/activate

# 가상환경 활성화 (Windows)
.venv\Scripts\activate
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

### 3. Supabase 설정

1. [Supabase](https://supabase.com)에서 계정 생성 및 프로젝트 생성
2. `config.env` 파일에 연결 정보 입력:

```env
# Supabase Database Configuration
SUPABASE_DB_HOST=your_host_here
SUPABASE_DB_NAME=your_database_name_here
SUPABASE_DB_USER=your_user_here
SUPABASE_DB_PASSWORD=your_password_here
SUPABASE_DB_PORT=5432

# API Configuration
NPS_API_KEY=your_nps_api_key_here
```

### 4. API 키 설정

`key.txt` 파일에 국민연금공단 API 키를 입력합니다.

## 🎯 사용법

### 기본 실행

```bash
python nps_workers.py
```

프로그램 실행 시:
1. **캐시 우선 조회**: 로컬 SQLite에서 먼저 데이터 조회
2. **API 호출**: 캐시에 없는 데이터만 API 호출
3. **자동 저장**: 조회한 데이터를 로컬 캐시와 Supabase에 저장
4. **동기화**: 로컬 변경사항을 Supabase에 자동 동기화

### 캐시 관리

```bash
# 캐시 통계 조회
python cache_manager.py stats

# 캐시 정리 (만료된 데이터 삭제)
python cache_manager.py cleanup

# 대기 중인 동기화 작업 처리
python cache_manager.py sync

# 특정 사업장 검색
python cache_manager.py search --name "사업장명"

# 모든 캐시 삭제
python cache_manager.py clear
```

## 📊 캐시 시스템 동작 방식

### 1. 데이터 조회 흐름
```
사용자 요청 → SQLite 캐시 확인 → 캐시 있음: 즉시 반환
                                → 캐시 없음: API 호출 → 캐시 저장 → 반환
```

### 2. 데이터 저장 흐름
```
새 데이터 → SQLite에 즉시 저장 → Supabase 동기화 (백그라운드)
```

### 3. 동기화 관리
- 로컬 변경사항은 `sync_log` 테이블에 기록
- 백그라운드에서 Supabase로 자동 동기화
- 동기화 실패 시 재시도 가능

## 🗄️ 데이터베이스 스키마

### SQLite 로컬 캐시
- `api_cache`: API 응답 캐시
- `workplace_cache`: 사업장 정보 캐시
- `sync_log`: 동기화 로그

### Supabase PostgreSQL
- `workplace_base_info`: 사업장 기본 정보
- `workplace_detail_info`: 사업장 상세 정보
- `workplace_monthly_status`: 월별 현황 정보
- `api_cache`: API 응답 캐시

## ⚡ 성능 최적화

- **로컬 캐시**: SQLite로 빠른 로컬 조회
- **API 호출 최소화**: 캐시된 데이터 우선 사용
- **백그라운드 동기화**: 사용자 경험에 영향 없이 동기화
- **자동 정리**: 만료된 캐시 자동 삭제

## 🔧 문제 해결

### Supabase 연결 실패
- `config.env` 파일의 연결 정보 확인
- Supabase 프로젝트 상태 확인
- 네트워크 연결 상태 확인

### 캐시 문제
```bash
# 캐시 통계 확인
python cache_manager.py stats

# 캐시 정리
python cache_manager.py cleanup

# 모든 캐시 삭제 후 재시작
python cache_manager.py clear
```

## 📝 주의사항

- 가상환경을 활성화한 후에 프로그램을 실행해야 합니다
- `config.env` 파일에 올바른 Supabase 연결 정보가 필요합니다
- `key.txt` 파일에 유효한 API 키가 필요합니다
- 첫 실행 시 Supabase에 테이블이 자동 생성됩니다

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
