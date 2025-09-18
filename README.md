# nps-workers

## 설치 및 실행 가이드

### 1. 가상환경 생성

프로젝트를 처음 실행하기 전에 Python 가상환경을 생성해야 합니다.

```bash
# 가상환경 생성 (.venv 폴더에 생성됨)
python -m venv .venv
```

### 2. 가상환경 활성화

#### Windows (WSL/Linux 환경)
```bash
source .venv/bin/activate
```

#### Windows (PowerShell/CMD)
```bash
.venv\Scripts\activate
```

### 3. 의존성 설치

가상환경이 활성화된 상태에서 필요한 패키지들을 설치합니다.

```bash
pip install -r requirements.txt
```

### 4. 프로그램 실행

의존성 설치가 완료되면 프로그램을 실행할 수 있습니다.

```bash
python nps_workers.py
```

### 5. 가상환경 비활성화

작업이 끝나면 가상환경을 비활성화할 수 있습니다.

```bash
deactivate
```

## 주의사항

- 가상환경을 활성화한 후에 `pip install` 명령어를 실행해야 합니다.
- 프로젝트를 실행할 때마다 가상환경을 활성화해야 합니다.
- `requirements.txt` 파일에 명시된 모든 패키지가 설치되어야 정상적으로 실행됩니다.
