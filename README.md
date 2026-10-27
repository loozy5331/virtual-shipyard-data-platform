# virtual-shipyard-data-platform

[가상 조선소 시뮬레이터](https://github.com/loozy5331/virtual-shipyard-simulator)가 내는 노이즈 섞인 이벤트를 받아 마트까지 만드는 데이터 플랫폼이다.
docker compose 위에서 Kafka(Redpanda) 토픽으로 받고, 같은 토픽을 배치(정확한 기록)와 스트림(지금 상태)이 서로 다른 소비자 그룹으로 나눠 받는다.
배치는 DuckDB·dbt와 Airflow 3로 중복·지연 도착·누락을 대사와 재발행으로 복원하고, 그 결과를 시뮬레이터의 정답 로그와 맞춘 점수판으로 채점한다.

- 계획: [docs/가상 조선소 데이터 플랫폼 단계별 개발 계획.md](docs/가상%20조선소%20데이터%20플랫폼%20단계별%20개발%20계획.md) ([편집용 원본](https://claude.ai/artifact/DvWWpHW7oCEF26bfxkKobW))
- 원천 쪽 계획: [시뮬레이터 단계별 개발 계획](https://github.com/loozy5331/virtual-shipyard-simulator/blob/main/docs/가상%20조선소%20시뮬레이터%20단계별%20개발%20계획.md). 플랫폼이 원천에 기대는 일은 플랫폼 계획의 "이웃 레포" 표에 있다

## 구조

```
pipelines/      Python 코드: ingest(수집) · reconcile(대사) · stream(스트림 뷰) · scoreboard(점수판)
airflow/dags/   Airflow DAG
dbt/            dbt-duckdb 프로젝트(silver·gold)
contracts/      원천 계약 스키마(배치·스트림이 같이 씀)
infra/          compose가 쓰는 스크립트(토픽 만들기)
runtime/        컨테이너 마운트(data/, requests/). git에 넣지 않는다
docs/           계획, adr/
tests/
```

## 실행

```bash
docker compose up -d   # Redpanda(localhost:9092), 토픽 6개, Console(http://localhost:8081), Airflow(http://localhost:8080)
# 시뮬레이터 레포에서: uv run --extra kafka python -m emitter --sink kafka
```

토픽은 `infra/create_topics.sh`만 만든다(자동 생성 꺼짐): `erp|mes|eam` × `.events`(파티션 3)·`.manifest`(파티션 1), 보존 기간 무기한.
Airflow 3는 LocalExecutor + Postgres이고 로그인 없이 쓴다. `runtime/data/`, `runtime/requests/`는 컨테이너의 `/data`, `/requests`로 마운트된다. 처음 뜰 때 `smoke` DAG로 실행을 확인할 수 있다.

## 만든 것

- 0단계 레포와 로컬 인프라(10/27): compose 하나로 Redpanda·Console·Airflow 3, 토픽 여섯(자동 생성 끔), 원천 `v0.3.0` 계약 사본과 CI의 원천 대조, [ADR-001](docs/adr/001-stack.md)·[ADR-002](docs/adr/002-source-contract-and-topics.md). 빈 볼륨에서 띄워 방출기의 60,588건이 모두 들어가고 `smoke` DAG가 성공, 메모리는 합쳐 약 1.2GB. 회고: 계약 테스트가 사본 안의 일관성만 봐서 손으로 고친 사본도 통과했다. 리뷰에서 "원천과 같은가"를 묻다 알았고, CI가 고정 커밋과 diff하게 고쳤다

## 개발

```bash
uv sync              # Python 3.12, 개발 도구(ruff·pytest)
uv run ruff check .
uv run pytest
```

PR은 `develop`으로 보내고, CI(`test`)가 통과하면 자동으로 머지된다.
