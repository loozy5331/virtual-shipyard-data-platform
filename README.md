# virtual-shipyard-data-platform

[가상 조선소 시뮬레이터](https://github.com/loozy5331/virtual-shipyard-simulator)가 내는 노이즈 섞인 이벤트를 받아 마트까지 만드는 데이터 플랫폼이다.
파일 착륙 구역으로 받는 작은 배치에서 시작해, 겪은 문제가 다음 구성 요소(대사, Kafka, 스트림)를 부를 때마다 Phase를 하나씩 더한다.
결과는 시뮬레이터의 정답 로그와 맞춘 점수판으로 채점한다.

- 계획: [docs/development-plan.md](docs/development-plan.md). 최종 형태, Phase 지도, 지금 Phase의 할 일
- 원천 쪽 계획: [시뮬레이터 단계별 개발 계획](https://github.com/loozy5331/virtual-shipyard-simulator/blob/develop/docs/development-plan.md). 플랫폼이 원천에 기대는 일은 플랫폼 계획의 "이웃 레포" 표에 있다

## 구조

```
docs/           계획(development-plan.md), adr/
pipelines/      ingest/(착륙 구역 → bronze)
tests/          pytest
.github/        CI(ruff, pytest), develop 자동 머지
.pre-commit-config.yaml  커밋 전 ruff
```

레포에는 지금 할 일이 반드시 쓰는 파일만 둔다. 폴더는 처음 쓰는 할 일과 함께 생긴다: `dbt/`·`contracts/`(`feat/silver`), `pipelines/scoreboard/`(`feat/scoreboard`), `docker-compose.yml`·`airflow/`(`feat/dag`). 할 일과 들이는 것은 [계획](docs/development-plan.md)의 "Phase별 할 일"에 있다.

## 실행

착륙 구역(`data/landing/`, git에 넣지 않음)으로 방출해 받는다. bronze 변환 `pipelines.ingest.bronze.ingest_dt`는 아직 명령줄이 없어 테스트로 돌린다.

```bash
# 시뮬레이터 레포에서: 정답 로그를 만들고 착륙 구역으로 방출
uv run python -m sim --seed 0
uv run python -m emitter --out-dir ../virtual-shipyard-data-platform/data/landing
```

## 만든 것

- 0단계 레포와 로컬 인프라(10/27): compose(Redpanda·Airflow 3), 계약 사본과 CI 대조, [ADR-001](docs/adr/001-stack.md). 회고: 계약 테스트가 사본 안의 일관성만 봐서 손으로 고친 사본도 통과했다. 리뷰에서 "원천과 같은가"를 묻다 알았고, CI가 고정 커밋과 diff하게 고쳤다
- Phase 0 정리(11/10): MVP 없이 Kafka부터 깐 것을 파일 입구로 되돌리고 계획을 Phase 0~6으로 다시 짰다([ADR-002](docs/adr/002-file-landing-zone.md)). 이어서 "반드시 필요한 파일만, 처음 쓰는 할 일과 함께"로 정하고 compose·Airflow·계약 사본·테스트까지 비웠다. 회고: 구조도를 처음 형태로 다시 보니 과했다. "왜 Kafka인가"에 겪은 문제로 답할 수 없다는 데서 알았다

## 개발

```bash
uv sync                    # Python 3.12, 개발 도구(ruff, pytest, pre-commit)
uv run pre-commit install  # 처음 한 번: 커밋 전에 ruff check --fix·ruff format
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

PR은 `develop`으로 보내고, CI(`test` job: ruff check·format, pytest)가 통과하면 자동으로 머지된다.
