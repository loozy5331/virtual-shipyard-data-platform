# virtual-shipyard-data-platform

[가상 조선소 시뮬레이터](https://github.com/loozy5331/virtual-shipyard-simulator)가 내는 노이즈 섞인 이벤트를 받아 마트까지 만드는 데이터 플랫폼이다.
파일 착륙 구역으로 받는 작은 배치에서 시작해, 겪은 문제가 다음 구성 요소(대사, Kafka, 스트림)를 부를 때마다 Phase를 하나씩 더한다.
결과는 시뮬레이터의 정답 로그와 맞춘 점수판으로 채점한다.

- 계획: [docs/development-plan.md](docs/development-plan.md). Phase 지도와 최종 형태가 있다
- 원천 쪽 계획: [시뮬레이터 단계별 개발 계획](https://github.com/loozy5331/virtual-shipyard-simulator/blob/develop/docs/development-plan.md). 플랫폼이 원천에 기대는 일은 플랫폼 계획의 "이웃 레포" 표에 있다

## 구조

```
docs/           계획(development-plan.md), adr/, specs/(설계), plans/(작업 계획)
.github/        CI(lint), develop 자동 머지
```

레포에는 지금 할 일이 반드시 쓰는 파일만 둔다. 폴더는 처음 쓰는 할 일과 함께 생긴다: `pipelines/ingest/`·`data/`(Phase 1 `feat/bronze`), `dbt/`·`contracts/`(`feat/silver`), `pipelines/scoreboard/`(`feat/scoreboard`), `docker-compose.yml`·`airflow/`(`feat/dag`). 할 일과 들이는 것은 [계획](docs/development-plan.md)의 "Phase별 할 일"에 있다.

## 실행

아직 실행할 것이 없다. Phase 1의 첫 할 일(`feat/bronze`)부터 착륙 구역으로 방출해 받는다.

```bash
# 시뮬레이터 레포에서: 착륙 구역으로 방출(feat/bronze부터)
uv run python -m emitter --out-dir ../virtual-shipyard-data-platform/data/landing
```

## 만든 것

- 0단계 레포와 로컬 인프라(10/27): compose 하나로 Redpanda·Console·Airflow 3, 토픽 여섯(자동 생성 끔), 원천 `v0.3.0` 계약 사본과 CI의 원천 대조, [ADR-001](docs/adr/001-stack.md)·[ADR-002](docs/adr/002-source-contract-and-topics.md). 빈 볼륨에서 띄워 방출기의 60,588건이 모두 들어가고 `smoke` DAG가 성공, 메모리는 합쳐 약 1.2GB. 회고: 계약 테스트가 사본 안의 일관성만 봐서 손으로 고친 사본도 통과했다. 리뷰에서 "원천과 같은가"를 묻다 알았고, CI가 고정 커밋과 diff하게 고쳤다
- Phase 0 정리(11/10): MVP 없이 Kafka부터 깐 것을 파일 입구로 되돌리고 계획을 Phase 0~6으로 다시 짰다([설계](docs/specs/2026-11-10-phased-replan-design.md), [ADR-003](docs/adr/003-file-landing-zone.md)). 이어서 "반드시 필요한 파일만, 처음 쓰는 할 일과 함께"로 정하고 compose·Airflow·계약 사본·테스트까지 비웠다. 회고: 구조도를 처음 형태로 다시 보니 과했다. "왜 Kafka인가"에 겪은 문제로 답할 수 없다는 데서 알았다

## 개발

```bash
uv sync              # Python 3.12, 개발 도구(ruff)
uv run ruff check .
```

PR은 `develop`으로 보내고, CI(`test` job, 지금은 lint만)가 통과하면 자동으로 머지된다. pytest는 `feat/bronze`에서 더한다.
