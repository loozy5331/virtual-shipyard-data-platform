# Phase로 나눠 키우기: 다시 짠 계획 설계

2026-11-10 · 지금 계획: [개발 계획](../development-plan.md)

## 의도

최종 형태(Kafka 입구 하나, 배치와 스트림, 대사, 마트, 루프 닫기)는 그대로 둔다. 다만 레포가 처음부터 그 형태를 담고 있지 않고, Phase마다 필요한 만큼만 자라게 한다.

- **까닭**: 지금 구조도는 첫 형태로는 과하다. MVP가 따로 없고, 0단계부터 Redpanda와 토픽 여섯을 깔았다. 그래서 "왜 Kafka를 들였나"에 "처음부터 그렇게 설계했다"밖에 답할 수 없다.
- **바꾸는 것**: 단순한 배치(파일 입구)로 시작한다. 점수판이나 운영에서 문제를 겪으면 그 문제가 다음 Phase의 구성 요소를 부른다. Kafka는 실시간 요구가 생길 때 들어온다.
- **목표**
  - 레포의 문서와 코드는 지금 있는 것만 설명한다. 최종 그림은 계획 문서의 "목표" 절에 짧게 둔다.
  - Phase마다 "겪은 문제 → 들인 것"이 커밋과 ADR로 남는다.
- **하지 않는 것**: git 이력을 다시 쓰지 않는다. 지금까지의 PR #1~14와 0단계의 Kafka 작업은 그대로 둔다.
- **실패로 볼 것**: 다음 Phase의 구성 요소를 "나중에 필요하니까" 미리 들이는 것. 들이는 까닭이 앞 Phase에서 겪은 문제로 설명되지 않으면 실패다.

## 접근

| 안 | 내용 | 판단 |
| --- | --- | --- |
| **A. 지금 레포에서 걷어내기** | Kafka 쪽을 compose·스크립트·문서에서 빼고, ADR-002를 "대체됨"으로 둔다. Phase 4에서 git 이력으로 되살린다 | **고름**. 이력이 실제 순서대로 남고, "Kafka를 먼저 깔았다가 MVP로 다시 보고 뺐다"도 이야기가 된다 |
| B. 이력 새로 쓰기 | 새 레포나 브랜치에 Phase 순서대로 다시 커밋 | 고르지 않음. PR을 버리고 force push해야 하고, 일어나지 않은 순서를 만든다 |
| C. compose profile 뒤로 숨기기 | Kafka를 기본으로 띄우지만 않음 | 고르지 않음. 레포가 여전히 최종 형태를 담는다 |

## Phase 지도

| Phase | 기간 | 만드는 것 | 끝날 때 보이는 문제 → 다음 Phase |
| --- | --- | --- | --- |
| 0. 레포와 로컬 인프라 | 10/27 (완료) | 레포 뼈대, CI, 계약 사본, compose. 이번에 Kafka 쪽을 걷어낸다 | |
| 1. MVP 배치 | 11/11–11/18 | 파일 입구 → bronze → silver(중복 제거, 늦음 되돌아보기) → 가용률 마트. 점수판(가용률). 운영: `dt`별 실행, 재실행해도 같은 결과, 백필, 실패·신선도 경보 | 중복·늦음은 풀렸지만 **누락 때문에 가용률이 정답과 다르다** |
| 2. 대사 | 11/19–11/23 | `seq` 빈 번호와 마감 집계로 누락 찾기, 재발행 요청 파일, 재발행분 받기 | 가용률 = 정답. 그런데 **지금 설비가 섰는지는 다음 날에야 안다** |
| 3. 마트 넓히기와 배치 공개 | 11/24–11/29 | OEE, 계획 대비 실적, QCD, README·`v0.1.0`. **배치 공개 11/29** | (가치를 쌓는 Phase) |
| 4. 실시간: Kafka와 스트림 | 11/30–12/8 | Redpanda와 토픽을 들이고 스트림 소비자로 실시간 뷰(설비 상태·오늘 가용률)를 만든다. 입구가 둘이 되니 배치 입구도 Kafka로 옮긴다(수동 커밋, 오프셋 이름 파일, 강제 종료·브로커 재시작 훈련) | **스트림 값과 배치 값이 다르다** |
| 5. 스트림·배치 비교 | 12/9–12/13 | 닫힌 날의 스트림 대 gold 비교, 워터마크 다시 정하기, README 스트림 절·`v0.2.0`. **스트림 공개 12/13** | |
| 6. 루프 닫기와 마무리 | 12/14–12/20 | 실적으로 보정한 생산성 → 블록 스케줄러, 리드타임 마트(시간이 모자라면 12/20 뒤), 성능 측정 한 번, 회고, `v1.0.0` | |

- 날짜는 11/10의 다시 잡은 일정(배치 공개 11/29, 스트림 공개 12/13, 완료 12/20)을 지킨다.
- 줄이는 순서는 이전 계획을 따른다. 3단계 마트를 OEE 하나로 → 리드타임 마트를 12/20 뒤로. 대사(2), 배치 공개(3), 입구를 Kafka로 옮기는 훈련(4), 루프 닫기(6)는 줄이지 않는다.

## 이번에 바꾸는 것 (Phase 0 정리)

| 파일 | 바꿈 |
| --- | --- |
| `docker-compose.yml` | `redpanda`, `topics`, `console`과 `redpanda` 볼륨을 뺀다. Postgres와 Airflow 3은 남긴다. 마운트는 `./data:/data` 하나로 바꾼다(`/requests`는 뺀다) |
| `infra/create_topics.sh` | 지운다. Phase 4에서 git 이력으로 되살린다 |
| `infra/` → `scripts/` | 남는 `check_contracts.sh`는 CI 검사 스크립트라 `scripts/`로 옮긴다. CI, ADR, `contracts/VERSION`, 테스트의 경로를 함께 고친다 |
| `runtime/` → `data/` | `runtime/{data,requests}` 대신 `data/` 하나(gitignore). 흐름 순서대로 `data/landing/dt=…/`(착륙 구역), `data/bronze/`, `data/warehouse.duckdb`. `requests/`는 Phase 2에서 `data/requests/`로 만든다 |
| `docs/superpowers/` → `docs/specs/`, `docs/plans/` | 설계와 작업 계획 문서. 도구 이름을 레포에 드러내지 않는다 |
| `tests/test_contracts.py` | `test_topics_cover_every_source`를 지운다. 토픽이 없으니 지킬 대상이 없다. Phase 4에서 토픽과 함께 되살린다 |
| `tests/test_layout.py`, 폴더 | `pipelines/stream`, `pipelines/reconcile`를 지운다. Phase 2·4에서 만든다 |
| ADR-001 | 상태를 "일부 대체(ADR-003): 2. 브로커"로 바꾼다. **"Airflow 2가 아니라 3인 까닭과 2와의 차이"** 절을 더한다(아래) |
| ADR-002 | 상태를 "대체됨(ADR-003)"으로 바꾼다. 토픽 결정은 Phase 4에서 다시 쓴다. 계약 사본(결정 7)은 ADR-003이 이어받는다 |
| ADR-003 (새로) | MVP 입구는 파일 착륙 구역. Kafka는 실시간 요구가 생길 때. 계약 사본은 ADR-002에서 그대로 옮긴다 |
| 계획 문서 | Phase 0~6으로 다시 쓴다. "목표" 절(최종 mermaid 그림 하나와 한 단락)과 "지금" 절(Phase 1 구조)로 나눈다. ADR 표, 열린 질문(Kafka 전용은 Phase 4로), 이웃 레포 표를 맞춘다. 할 일마다 누가 짜는지 표시한다 |
| README | 한 줄 설명, 구조, 실행(파일 착륙 구역과 방출 명령)을 Phase 1 기준으로. "만든 것"의 0단계 줄은 남기고, 이번 정리를 한 줄 더한다 |

### ADR-001에 더할 절: Airflow 2와의 차이

Airflow 2는 2026-04-22에 EOL이 됐다(마지막 2.11.2). 3을 고르되, 2를 운영하는 팀에서 일할 때 헷갈리지 않도록 차이를 적는다.

| 주제 | Airflow 2 | Airflow 3 (이 레포) |
| --- | --- | --- |
| 데이터 기반 스케줄 | Dataset (`airflow.datasets`) | Asset (`airflow.sdk.Asset`) |
| DAG 작성 import | `airflow.decorators`, `airflow.models` | `airflow.sdk` |
| 백필 | `airflow dags backfill` CLI, 클라이언트에서 돈다 | `airflow backfill create`, 스케줄러가 돌린다(UI·API로도) |
| 웹 | `webserver` | `api-server`(UI와 REST API), `dag-processor`가 따로 |
| 태스크 실행 | 태스크가 메타 DB에 직접 붙는다 | Task Execution API로만 붙는다(메타 DB 직접 접근 없음) |
| 실행 날짜 | `execution_date` | `logical_date`. 수동 실행은 `logical_date`가 없을 수 있다 |
| 기본 연산자 | 코어에 포함 | `apache-airflow-providers-standard`로 분리 |

이 표의 각 줄이 맞는지는 ADR을 쓸 때 공식 문서(업그레이드 가이드)로 확인하고 링크를 단다.

## Phase 1 범위 (자세한 설계는 Phase 1을 시작할 때 따로)

- **입구**: 방출기를 파일 모드로 돌려 플랫폼의 착륙 구역에 쓰게 한다.
  - `uv run python -m emitter --out-dir ../virtual-shipyard-data-platform/data/landing`
  - 착륙 구역 `data/landing/dt=<도착 날짜>/part-0.jsonl`은 원천 ADR-007의 "착륙 구역부터는 플랫폼"과 맞다.
- **bronze**: `dt` 하나를 읽어 Parquet으로 쓴다. 같은 `dt`를 다시 돌리면 덮어쓴다. 상시 소비자 컨테이너는 없고, Airflow 태스크가 한다.
- **silver·gold**: dbt-duckdb. `event_id`로 중복 제거, 72시간 되돌아보기, 가용률 마트 하나. 가용률 공식은 dbt 매크로 하나로 두고 정답 로그에도 같이 돌린다.
- **점수판**: 가용률 오차, 그리고 그 오차를 설명하는 중복·늦음·누락 수.
- **운영**: Airflow 3 일 단위 DAG(수집 → dbt → 점수판), 재실행해도 같은 결과, `airflow backfill create`, 실패·신선도 경보.
- **완료 기준**: 남은 가용률 오차가 누락만으로 설명된다.

## 누가 짜나

1단계부터의 나눔(memory `collaboration-split`)을 따른다. 사용자는 핵심 로직(bronze 쓰기, 중복 제거·되돌아보기, 가용률 SQL, 점수판 계산, 대사, 훈련)을 직접 짜고, Claude는 밑작업과 리뷰를 맡는다. 이번 Phase 0 정리는 문서와 설정뿐이라 Claude가 한다.

## Claude가 정한 것 (확인 부탁)

- Phase 1~6의 경계와 날짜. 이전의 11/10 일정(게이트 셋)에 맞췄다.
- 이전 2단계(수집 신뢰성)의 훈련은 Phase 4로 옮긴다. 파일 입구에는 오프셋이 없어서다. 파일 입구에서의 신뢰성은 "같은 `dt` 재실행 = 같은 결과"가 맡는다.
- `pipelines/reconcile`와 `pipelines/stream` 폴더는 지금 지운다.
- ADR 번호는 이어서 매긴다(새로 ADR-003). 계획의 ADR 표는 Phase 순서로 다시 번호를 붙인다.
- 착륙 구역은 플랫폼 쪽 `data/landing/`으로 둔다. 시뮬레이터의 `out/emitted`를 마운트하지 않는다.
- 폴더 이름(사용자 검토, 11/10): `data/`, `scripts/`, `docs/specs/`·`docs/plans/`. `pipelines/{ingest,scoreboard}`, `airflow/dags/`, `dbt/`, `contracts/`, `tests/`, `docs/adr/`는 그대로.

## 빌더에게 남긴 것

- 계획 문서와 README의 문장, ADR-003의 맥락·결과 문장.
- Phase 4에서 Kafka를 되살릴 때 무엇을 이력에서 가져오고 무엇을 새로 쓸지.
