# Phase 0 정리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 레포에서 Kafka 쪽과 앞 Phase의 폴더를 걷어내고, 폴더 이름과 ADR·계획·README를 Phase 1(파일 입구 MVP) 기준으로 맞춘다.

**Architecture:** 코드는 없고 설정·문서·테스트만 바꾼다. 관심사 하나 = PR 하나(develop, CI 통과 시 자동 머지). 순서: Kafka 걷어내기 → 폴더 이름 → ADR → 계획·README.

**Tech Stack:** docker compose, Airflow 3.3.2, uv·ruff·pytest, GitHub Actions

**Spec:** `docs/specs/2026-11-10-phased-replan-design.md`

## Global Constraints

- 문서는 한국어, 프로그래밍 용어는 영어. 기존 문서의 문체(짧은 평서문, "~다")를 따른다.
- 커밋 메시지는 `<type>: <한국어 요약>`, 끝에 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. PR 본문 끝에 `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- 브랜치는 develop에서 따고 develop으로 PR. 머지 뒤 원격 브랜치를 `git push origin --delete <branch>`로 지운다.
- git 이력은 다시 쓰지 않는다(force push 없음).
- Airflow는 3을 유지한다(`apache/airflow:3.3.2-python3.12`).
- 폴더: `data/`(gitignore), `scripts/`, `docs/specs/`, `docs/plans/`. `pipelines/{ingest,scoreboard}`, `airflow/dags/`, `dbt/`, `contracts/`, `tests/`, `docs/adr/`는 그대로.
- 모든 PR에서 `uv run ruff check .`, `uv run pytest`, `bash scripts/check_contracts.sh`(Task 2 전에는 `infra/`)가 통과한다.

## Review Focus

1. 옛 경로가 남은 곳: `infra/`, `runtime/`, `/requests`, `create_topics`, `redpanda`, 옛 계획 파일 이름. 각 Task 끝에 `git grep`으로 0건을 확인한다.
2. compose가 문법상 맞고 Airflow만으로 뜨는가: Task 1에서 `docker compose config -q`로 확인한다(docker가 꺼져 있어도 된다). 실제로 띄우는 확인은 Task 1 Step 6.
3. ADR-001의 Airflow 2/3 차이표가 사실인가: Task 3에서 공식 업그레이드 문서를 열어 줄마다 확인하고 링크를 단다. 확인되지 않은 줄은 넣지 않는다.
4. 계획 문서가 "지금 있는 것만" 설명하는가: Task 4 Step 3의 grep으로 Phase 1 이전 설명에 Kafka·토픽이 나오지 않는지 확인한다(목표 절과 Phase 4 이후는 예외).
5. 로컬에 남은 `runtime/` 디렉터리: gitignore라 git이 모른다. Task 2 Step 5에서 비어 있는지 보고 지운다.

---

### Task 1: Kafka 쪽 걷어내기 (`chore/drop-kafka`)

**Files:**
- Modify: `docker-compose.yml`
- Delete: `infra/create_topics.sh`
- Modify: `tests/test_contracts.py` (`test_topics_cover_every_source` 삭제)

**Interfaces:** 없음

- [ ] **Step 1: `docker-compose.yml`에서 `redpanda`, `topics`, `console` 서비스와 `redpanda` 볼륨을 지운다.** 머리 주석의 방출기·Redpanda·Console 줄도 지우고 "Airflow는 http://localhost:8080"만 남긴다. 머리 주석의 첫 줄은 `# 플랫폼 로컬 인프라(Phase 1: 파일 입구 배치). docker compose up -d`로 바꾼다.
- [ ] **Step 2: `infra/create_topics.sh`를 지운다** (`git rm`).
- [ ] **Step 3: `tests/test_contracts.py`에서 `test_topics_cover_every_source`를 지운다.** `re`는 `test_pinned_to_source_tag`가 쓰므로 import는 남긴다.
- [ ] **Step 4: 확인**

Run: `docker compose config -q && uv run ruff check . && uv run pytest -q`
Expected: 오류 없음, 테스트 모두 PASS

- [ ] **Step 5: 남은 참조 확인**

Run: `git grep -nE "redpanda|create_topics|9092|8081" -- ':!docs/adr' ':!docs/specs' ':!docs/development-plan.md' ':!README.md'`
Expected: 0건(ADR·설계·계획·README는 Task 3·4에서 고친다)

- [ ] **Step 6: docker가 켜져 있으면 실제로 띄워 본다**

Run: `docker compose up -d && docker compose ps --format '{{.Service}} {{.State}}'`
Expected: postgres, airflow-apiserver, airflow-scheduler, airflow-dag-processor가 running, airflow-init은 exited(0). 꺼져 있으면 건너뛰고 PR에 적는다.

- [ ] **Step 7: 커밋하고 PR**

```bash
git add -A && git commit -m "chore: compose에서 Redpanda·Console·토픽을 걷어냄(Phase 1은 파일 입구)"
```

---

### Task 2: 폴더 이름 (`chore/folders`)

**Files:**
- Move: `infra/check_contracts.sh` → `scripts/check_contracts.sh`
- Modify: `.github/workflows/ci.yml:21`, `scripts/check_contracts.sh:3`(주석의 경로)
- Modify: `docker-compose.yml`(마운트), `.gitignore`
- Delete: `pipelines/reconcile/`, `pipelines/stream/`
- Modify: `tests/test_layout.py`, `tests/test_contracts.py:3`(docstring의 경로)

**Interfaces:** 없음

- [ ] **Step 1: 실패하는 테스트로 바꾼다.** `tests/test_layout.py`의 `FOLDERS`를 아래로 바꾸고, docstring의 `infra/`를 `scripts/`로, "계획 0단계"를 "설계 docs/specs/2026-11-10-phased-replan-design.md"로 고친다.

```python
FOLDERS = [
    "pipelines/ingest",
    "pipelines/scoreboard",
    "airflow/dags",
    "dbt",
    "contracts",
    "scripts",
    "docs/adr",
    "docs/specs",
    "docs/plans",
    "tests",
]
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_layout.py -q`
Expected: FAIL, `missing == ['scripts']`

- [ ] **Step 3: 옮기고 지운다**

```bash
mkdir -p scripts
git mv infra/check_contracts.sh scripts/check_contracts.sh
git rm -r pipelines/reconcile pipelines/stream
```

`scripts/check_contracts.sh`의 주석 `bash infra/check_contracts.sh` → `bash scripts/check_contracts.sh`, `ci.yml`의 `run: bash infra/check_contracts.sh` → `run: bash scripts/check_contracts.sh`, `tests/test_contracts.py` docstring의 `infra/check_contracts.sh` → `scripts/check_contracts.sh`.

- [ ] **Step 4: compose와 gitignore**

`docker-compose.yml`의 `x-airflow.volumes`에서 `./runtime/data:/data`와 `./runtime/requests:/requests`를 `./data:/data` 한 줄로 바꾼다. `.gitignore`의 `runtime/`을 `data/`로 바꾼다.

- [ ] **Step 5: 로컬 `runtime/`을 정리한다**

Run: `find runtime -type f | wc -l`
Expected: `0`이면 `rm -r runtime`. 0이 아니면 멈추고 사용자에게 무엇이 있는지 보인다.

- [ ] **Step 6: 확인**

Run: `uv run pytest -q && uv run ruff check . && bash scripts/check_contracts.sh && docker compose config -q`
Expected: 모두 통과, 마지막에 `contracts/ = …@521d804… schemas/`

Run: `git grep -nE "infra/|runtime/|/requests" -- ':!docs/adr' ':!docs/specs' ':!docs/development-plan.md' ':!README.md'`
Expected: 0건

- [ ] **Step 7: 커밋하고 PR**

```bash
git add -A && git commit -m "chore: 폴더 이름 정리(infra→scripts, runtime→data), 앞 Phase 폴더 걷어냄"
```

---

### Task 3: ADR (`docs/adr-003-file-landing`)

**Files:**
- Create: `docs/adr/003-file-landing-zone.md`
- Modify: `docs/adr/001-stack.md`, `docs/adr/002-source-contract-and-topics.md`

**Interfaces:** ADR-003은 Phase 1 설계가 기대는 착륙 구역 계약이다: 경로 `data/landing/dt=<도착 날짜 YYYY-MM-DD>/part-0.jsonl`, 한 줄 = 봉투 JSON, `dt` = `emitted_at`의 날짜(+09:00).

- [ ] **Step 1: 공식 문서로 Airflow 2/3 차이를 확인한다.** Airflow 3 업그레이드 가이드(`https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html`)와 지원 버전 표(`…/installation/supported-versions.html`)를 열어, 설계 문서의 차이표 일곱 줄(Dataset↔Asset, import, backfill, webserver↔api-server, Task Execution API, `execution_date`↔`logical_date`, standard provider)을 줄마다 대조한다. 확인된 줄만 남기고, 줄마다 근거 링크를 단다.
- [ ] **Step 2: ADR-001을 고친다.**
  - 상태 줄: `- 상태: 확정, 일부 대체(ADR-003: 2. 브로커는 Phase 4에서 다시 정한다)`
  - 결정 1의 `브로커, 콘솔,`과 결정 2 전체, 결과의 Redpanda 언급에 "(Phase 4로 미룸, ADR-003)"를 덧붙인다. 지우지 않는다(결정 기록이라서).
  - 끝에 `## Airflow 2가 아니라 3인 까닭과 2와의 차이 (2026-11-10 추가)` 절: 한 단락(2.x EOL 2026-04-22, 마지막 2.11.2, 2를 운영하는 팀에서 헷갈리지 않도록 적는다) + Step 1에서 확인한 표 + 출처 링크.
- [ ] **Step 3: ADR-002를 고친다.** 상태 줄을 `- 상태: 대체됨(ADR-003). 토픽 결정(1~6)은 Phase 4에서 다시 쓴다. 계약 사본(7)은 ADR-003이 이어받는다`로 바꾸고, 본문의 `infra/`를 `scripts/`로 고친다. 그 밖의 본문은 그대로 둔다.
- [ ] **Step 4: ADR-003을 쓴다.** 상태 확정, 날짜 2026-11-10. 맥락·결정·결과 형식(ADR-002와 같은 모양):
  - 맥락: 0단계가 Redpanda와 토픽 여섯을 먼저 깔았다. MVP가 없고 Kafka를 들인 까닭을 겪은 문제로 설명할 수 없다. 원천 ADR-007은 파일 모드의 착륙 구역(`out/emitted`)도 정해 두었다.
  - 결정: (1) Phase 1~3의 입구는 파일 착륙 구역 `data/landing/`, 방출기 `--out-dir`로 쓴다. (2) 같은 `dt`를 다시 읽어도 결과가 같아야 한다(bronze는 `dt`마다 덮어쓴다). (3) Kafka는 실시간 요구(Phase 4)가 생길 때 들이고, 그때 배치 입구도 옮긴다. (4) 계약 사본은 ADR-002 결정 7을 그대로 잇는다(`scripts/check_contracts.sh`).
  - 결과: 오프셋·리밸런스 훈련은 Phase 4로 간다. 착륙 구역 보존·정리는 플랫폼이 한다. Phase 4에서 `infra/create_topics.sh`와 compose의 Redpanda는 git 이력(PR #2)에서 되살린다.
- [ ] **Step 5: 확인**

Run: `git grep -nE "infra/" -- docs/adr`
Expected: ADR-003의 "git 이력에서 되살린다" 한 곳만

- [ ] **Step 6: 커밋하고 PR**

```bash
git add -A && git commit -m "docs: ADR-003 파일 착륙 구역, ADR-002 대체, ADR-001에 Airflow 2와의 차이"
```

---

### Task 4: 계획과 README (`docs/plan-phases`)

**Files:**
- Modify: `docs/development-plan.md` (다시 쓴다)
- Modify: `README.md`

**Interfaces:** 계획 문서의 Phase 지도와 날짜는 설계 문서의 "Phase 지도" 표를 그대로 쓴다.

- [ ] **Step 1: 계획 문서를 다시 쓴다.** 제목은 `# 가상 조선소 데이터 플랫폼 개발 계획`. 절 순서:
  1. 머리말(기준일 2026-11-10, 원천 링크), 한 단락 소개, **진행 상황**(Phase 0 완료 10/27, 11/10 Phase로 다시 짬, 그 까닭 한 줄)
  2. **목표**: 최종 mermaid 그림(지금 문서의 그림 그대로) + 한 단락. "레포는 Phase마다 필요한 만큼만 자란다"
  3. **지금(Phase 1)**: 파일 착륙 구역 → bronze → silver → 가용률 마트 → 점수판, Airflow DAG. mermaid 그림 하나(노드 다섯~여섯)
  4. **원칙**: 지금 문서의 원칙에서 Kafka 전용 줄(입구는 0단계부터 Kafka 등)을 고쳐 "필요할 때 들인다"를 더하고, "누가 짜나" 한 줄(핵심 로직은 사용자, 밑작업·리뷰는 Claude)
  5. **Phase 지도**: 설계 문서의 표 + 게이트 표(11/29, 12/13, 12/20) + 줄이는 순서
  6. **Phase별 할 일**: Phase 1~6. 항목마다 브랜치 이름과 `(직접)`/`(Claude)` 표시. 이전 계획의 1~11단계 항목을 Phase로 옮긴다(이전 2단계 훈련 → Phase 4, 3단계 silver → Phase 1, 4단계 대사 → Phase 2, 5단계 운영 → Phase 1, 6단계 마트 → Phase 3, 7·8 → Phase 4·5, 9 리드타임·10·11 → Phase 6)
  7. **점수판**: Phase 1은 가용률 오차 + 그것을 설명하는 중복·늦음·누락 수. 칸이 Phase마다 느는 표
  8. **이웃 레포**: 시뮬레이터가 `v0.5.0`(컨테이너, Kafka 재발행, 재생 속도 완료)이라 플랫폼이 기다릴 것이 없다고 고친다. 파일 모드에서는 방출기 `--out-dir`를 쓴다
  9. **결정 기록과 열린 질문**: ADR 001~003(있음) + Phase별로 쓸 ADR(004 silver·늦음, 005 운영·백필, 006 대사, 007 Kafka 입구·전달 보장, 008 스트림). 열린 질문은 지금 것을 Phase 1(DAG 단위, `dt` 닫힘, DuckDB 잠금, 72시간 넘는 늦음)·Phase 2(통째로 빠진 개체)·Phase 4(bronze 묶음 크기, 원자적 쓰기, 파티션별 워터마크, 재생 속도)로 나눈다
  10. **참고**: 지금 그대로
- [ ] **Step 2: README를 고친다.** 한 줄 설명을 "파일 착륙 구역으로 받아 시작해 Phase마다 자란다"로. 구조 표를 새 폴더로(`data/`, `scripts/`, `docs/specs/`·`docs/plans/`, `pipelines/{ingest,scoreboard}`). 실행: `docker compose up -d`(Airflow http://localhost:8080) + 시뮬레이터 레포에서 `uv run python -m emitter --out-dir ../virtual-shipyard-data-platform/data/landing`. "만든 것"에 한 줄: `- Phase 0 정리(11/10): MVP 없이 Kafka부터 깐 것을 파일 입구로 되돌리고 Phase로 다시 짬([설계](docs/specs/2026-11-10-phased-replan-design.md), ADR-003). 회고: 구조도를 처음 형태로 다시 보니 과했다`. 개발 절의 CI 설명은 그대로.
- [ ] **Step 3: 확인**

Run: `git grep -nE "infra/|runtime/|create_topics|단계별%20개발|9092" -- README.md docs/development-plan.md`
Expected: 0건(시뮬레이터 계획 링크의 한국어 파일 이름은 시뮬레이터 쪽 이름이 바뀔 때까지 남는다)

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: 커밋하고 PR**

```bash
git add -A && git commit -m "docs: 계획을 Phase 0~6으로 다시 쓰고 README를 Phase 1 기준으로"
```
