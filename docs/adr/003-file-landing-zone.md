# ADR-003 MVP 입구는 파일 착륙 구역

- 상태: 확정. ADR-002를 대체하고, ADR-001의 2(브로커)를 Phase 4로 미룬다
- 날짜: 2026-11-10

## 맥락

0단계(10/27)는 처음부터 최종 형태로 깔았다. Redpanda와 Console, 토픽 여섯(ADR-002)을 두고, 입구는 0단계부터 Kafka라고 정했다(ADR-001).
11/10에 구조도를 처음 형태로 다시 보니 과했다. MVP가 따로 없었고, Kafka를 들인 까닭을 겪은 문제로 설명할 수 없었다. "왜 Kafka인가"에 "처음부터 그렇게 설계했다"밖에 답할 수 없다.
원천은 Kafka 말고 파일로도 보낸다. 원천 ADR-007은 파일 모드의 착륙 구역(방출기의 `--out-dir`)부터는 플랫폼의 것이라고 했다. 파일 형식은 원천 ADR-005 결정 6(날짜별 `dt=<도착 날짜>/part-0.jsonl`)과 ADR-006(재발행은 `part-replay-<처리 시각>.jsonl`)이 정했다.

## 결정

1. **Phase 1~3의 입구는 파일 착륙 구역 `data/landing/`이다**
   - 원천 방출기가 `--out-dir`로 이곳에 쓴다(시뮬레이터 레포에서 `uv run python -m emitter --out-dir ../virtual-shipyard-data-platform/data/landing`).
   - 경로는 `data/landing/dt=<날짜 YYYY-MM-DD>/part-*.jsonl`이다. 한 줄이 봉투 JSON 하나다.
     - `part-0.jsonl`: 방출기가 쓴다. `dt`는 `emitted_at`(+09:00)의 날짜, 곧 도착 날짜다(원천 ADR-005).
     - `part-replay-<처리 시각>.jsonl`: 재발행 서비스가 쓴다(Phase 2). `dt`는 처리 날짜다(원천 ADR-006).
     - 그래서 bronze는 `dt`의 `part-0.jsonl` 하나가 아니라 `part-*.jsonl` 전부를 읽는다. 하나만 읽으면 Phase 2의 재발행분이 조용히 빠진다.
   - 착륙 구역의 보존과 정리, 어디까지 읽었는지는 플랫폼이 맡는다(원천 ADR-007).
2. **같은 `dt`를 다시 읽어도 결과가 같다**: bronze는 `dt`마다 덮어쓴다. 파일 입구에는 오프셋이 없으니, 수집의 신뢰성은 "같은 `dt`를 다시 돌리면 같은 결과"가 맡는다.
3. **Kafka는 실시간 요구가 생길 때 들인다(Phase 4)**
   - 하루 단위 파일 배치로는 지금 설비가 섰는지 다음 날에야 안다. 이 문제가 스트림을 부르고, 스트림이 Kafka를 부른다.
   - 그때 입구가 파일과 Kafka 둘이 되니, 배치 입구도 Kafka로 옮겨 하나로 합친다. 토픽·키·파티션·보존 기간도 그때 새 ADR로 다시 쓴다.
4. **계약 사본은 ADR-002의 결정 7을 그대로 잇는다**
   - `contracts/`는 원천 `v0.3.0`의 `schemas/`를 고정한 사본이다. 손으로 고치지 않는다.
   - CI가 대조 스크립트(`scripts/check_contracts.sh`)로 원천과 diff한다.
   - 사본과 대조 스크립트는 처음 쓰는 할 일인 Phase 1 `feat/silver`에서 레포에 들인다(그때의 원천 태그에서 복사). bronze는 줄을 그대로 담기만 해서 사본이 필요 없다(schema on read).
   - 스키마 레지스트리를 쓰지 않는 까닭도 ADR-002와 같다.
   - 실제로 온 줄을 이 사본으로 검증하고 위반은 격리 표로 보내는 일(ADR-002 결정 7의 약속)은 Phase 3(`feat/contract`)에서 한다. 그때까지는 silver가 모르는 형식의 줄에서 실패한다.

## 결과

- compose는 Phase 1 `feat/dag`에서 Postgres와 Airflow 3만으로 다시 들인다. 마운트는 `./data:/data` 하나다. 그 전의 할 일은 호스트에서 `uv run`으로 돌린다.
- 오프셋 관리, 수동 커밋, 리밸런스, 소비자 강제 종료·브로커 재시작 훈련은 Phase 4로 간다.
- Phase 4에서 Redpanda는 git 이력(PR #2의 compose와 `infra/create_topics.sh`)에서 되살린다. 그때 폴더는 `scripts/`로 둔다.
- 재생 속도 없이 방출하면 5년치 파일이 한꺼번에 생긴다. Phase 1의 신선도 경보는 시뮬레이터의 `--speed`(원천 ADR-013)나 방출을 멈췄다 다시 보내는 방법으로 훈련한다.
- 이력에는 "Kafka를 먼저 깔았다가 MVP로 다시 보고 뺐다"가 그대로 남는다.
