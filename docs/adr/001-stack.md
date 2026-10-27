# ADR-001 스택

- 상태: 확정
- 날짜: 2026-10-27

## 맥락

플랫폼은 노트북 한 대에서 돈다. 원천 5년 실행(시드 0)의 정답 이벤트는 55,803개이고, 노이즈를 더해도 수만 건이다.
그래도 모양은 실제처럼 만들고 싶다. 입구는 메시지 브로커, 변환은 오케스트레이터가 돌리는 일 단위 작업, 저장은 열 지향 파일과 SQL 모델이다.
구성 요소가 늘면 띄우고 고치는 데 시간이 든다. 하루 2~4시간으로 12/20까지 끝내야 한다.

## 결정

1. **실행은 docker compose 하나**: `docker compose up -d`로 브로커, 콘솔, 오케스트레이터, 소비자가 함께 뜬다. 컨테이너끼리는 서비스 이름(`redpanda:29092`)으로, 호스트는 `localhost`로 붙는다. 컨테이너가 쓰는 데이터는 `runtime/data/`(`/data`), 재발행 요청은 `runtime/requests/`(`/requests`)에 마운트한다.
2. **브로커는 Redpanda**: Kafka API를 그대로 쓰면서 바이너리 하나로 뜨고, 따로 띄울 컨트롤러(ZooKeeper·KRaft)가 없다. 단일 노드(`--smp=1`, `--memory=1G`), 복제 1이다. 토픽을 보는 화면은 Redpanda Console이다. 클라이언트 코드는 Kafka 클라이언트라 브로커를 Kafka로 바꿔도 그대로 쓴다.
3. **오케스트레이터는 Airflow 3, LocalExecutor + Postgres**: 공식 compose에서 Celery 워커·Redis·triggerer를 빼고 api-server·scheduler·dag-processor만 둔다. 메타 DB는 Postgres다. Airflow가 SQLite를 시험용으로만 권하고, 태스크가 함께 돌 때 메타 DB 쓰기가 겹치기 때문이다. 로그인은 끈다(SimpleAuthManager, 모두 관리자). 개발용이라 인증 비용을 들이지 않는다.
4. **저장과 변환은 Parquet + DuckDB + dbt-duckdb**
   - bronze는 Parquet 파일이다. 수집 소비자만 쓴다.
   - silver·gold는 `warehouse.duckdb` 하나에 dbt로 만든다. Airflow만 쓰고, 동시에 하나만 돌도록 pool 1·`max_active_runs=1`로 막는다(5단계).
   - 스트림 소비자의 상태는 따로 `stream.duckdb`에 둔다(7단계).
   - DuckDB 파일마다 쓰는 쪽은 하나다. DuckDB는 한 파일에 쓰는 프로세스가 하나뿐이라, 쓰는 쪽을 나누는 것이 곧 동시성 규칙이다.
5. **Spark는 쓰지 않는다**: 수만 건은 DuckDB 한 프로세스가 메모리 안에서 다루는 크기다. Spark는 JVM, 드라이버·실행기 설정, 클러스터 구성 비용이 들고, 이 크기에서는 배울 것(파티션·셔플)보다 기다리는 시간이 길다. 분산 처리가 필요한 크기는 12/20 뒤 선택 과제(15년 × 시드 5개)에서 재 보고 다시 정한다.
6. **개발 도구는 uv(Python 3.12), ruff, pytest**: CI는 같은 명령(`uv sync`, `uv run ruff check .`, `uv run pytest`)을 돌린다. 원천 레포와 같은 도구라 오가며 헷갈리지 않는다.

## 결과

- 처음부터 실제 모양으로 돌린다. 소비자 그룹, 오프셋 커밋, 리밸런스, DAG 재실행·백필을 진짜 도구로 연습한다.
- 노트북 한 대의 메모리가 한계다. Airflow 컨테이너 넷과 Redpanda·Postgres가 같이 떠야 한다. 0단계 완료 확인 때 메모리를 재고, 모자라면 Airflow를 `standalone` 컨테이너 하나로 줄인다.
- 단일 브로커·복제 1이라 브로커 장애는 "브로커 재시작" 훈련까지만 다룬다. 복제와 리더 선출은 다루지 않는다.
- DuckDB 단일 쓰기 때문에 변환은 직렬로만 돈다. 이 크기에서는 문제가 되지 않고, 11단계의 성능 측정(전체 재계산 대 증분)에서 실제로 재 본다.
- 확장 이야기는 "Spark를 몰라서"가 아니라 "이 크기에서 재 보고 고르지 않았다"로 할 수 있어야 한다. 그래서 규모 실험을 선택 과제로 남겨 둔다.
