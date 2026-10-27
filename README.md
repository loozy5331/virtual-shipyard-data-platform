# virtual-shipyard-data-platform

[가상 조선소 시뮬레이터](https://github.com/loozy5331/virtual-shipyard-simulator)가 내는 노이즈 섞인 이벤트를 받아 마트까지 만드는 데이터 플랫폼이다.
docker compose 위에서 Kafka(Redpanda) 토픽으로 받고, 같은 토픽을 배치(정확한 기록)와 스트림(지금 상태)이 서로 다른 소비자 그룹으로 나눠 받는다.
배치는 DuckDB·dbt와 Airflow 3로 중복·지연 도착·누락을 대사와 재발행으로 복원하고, 그 결과를 시뮬레이터의 정답 로그와 맞춘 점수판으로 채점한다.

- 계획: [docs/가상 조선소 데이터 플랫폼 단계별 개발 계획.md](docs/가상%20조선소%20데이터%20플랫폼%20단계별%20개발%20계획.md) ([편집용 원본](https://claude.ai/artifact/DvWWpHW7oCEF26bfxkKobW))
- 원천 쪽 계획: [시뮬레이터 단계별 개발 계획](https://github.com/loozy5331/virtual-shipyard-simulator/blob/main/docs/가상%20조선소%20시뮬레이터%20단계별%20개발%20계획.md). 플랫폼이 원천에 기대는 일은 플랫폼 계획의 "이웃 레포" 표에 있다

## 실행

```bash
docker compose up -d   # Redpanda(localhost:9092), 토픽 6개, Console(http://localhost:8081)
# 시뮬레이터 레포에서: uv run --extra kafka python -m emitter --sink kafka
```

토픽은 `scripts/create_topics.sh`만 만든다(자동 생성 꺼짐): `erp|mes|eam` × `.events`(파티션 3)·`.manifest`(파티션 1), 보존 기간 무기한.

## 개발

```bash
uv sync              # Python 3.12, 개발 도구(ruff·pytest)
uv run ruff check .
uv run pytest
```

PR은 `develop`으로 보내고, CI(`test`)가 통과하면 자동으로 머지된다.
