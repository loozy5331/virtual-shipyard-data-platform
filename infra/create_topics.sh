#!/usr/bin/env bash
# 원천 계약의 토픽(<source>.events, <source>.manifest)을 만든다. 여러 번 돌려도 같은 결과.
# events는 파티션 3개(소비자 둘의 리밸런스 훈련용), manifest는 순서가 중요해 1개.
# 보존 기간은 무기한: 5년치가 수만 건이라 작고, 메시지 시각(emitted_at)이 가상 시각이라
# 시간 기준 보존은 실제 시각과 어긋난다. 근거는 ADR-002.
set -euo pipefail

SOURCES=(erp mes eam)
EVENTS_PARTITIONS=3
MANIFEST_PARTITIONS=1

ensure() {
  local name=$1 partitions=$2
  if rpk topic describe "$name" >/dev/null 2>&1; then
    rpk topic alter-config "$name" --set retention.ms=-1
  else
    rpk topic create "$name" -p "$partitions" -r 1 -c retention.ms=-1
  fi
}

# 토픽은 이 스크립트만 만든다. 없는 토픽으로 보내면 방출기는 기다린다
rpk cluster config set auto_create_topics_enabled false

for s in "${SOURCES[@]}"; do
  ensure "$s.events" "$EVENTS_PARTITIONS"
  ensure "$s.manifest" "$MANIFEST_PARTITIONS"
done
rpk topic list
