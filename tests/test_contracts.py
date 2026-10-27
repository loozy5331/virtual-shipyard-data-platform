"""원천 계약 사본(contracts/)을 지키는 테스트(ADR-002).

사본이 원천 태그와 같은지는 CI의 infra/check_contracts.sh가 본다. 여기서는
사본이 고정한 버전인지, 사본 안에서 앞뒤가 맞는지, 플랫폼의 토픽과 맞는지를 본다.
실제로 온 메시지의 검증은 3단계(feat/contract)에서 이 사본으로 한다.
"""

import json
import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parent.parent
CONTRACTS = ROOT / "contracts"
EXAMPLES = sorted((CONTRACTS / "examples" / "valid").glob("*.json"))

# 원천 v0.3.0의 이벤트 종류. 원천이 종류를 더하면 contracts/를 다시 복사하고 여기를 고친다
EVENT_TYPES = [
    "order.contracted",
    "block.process_started",
    "block.process_completed",
    "hull.process_started",
    "hull.process_completed",
    "hull.milestone_reached",
    "equipment.status_changed",
    "material.ordered",
    "material.received",
    "system.manifest_published",
]


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _envelope_validator() -> Draft202012Validator:
    """봉투와 payload 10종을 함께 검사하는 검증기. 3단계 수집 검증도 같은 방식으로 만든다."""
    # 봉투의 $ref("payloads/<종류>.json")는 $id 기준 상대 경로라 같은 $id로 등록한다
    schemas = [CONTRACTS / "envelope.json", *(CONTRACTS / "payloads").glob("*.json")]
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in map(_load, schemas)
    )
    return Draft202012Validator(_load(CONTRACTS / "envelope.json"), registry=registry)


def test_pinned_to_source_tag():
    """사본이 원천 v0.3.0 태그와 그 커밋에 고정되어 있다.

    VERSION의 commit은 check_contracts.sh가 원천을 받아 비교하는 기준이다.
    원천 태그를 올릴 때는 이 테스트가 먼저 깨지므로, 바뀐 계약을 보고 함께 고친다.
    """
    pin = (CONTRACTS / "VERSION").read_text(encoding="utf-8")
    assert re.search(r"^tag: v0\.3\.0$", pin, re.M)
    assert re.search(r"^commit: [0-9a-f]{40}$", pin, re.M)


def test_every_event_type_has_payload_schema_and_example():
    """봉투의 event_type 목록, payload 스키마, 예시가 같은 10종이다.

    봉투에 종류가 더해졌는데 payload 스키마가 없으면 그 종류의 payload는 검사되지 않는다
    (봉투의 payload는 "object"뿐이고 종류별 스키마는 $ref로만 걸린다).
    """
    envelope = _load(CONTRACTS / "envelope.json")
    assert envelope["properties"]["event_type"]["enum"] == EVENT_TYPES
    assert sorted(p.stem for p in (CONTRACTS / "payloads").glob("*.json")) == sorted(EVENT_TYPES)
    assert sorted(p.stem for p in EXAMPLES) == sorted(EVENT_TYPES)


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_examples_pass_envelope(path):
    """원천이 낸 예시가 사본의 봉투를 통과한다. 사본을 복사하다 빠지거나 깨진 파일을 잡는다."""
    errors = list(_envelope_validator().iter_errors(_load(path)))
    assert not errors, [e.message for e in errors]


def test_envelope_checks_payload():
    """봉투 검증이 payload까지 들어간다.

    $ref가 풀리지 않으면 payload는 아무 object나 통과해, 위 예시 테스트만으로는 알 수 없다.
    payload 값만 깨뜨린 이벤트가 실패하는 것으로 $ref가 실제로 걸렸음을 확인한다.
    """
    event = _load(CONTRACTS / "examples" / "valid" / "equipment.status_changed.json")
    event["payload"]["status"] = "broken"
    assert not _envelope_validator().is_valid(event)


def test_topics_cover_every_source():
    """토픽 스크립트의 출처 시스템이 봉투의 source와 같다.

    토픽은 <source>.events·<source>.manifest이고 자동 생성을 껐다(ADR-002).
    원천에 출처가 더해졌는데 토픽이 없으면 방출기는 버리지 않고 기다리기만 해(원천 ADR-007)
    오류 없이 멈춘 것처럼 보인다. 그 전에 여기서 깨진다.
    """
    sources = _load(CONTRACTS / "envelope.json")["properties"]["source"]["enum"]
    script = (ROOT / "infra" / "create_topics.sh").read_text(encoding="utf-8")
    topic_sources = re.search(r"^SOURCES=\((.*)\)$", script, re.M).group(1).split()
    assert sorted(topic_sources) == sorted(sources)


def test_replay_request_schema():
    """재발행 요청 한 줄(entity_key, seq)의 형식. 4단계에서 플랫폼이 이 형식으로 요청을 쓴다.

    seq는 1부터라 0은 틀린 요청이다. 형식이 틀린 줄이 하나라도 있으면 원천은 파일 전체를
    보내지 않으므로(원천 README), 플랫폼이 쓰기 전에 이 스키마로 걸러야 한다.
    """
    v = Draft202012Validator(_load(CONTRACTS / "replay_request.json"))
    assert v.is_valid({"entity_key": "H-2601-B006", "seq": 3})
    assert not v.is_valid({"entity_key": "H-2601", "seq": 0})
