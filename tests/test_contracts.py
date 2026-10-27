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
    # 봉투의 $ref("payloads/<종류>.json")는 $id 기준 상대 경로라 같은 $id로 등록한다
    schemas = [CONTRACTS / "envelope.json", *(CONTRACTS / "payloads").glob("*.json")]
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in map(_load, schemas)
    )
    return Draft202012Validator(_load(CONTRACTS / "envelope.json"), registry=registry)


def test_pinned_to_source_tag():
    pin = (CONTRACTS / "VERSION").read_text(encoding="utf-8")
    assert re.search(r"^tag: v0\.3\.0$", pin, re.M)
    assert re.search(r"^commit: [0-9a-f]{40}$", pin, re.M)


def test_every_event_type_has_payload_schema_and_example():
    envelope = _load(CONTRACTS / "envelope.json")
    assert envelope["properties"]["event_type"]["enum"] == EVENT_TYPES
    assert sorted(p.stem for p in (CONTRACTS / "payloads").glob("*.json")) == sorted(EVENT_TYPES)
    assert sorted(p.stem for p in EXAMPLES) == sorted(EVENT_TYPES)


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_examples_pass_envelope(path):
    errors = list(_envelope_validator().iter_errors(_load(path)))
    assert not errors, [e.message for e in errors]


def test_envelope_checks_payload():
    event = _load(CONTRACTS / "examples" / "valid" / "equipment.status_changed.json")
    event["payload"]["status"] = "broken"
    assert not _envelope_validator().is_valid(event)


def test_topics_cover_every_source():
    # 토픽은 <source>.events·<source>.manifest(ADR-002). 원천의 출처 시스템과 같아야 한다
    sources = _load(CONTRACTS / "envelope.json")["properties"]["source"]["enum"]
    script = (ROOT / "infra" / "create_topics.sh").read_text(encoding="utf-8")
    topic_sources = re.search(r"^SOURCES=\((.*)\)$", script, re.M).group(1).split()
    assert sorted(topic_sources) == sorted(sources)


def test_replay_request_schema():
    v = Draft202012Validator(_load(CONTRACTS / "replay_request.json"))
    assert v.is_valid({"entity_key": "H-2601-B006", "seq": 3})
    assert not v.is_valid({"entity_key": "H-2601", "seq": 0})
