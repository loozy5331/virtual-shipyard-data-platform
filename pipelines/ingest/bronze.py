"""착륙 구역의 dt 하나를 bronze Parquet으로 옮긴다(ADR-002).

입력: landing_root/dt=YYYY-MM-DD/part-*.jsonl(방출분 part-0, 재발행분 part-replay-*)
출력: bronze_root/dt=YYYY-MM-DD/*.parquet, 한 행 = 원본 한 줄
"""
import os
from datetime import date
from pathlib import Path

import pandas as pd


def ingest_dt(landing_root: Path, bronze_root: Path, dt: str) -> int:
    """landing_root/dt={dt}/의 part-*.jsonl을 모두 읽어 bronze_root/dt={dt}/에 Parquet으로 쓴다.

    - 열: raw(원본 줄, 줄바꿈 뺌), source_file(파일 이름), line_no(파일 안 줄 번호, 1부터)
    - 같은 dt를 다시 돌리면 그 dt의 bronze를 통째로 덮어쓴다. 같은 입력이면 같은 결과다.
    - 쓴 행 수를 돌려준다.
    """
    # check date format
    if date.fromisoformat(dt).isoformat() != dt:
        raise ValueError(f"dt={dt} is not in YYYY-MM-DD format")

    line_cnt = 0
    landing_dir = landing_root / f"dt={dt}"
    bronze_dir = bronze_root / f"dt={dt}"

    # cleanup
    if bronze_dir.exists():
        for file_path in bronze_dir.glob("*.parquet"):
            os.remove(file_path)
    os.makedirs(bronze_dir, exist_ok=True)

    if not landing_dir.exists():
        bronze_dir.rmdir()
        return 0

    for file_path in landing_dir.glob("part-*.jsonl"):
        # read jsonl
        dataset = [] # (raw, source_file, line_no)
        with open(file_path, encoding="utf-8") as f:
            lines = f.readlines()
            for cnt, line in enumerate(lines, start=1):
                dataset.append((line.rstrip('\n'), file_path.name, cnt))
                line_cnt += 1

        # write to parquet
        df = pd.DataFrame(dataset, columns=["raw", "source_file", "line_no"])
        df.to_parquet(os.path.join(bronze_dir, f"{file_path.stem}.parquet"), index=False)

    return line_cnt
