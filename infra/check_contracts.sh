#!/usr/bin/env bash
# contracts/가 원천의 고정 커밋(contracts/VERSION)의 schemas/와 같은지 본다(ADR-002).
# 다르면 diff를 보이고 실패한다. CI와 로컬에서 같이 쓴다: bash infra/check_contracts.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
VERSION="$ROOT/contracts/VERSION"
repo=$(sed -n 's/^repo: //p' "$VERSION")
commit=$(sed -n 's/^commit: //p' "$VERSION")

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git clone -q --filter=blob:none --no-checkout "$repo" "$tmp/src"
mkdir "$tmp/schemas"
git -C "$tmp/src" archive "$commit" schemas | tar -x --strip-components=1 -C "$tmp/schemas"

if diff -r --exclude=VERSION "$tmp/schemas" "$ROOT/contracts"; then
  echo "contracts/ = $repo@$commit schemas/"
else
  echo "contracts/가 원천과 다르다. 손으로 고치지 말고 원천 태그에서 다시 복사한다(contracts/VERSION)" >&2
  exit 1
fi
