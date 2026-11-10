#!/bin/sh
# Runs the self-contained test suite (Linux; needs only Python 3.8+). Exit 0 = all green.
set -e
cd "$(dirname "$0")"
PY=${PYTHON:-python3}
for t in tests/test_roundtrip.py tests/test_torn_and_retention.py tests/test_guard.py; do
  echo "== $t"
  "$PY" "$t"
done
echo "ALL TESTS PASSED"
