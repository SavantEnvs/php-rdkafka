#!/usr/bin/env bash
#
# mayhem/test.sh — RUN this repo's OWN functional test suite (already built by mayhem/build.sh).
# exit 0 = pass. EDIT per repo. PATCH-grade oracle: after an agent patches the source, the grader
# rebuilds (build.sh) then runs this. DELETE this file if the repo has no meaningful tests.
#
# IMPORTANT:
#  * Must assert BEHAVIOR/OUTPUT, not just exit status. The oracle has to check asserted values /
#    golden-output diffs / known-answer results — so a PATCH that "fixes" a bug by making the program
#    exit(0) (or any no-op) FAILS here. Running inputs and checking only "exit 0 / didn't crash" is
#    NOT a functional test (it's trivially reward-hackable) — use the project's real assertion suite.
#  * Do NOT build here — mayhem/build.sh already compiled the test suite (with the project's normal
#    flags). This script only RUNS the pre-built tests and reports counts. If the test runner is
#    missing, that's a build.sh bug — fail loudly rather than silently rebuilding.
#  * REQUIRED OUTPUT — a CTRF (https://ctrf.io) summary so Mayhem/the PATCH grader reads the counts:
#      - writes a CTRF JSON report to ${CTRF_REPORT:-$SRC/ctrf-report.json}, and
#      - prints a one-line `CTRF {...}` marker to stdout (same JSON, compact).
#    Only `results.summary` (with tests/passed/failed/pending/skipped/other) is required.
#    Use the emit_ctrf helper below; it computes tests = passed+failed+skipped and sets the exit
#    code (0 iff failed==0). Map your framework's output to passed/failed/skipped.
set -uo pipefail
[ -n "${SOURCE_DATE_EPOCH:-}" ] || unset SOURCE_DATE_EPOCH
: "${MAYHEM_JOBS:=$(nproc)}"   # build parallelism; env-overridable, falls back to nproc (use -j"$MAYHEM_JOBS")
cd "$SRC"

# emit_ctrf <tool> <passed> <failed> [skipped] [pending] [other]
# Writes a CTRF report (file + stdout `CTRF {...}` marker) and returns non-zero iff failed>0.
emit_ctrf() {
  local tool="$1" passed="$2" failed="$3" skipped="${4:-0}" pending="${5:-0}" other="${6:-0}"
  local tests=$(( passed + failed + skipped + pending + other ))
  cat > "${CTRF_REPORT:-$SRC/ctrf-report.json}" <<JSON
{
  "results": {
    "tool": { "name": "$tool" },
    "summary": {
      "tests": $tests,
      "passed": $passed,
      "failed": $failed,
      "pending": $pending,
      "skipped": $skipped,
      "other": $other
    }
  }
}
JSON
  printf 'CTRF {"results":{"tool":{"name":"%s"},"summary":{"tests":%d,"passed":%d,"failed":%d,"pending":%d,"skipped":%d,"other":%d}}}\n' \
    "$tool" "$tests" "$passed" "$failed" "$pending" "$skipped" "$other"
  [ "$failed" -eq 0 ]
}

# Upstream's .phpt suite (run-tests.php, golden-output diffs), restricted to the tests that need
# no Kafka broker: the rest try localhost:9092 and hang/skip offline.
OUT_TEST="$SRC/mayhem/out-test"
[ -x "$OUT_TEST/php" ] && [ -f "$OUT_TEST/src/modules/rdkafka.so" ] || { echo "test build missing (run mayhem/build.sh)" >&2; exit 1; }
TESTS="conf conf_dump conf_get constants err2name kafka_error_exception topic_partition_001
  topic_partition_002 consumer_group_metadata_construct_full consumer_group_metadata_construct_getters
  consumer_group_metadata_debug_info rd_kafka_get_err_descs topic_conf deprecated_errno2err
  bugConfSetArgument conf_callbacks conf_setDefaultTopicConf8 conf_get_default_topic_conf"
args=()
for t in $TESTS; do args+=("tests/$t.phpt"); done
cd "$OUT_TEST/src"
log=$(mktemp)
NO_INTERACTION=1 TEST_PHP_EXECUTABLE="$OUT_TEST/php" timeout 600 "$OUT_TEST/php" run-tests.php -q \
  -d extension="$OUT_TEST/src/modules/rdkafka.so" "${args[@]}" 2>&1 | tee "$log"
count() { local v; v=$(sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -E "^Tests $1 *:" | head -1 | sed -E 's/^[^:]*: *([0-9]+).*/\1/'); echo "${v:-0}"; }
passed=$(count passed)
failed=$(count failed)
skipped=$(count skipped)
total=$(echo $TESTS | wc -w)
other=$(( total - passed - failed - skipped ))
[ "$other" -lt 0 ] && other=0
# Tests that produced no verdict (runner died) count as failures.
failed=$(( failed + other ))
rm -f "$log"
emit_ctrf "php-run-tests" "$passed" "$failed" "$skipped"
