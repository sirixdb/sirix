#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT=/var/tmp/sirix-bitemporal
M2="${ROOT}/m2"
CLASSPATH_FILE="${ROOT}/xtdb-2.1.0.classpath"

mkdir -p "${M2}"
mvn --batch-mode --no-transfer-progress \
  -Dmaven.repo.local="${M2}" \
  -f "${HERE}/pom.xml" \
  dependency:build-classpath \
  -Dmdep.outputFile="${CLASSPATH_FILE}"

sha256sum "${HERE}/pom.xml" "${CLASSPATH_FILE}"
printf 'XTDB 2.1.0 classpath: %s\n' "${CLASSPATH_FILE}"
