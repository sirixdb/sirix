#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT=/var/tmp/sirix-bitemporal
CLASSPATH_FILE="${ROOT}/xtdb-2.1.0.classpath"
JAVA=/usr/lib/jvm/java-21-openjdk-amd64/bin/java

if [[ ! -f "${CLASSPATH_FILE}" ]]; then
  printf 'Missing %s; run %s/setup-xtdb.sh first.\n' "${CLASSPATH_FILE}" "${HERE}" >&2
  exit 2
fi
if [[ $# -lt 1 ]]; then
  printf 'Usage: %s load <tier> <events.jsonl> <database-dir> | run <database-dir> <output-dir> | probe <database-dir>\n' "$0" >&2
  exit 2
fi

mkdir -p "${ROOT}/tmp"
exec "${JAVA}" --enable-preview -Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g \
  -XX:-UsePerfData -Djava.io.tmpdir="${ROOT}/tmp" \
  -Dlogback.configurationFile="${HERE}/logback.xml" \
  -Dclojure.main.report=stderr --add-opens=java.base/java.nio=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -cp "$(cat "${CLASSPATH_FILE}")" clojure.main "${HERE}/xtdb.clj" "$@"
