#!/usr/bin/env bash
#
# download-data.sh <tier> <dir>
#
# Fetch the JSONBench Bluesky corpus from the ClickHouse public dataset bucket.
#
#   tier   1m | 10m | 100m | 1000m   (files 0001..0001 / 0010 / 0100 / 1000)
#   dir    where the .json.gz files go
#
# Sizes: each file is ~135 MB gzipped and ~480 MB raw, one million events.
#   1m    135 MB      10m   1.4 GB      100m  13 GB      1000m  135 GB
#
# Idempotent, and idempotent *offline*: a file that is already present is
# verified with `gzip -t` and skipped without touching the network. Only a
# missing or truncated file is (re)fetched, with `wget --continue`, so an
# interrupted 13 GB download resumes instead of restarting. SKIP_VERIFY=1 skips
# the integrity pass (it reads every byte -- ~60 s for the 100m tier);
# FORCE=1 re-fetches everything.
#
# The bucket is plain HTTPS with no credentials. If your environment filters
# egress, allow `clickhouse-public-datasets.s3.amazonaws.com`.
set -u

. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../common" && pwd)/bench-common.sh"

readonly BUCKET_URL="https://clickhouse-public-datasets.s3.amazonaws.com/bluesky"

usage() {
  cat >&2 <<'USAGE'
usage: download-data.sh <tier> <dir>

  tier   1m | 10m | 100m | 1000m
  dir    destination directory for file_NNNN.json.gz

env: SKIP_VERIFY=1  do not gzip -t existing files
     FORCE=1        re-download even if present and intact
USAGE
  exit 2
}

[ "$#" -eq 2 ] || usage

TIER="$1"
DEST="$2"

case "${TIER}" in
  1m)    FILES=1 ;;
  10m)   FILES=10 ;;
  100m)  FILES=100 ;;
  1000m) FILES=1000 ;;
  *)     die "unknown tier '${TIER}' (expected 1m, 10m, 100m or 1000m)" ;;
esac

need_cmd wget "the downloader; the kit uses --continue so partial files resume"
need_cmd gzip "used to verify that an already-present file is complete"

mkdir -p "${DEST}" || die "cannot create destination directory: ${DEST}"

log "tier ${TIER}: ${FILES} file(s) -> ${DEST}"

have=0
fetched=0
for i in $(seq 1 "${FILES}"); do
  name="$(printf 'file_%04d.json.gz' "${i}")"
  path="${DEST}/${name}"

  if [ "${FORCE:-0}" != "1" ] && [ -s "${path}" ]; then
    if [ "${SKIP_VERIFY:-0}" = "1" ]; then
      have=$((have + 1))
      continue
    fi
    if gzip -t "${path}" 2>/dev/null; then
      have=$((have + 1))
      continue
    fi
    warn "${name} is present but not a complete gzip stream -- resuming the download"
  fi

  log "fetching ${name}"
  wget --continue --timestamping --progress=dot:giga \
       --directory-prefix "${DEST}" "${BUCKET_URL}/${name}" \
    || die "download of ${name} failed (network, or the bucket is unreachable from here)"

  gzip -t "${path}" 2>/dev/null \
    || die "${name} downloaded but is not a valid gzip stream -- delete it and retry"
  fetched=$((fetched + 1))
done

total_bytes=$(du -sb "${DEST}" 2>/dev/null | cut -f1)
log "tier ${TIER} ready: ${have} file(s) already present and verified, ${fetched} fetched"
log "corpus directory: ${DEST} ($(numfmt --to=iec-i --suffix=B "${total_bytes:-0}" 2>/dev/null || echo "${total_bytes:-?} bytes"))"
log "next: clean-corpus.py '${DEST}' ${TIER}"
