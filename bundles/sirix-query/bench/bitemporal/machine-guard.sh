#!/usr/bin/env bash
set -euo pipefail

ROOT=/var/tmp/sirix-bitemporal
MIN_FREE_BYTES=$((20 * 1024 * 1024 * 1024))

mkdir -p "${ROOT}"

filesystem="$(findmnt -T "${ROOT}" -n -o FSTYPE)"
if [[ "${filesystem}" != ext4 ]]; then
  printf 'SH1 requires ext4 at %s; found %s\n' "${ROOT}" "${filesystem}" >&2
  exit 1
fi

free_bytes="$(df -B1 --output=avail "${ROOT}" | tail -n 1 | tr -d ' ')"
if (( free_bytes < MIN_FREE_BYTES )); then
  printf 'SH1 requires at least %d free bytes; found %d\n' "${MIN_FREE_BYTES}" "${free_bytes}" >&2
  exit 1
fi

containers="$(docker ps --format '{{.ID}} {{.Image}} {{.Status}}')"
if [[ -n "${containers}" ]]; then
  printf 'Running containers make the machine gate fail:\n%s\n' "${containers}" >&2
  exit 1
fi

heavy="$(ps -eo pid=,pcpu=,comm=,args= | awk '$2 >= 50 && ($3 == "java" || $3 == "docker" || $3 == "podman")')"
if [[ -n "${heavy}" ]]; then
  printf 'Concurrent heavy JVM/container processes make the machine gate fail:\n%s\n' "${heavy}" >&2
  exit 1
fi

printf 'SH1_MACHINE_GATE filesystem=%s free_bytes=%s\n' "${filesystem}" "${free_bytes}"
ps -eo pid,etimes,pcpu,pmem,rss,comm,args | sed -n '1p;/java/p;/docker/p;/podman/p'
