#!/usr/bin/env bash
# SessionStart setup for Claude Code on the web.
# Ensures the JDK 25 toolchain required by the Gradle build is present, since the
# base image ships an older JDK and the network policy may block auto-provisioning.
set -uo pipefail

JDK25_DIR=/usr/lib/jvm/java-25-openjdk-amd64

if [ ! -d "$JDK25_DIR" ]; then
  echo "[setup] Installing OpenJDK 25 (required by the Gradle toolchain)..."
  apt-get update -qq || true
  if apt-get install -y --no-install-recommends openjdk-25-jdk-headless; then
    echo "[setup] OpenJDK 25 installed at $JDK25_DIR"
  else
    echo "[setup] WARNING: could not install OpenJDK 25; './gradlew build' will need a JDK 25 toolchain." >&2
  fi
else
  echo "[setup] OpenJDK 25 already present at $JDK25_DIR"
fi
