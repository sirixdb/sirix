# SirixDB developer tasks. Run `just` (or `just --list`) to see them.
#
# Requires JDK 25. Gradle's toolchain auto-detects it, so the Gradle daemon
# itself can keep running on whatever JDK your JAVA_HOME points at — you do
# not need to switch your default JDK. Verify detection with `just toolchains`.

# List available recipes.
default:
    @just --list

# Show every JDK Gradle detected (confirm a "Language Version: 25" entry exists).
toolchains:
    ./gradlew -q javaToolchains

# Compile all modules without running tests.
build:
    ./gradlew build -x test

# Run the full test suite.
test:
    ./gradlew test

# Apply Spotless formatting; run before committing.
format:
    ./gradlew spotlessApply

# Run one JMH benchmark; `just bench JsonWritePathBenchmark compressionPipeline=NONE`.
bench name params="":
    # `name` is a class-name pattern; `params` overrides JMH @Param values
    # (pipe-separated for multiple, e.g. storageType=FILE_CHANNEL|MEMORY_MAPPED).
    ./gradlew :sirix-benchmarks:jmh -Pjmh.includes={{name}} -Pjmh.benchmarkParameters="{{params}}"

# Run the entire JMH benchmark suite (slow).
bench-all:
    ./gradlew :sirix-benchmarks:jmh

# Run the large-dataset scale benchmark; `just scale "1000000 true 3"`.
scale args="1000000 true 3":
    # args = "records shred? iterations". Task uses -Xms8g -Xmx24g; shrink on a laptop.
    ./gradlew :sirix-benchmarks:runScale -Pscale.args="{{args}}"
