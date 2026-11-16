# NativeHeaderContract

This is the executable declaration check for
[NATIVE-404/405/407](../../spec/NATIVE-embedded-c-api.md). It is **not** a libsirix
runtime test or evidence that any native platform package exists.

The independent oracle is `abi-layout.json`: every public field's type and
byte offset, struct size/alignment, numeric constant, function signature and
initializer value from ABI 1.0. `symbols.txt` is the complete required symbol
inventory for the later library export check. Changing either is a contract
change requiring specification review, not a way to silence a failed test.

Prerequisites on a 64-bit little-endian host:

- C11 and C++17 compilers (`CC`/`CXX`, default `cc`/`c++`).
- Python 3.12 and `python3 -m pip install --only-binary :all: --require-hashes -r tests/native/requirements.txt`.
- Rust 1.94.0 (`RUSTC`, default `rustc`), libclang 18 and
  `cargo install bindgen-cli --version 0.72.1 --locked` (`BINDGEN`, default `bindgen`).
- Set `LIBCLANG_PATH` if libclang is outside the loader's search path.

The dependency lock includes the published SHA-256 hashes of Python 3.12 cffi
wheels and the platform-independent pycparser wheel. Keep both versions and
hashes pinned when updating it; installation rejects source distributions and
unrecognized artifact bytes.

Run from the repository root, or pass the absolute script path from anywhere:

```sh
python3 tests/native/check_header.py --output-dir /tmp/sirix-header-check --mutations
```

The checker never invokes Gradle, Java or native-image. The GitHub workflow runs
in its own hosted runner.

The checker executes these acceptance tests:

1. C11 and C++17 compile all public types, constants, signatures and aggregate
   initializers with warnings as errors; executable clients check initializer
   fields, including nested headers, reserved fields and arrays. Every field has
   a type assertion, not just a matching `sizeof`.
2. C definitions of all declared functions link to C++ references. These generated
   no-op **test stubs** check language linkage only and are not library code.
3. The real C preprocessor produces cffi input with `-DSIRIX_FFI`; cffi parses it
   and checks its layout and function declaration model against the manifest.
   The pinned cffi parser model is used only for function-type comparison; no
   function name is misinterpreted as a typedef. SIRIX_FFI removes visibility and
   calling-convention decorations for parsing; production C clients do not set it.
4. Rust bindgen consumes the original header, then rustc compiles and runs both
   generated checks and independent manifest assertions for fields and constants.
5. Eight scratch header mutations change status numbers, ABI version, status
   width, handle signedness, field order, worker default, capability mask and
   C linkage. Each must fail a compiler, executable assertion or linker check.
   The checker refuses a stale mutation fixture rather than counting it killed.

Each invocation prints its unique evidence directory under `--output-dir` and
retains generated C/C++/Rust sources, binaries, preprocessed declarations and
mutation rejection diagnostics. CI uploads this directory. Optimized Python
(`-O`) is rejected so assertions cannot silently disappear. No artifacts are
written beside the checked-in header or tests.

The CI job currently runs Linux x64 header consumers. It does not certify macOS
arm64 or Windows x64 packages. NATIVE-501 still requires native execution, symbol
exports and old-client/new-library loading on every platform before it is claimed.
No public ABI library exists to run those release tests against. The 24-requirement runtime
suite plan and adversarial/differential cases are in the chapter's traceability
table; they are explicitly pending I2–I10 and XML parity work.

## CoreNativeMetadataSmoke / NativeNoJvmSmoke (I2)

The separate [shared-image specification](SMOKE.md) maps this regression to
NATIVE-101/102/201/205/301/406. It uses test-only entrypoints, not `sirix.h` or a
public ABI implementation. The existing CLI/REST build targets are unaffected.

On Linux x64 with GraalVM 25, a C compiler, zlib development headers, `strace`,
`readelf`, Python 3 and `timeout`:

```sh
GRAALVM_HOME=/path/to/graalvm tests/native/run-smoke.sh all /tmp/sirix-native-smoke
```

`core` and `query` select individual profiles. The core profile uses only core's
runtime classpath; query/REST metadata is forbidden there. Each image runs twice
in fresh C processes. Runtime execution clears the environment, sets
`PATH=/nonexistent`, checks `/proc/self/maps` and ELF dependencies for libjvm,
and rejects every executable launch after the C harness starts. File-open traces
also reject attempted libjvm loading during the flow. Build-time
GraalVM remains installed on the host; clean-package installation and all
four language bindings are later release conformance, not claimed by this test.

Without GraalVM or outside Linux x64 the script prints `SKIP` and succeeds;
a configured toolchain's build/runtime failures are fatal. Build logs, generated
headers, classpaths, libraries, thread stacks and exec traces stay in the supplied
output directory. Use a fresh output directory for each evidence run. The 60s
runtime watchdog detects non-return; it is not a performance budget.

The native builder uses two threads and at most 4 GiB heap; Gradle uses two workers
and 2 GiB. `SIRIX_HEAVY_RUNNER=/path/to/heavy.sh` wraps every JVM/build invocation
for shared developer machines. The local free-space floor defaults to 60 GiB;
`SIRIX_SMOKE_DISK_FLOOR_KIB` may select a runner-appropriate floor in CI.

For explicit metadata mutation evidence, run each constructor omission in a
fresh output directory:

```sh
SIRIX_SMOKE_OMIT_CONSTRUCTOR=PSMS tests/native/run-smoke.sh core /tmp/sirix-missing-psms
SIRIX_SMOKE_OMIT_CONSTRUCTOR=PSMW tests/native/run-smoke.sh core /tmp/sirix-missing-psmw
```

These runs copy core resources outside the checkout and remove just the named
constructor. They succeed only when native C execution rejects that constructor
with `MissingReflectionRegistrationError` and the failure exit code; an unrelated
build failure, crash or teardown timeout does not count as a killed mutation.

Set `SIRIX_SMOKE_TEST_TEARDOWN_MUTATION=1` to additionally compile a C caller
that omits the snapshot-pool shutdown hook against the same library. It must
complete data work with 42, then hit the watchdog with the snapshot append
workers explicitly reported as teardown laggards. This diagnostic failure is
counted only for the deliberately mutated caller; the normal caller must exit
successfully first.
