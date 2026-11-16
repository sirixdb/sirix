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
- Python 3.12 and `python3 -m pip install -r tests/native/requirements.txt`.
- Rust 1.94.0 (`RUSTC`, default `rustc`), libclang 18 and
  `cargo install bindgen-cli --version 0.72.1 --locked` (`BINDGEN`, default `bindgen`).
- Set `LIBCLANG_PATH` if libclang is outside the loader's search path.

Run from the repository root, or pass the absolute script path from anywhere:

```sh
python3 tests/native/check_header.py --output-dir /tmp/sirix-header-check --mutations
```

The checker never invokes Gradle, Java or native-image. Installation/build resource
limits remain those of the host; use the required shared heavy-build wrapper in
managed laptop lanes. The GitHub workflow runs in its own hosted runner.

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
No current library exists to run those tests against. The 24-requirement runtime
suite plan and adversarial/differential cases are in the chapter's traceability
table; they are explicitly pending I2–I10 and XML parity work.
