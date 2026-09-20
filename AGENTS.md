## Code Style

- **Always use explicit imports** - no star imports (`import foo.*`), no inline fully-qualified class names. Every type must be imported at the top of the file.
- Produce production-ready code, use best practices, create tests where appropriate, check input parameters to functions/methods, use common software engineering patterns where appropriate

## Performance Requirements

- Make sure the resulting code is correct and as we're a DBS we need **extreme performant code**
- Write **HFT (High-Frequency Trading) style high-performance code**:
  - Minimize object allocations in hot paths - reuse objects where possible
  - Prefer primitive types over boxed types (int over Integer, long over Long)
  - Avoid autoboxing/unboxing in performance-critical code
  - Use efficient data structures (primitive collections like fastutil, eclipse-collections, or RoaringBitmaps where appropriate)
  - Minimize garbage collection pressure
  - Use `final` for fields and variables where possible
  - Avoid unnecessary synchronization - prefer lock-free data structures when thread-safety is needed
  - Consider cache locality - keep related data together
  - Avoid virtual method calls in tight loops where possible
  - Pre-size collections when the size is known
  - Use StringBuilder for string concatenation in loops
- A change to a read, load or query path must keep the **work-budget tests** green; the *Work budgets* block of `docs/VERIFICATION.md` runs all of them. A broken budget is a regression until shown otherwise: never widen a bound to get a green build, and never add a wall-clock assertion. Rules and how to add one: `bundles/sirix-core/src/test/java/io/sirix/budget/README.md`.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
