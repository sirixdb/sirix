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

## Git/PR Guidelines

- Do not mention Claude in commits or PRs - no Co-Authored-By lines or references to Claude/AI
