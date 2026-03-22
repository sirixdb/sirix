# Plan: Extract XPath from sirix-core to sirix-xpath-legacy

## Overview

Move the XPath 2.0 implementation from `sirix-core` (`io.sirix.service.xml.xpath.*`) into a new
`sirix-xpath-legacy` Gradle module under `bundles/`. The project now uses `sirix-query` (Brackit-based
XQuery) for query processing, making the old XPath engine legacy code.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Module name | `sirix-xpath-legacy` | Clearly signals deprecated status |
| AtomicValue & ItemListImpl | Move to legacy module | User preference; avoid circular deps via interface refactoring |
| UnionAxis | Keep in sirix-core | Generic set operation; sirix-query depends on it |
| Package names | Preserved as `io.sirix.service.xml.xpath.*` | Minimize churn for downstream consumers |
| SirixXPathException | Stays in sirix-core | Already in `io.sirix.exception`, not xpath-specific |
| ItemList\<T\> interface | Stays in sirix-core | Already in `io.sirix.api`, not xpath-specific |

---

## Dependency Graph (after extraction)

```
sirix-core  (no XPath code, no dependency on legacy)
    ↑
sirix-xpath-legacy  (depends on sirix-core)
    ↑
sirix-query  (currently only uses UnionAxis which stays in core → no new dep needed)
```

---

## Phase 1: Break Coupling — Refactor sirix-core APIs

Before we can move anything, we must eliminate sirix-core's compile-time dependency on XPath classes.

### Step 1.1: Move `UnionAxis` to `io.sirix.axis` package (stays in sirix-core)

**Files changed:**
- `bundles/sirix-core/src/main/java/io/sirix/service/xml/xpath/expr/UnionAxis.java`
  → Move to `bundles/sirix-core/src/main/java/io/sirix/axis/UnionAxis.java`
- Change `extends AbstractAxis` (xpath) → `extends io.sirix.axis.AbstractAxis` (the core one that
  already exists, or implement `Axis` directly)

**Verify:** `io.sirix.axis.AbstractAxis` must already exist. If it doesn't, `UnionAxis` can directly
implement `Axis` or we keep a minimal base class.

**Update imports in:**
- `sirix-core`: `ConcurrentUnionAxis.java`, `DupFilterAxis.java`, `ExpressionSingle.java`,
  `PipelineBuilder.java`
- `sirix-query`: `SirixTranslator.java` → change import to `io.sirix.axis.UnionAxis`

### Step 1.2: Replace `AtomicValue` usage in sirix-core public API

**Problem:** `XmlNodeReadOnlyTrx` (public interface) imports `AtomicValue`. Transaction impls create
`AtomicValue` instances.

**Solution:** Introduce a minimal `io.sirix.api.AtomicValue` interface (or use existing `ValueNode`)
in sirix-core. The concrete `AtomicValue` class moves to the legacy module and implements this
interface.

Detailed approach:
1. **Check what methods in `XmlNodeReadOnlyTrx` reference `AtomicValue`** — likely `getAtomicValue()`
   or similar. Change return type to `ValueNode` or a new slim interface.
2. **In `AbstractNodeReadOnlyTrx`** — change any `AtomicValue` references to the interface type.
3. **In `ForwardingXmlNodeReadOnlyTrx`** — same treatment.

### Step 1.3: Replace `ItemListImpl` usage in sirix-core

**Problem:** `XmlNodeReadOnlyTrxImpl` and `JsonNodeReadOnlyTrxImpl` instantiate `ItemListImpl`.

**Solution:** Create a simple `DefaultItemList<T extends Node>` in `io.sirix.api` (or `io.sirix.node`)
that provides the same ArrayList-based storage. This replaces `ItemListImpl` in core. The legacy
`ItemListImpl` can extend or delegate to it.

Alternative: Since `ItemListImpl` is tiny (< 30 lines of logic), we can just duplicate it as
`DefaultItemList` in sirix-core. The legacy module's `ItemListImpl` becomes a thin wrapper or is
just used directly by xpath code.

### Step 1.4: Remove `EXPathError` usage from sirix-core

**Files affected:**
- `io/sirix/axis/concurrent/ConcurrentUnionAxis.java` — uses `EXPathError.XPTY0004.getEncapsuledException()`
- `io/sirix/axis/concurrent/Util.java` — same

**Solution:** Replace with direct `SirixXPathException` throw (which stays in core), or a more
generic exception. The error message can be inlined.

### Step 1.5: Remove `XPathAxis` usage from `WikipediaImport`

**File:** `io/sirix/service/xml/shredder/WikipediaImport.java`

**Solution:** This is a legacy XML import utility. Either:
- Move `WikipediaImport` to the legacy module too, OR
- Remove the XPath dependency by refactoring the query to use standard axis navigation, OR
- Accept that `WikipediaImport` moves to the legacy module (it's rarely used)

**Recommended:** Move `WikipediaImport` to `sirix-xpath-legacy` since it's a utility that specifically
uses XPath queries for Wikipedia XML processing.

---

## Phase 2: Create the `sirix-xpath-legacy` Module

### Step 2.1: Create module directory structure

```
bundles/sirix-xpath-legacy/
├── build.gradle
└── src/
    ├── main/java/io/sirix/service/xml/xpath/
    │   ├── AbstractAxis.java          (the xpath one, NOT io.sirix.axis.AbstractAxis)
    │   ├── AtomicValue.java
    │   ├── DupState.java
    │   ├── EXPathError.java
    │   ├── ExpressionSingle.java
    │   ├── ItemListImpl.java
    │   ├── OrdState.java
    │   ├── PipelineBuilder.java
    │   ├── SequenceType.java
    │   ├── SingleType.java
    │   ├── XPathAxis.java
    │   ├── XPathError.java
    │   ├── comparators/
    │   │   ├── AbstractComparator.java
    │   │   ├── CompKind.java
    │   │   ├── GeneralComp.java
    │   │   ├── NodeComp.java
    │   │   └── ValueComp.java
    │   ├── expr/
    │   │   ├── AbstractExpression.java
    │   │   ├── AndExpr.java
    │   │   ├── CastableExpr.java
    │   │   ├── CastExpr.java
    │   │   ├── EveryExpr.java
    │   │   ├── ExceptAxis.java
    │   │   ├── IfAxis.java
    │   │   ├── IntersectAxis.java
    │   │   ├── InstanceOfExpr.java
    │   │   ├── IObserver.java
    │   │   ├── LiteralExpr.java
    │   │   ├── OrExpr.java
    │   │   ├── RangeAxis.java
    │   │   ├── SequenceAxis.java
    │   │   ├── SomeExpr.java
    │   │   ├── VarRefExpr.java
    │   │   └── VariableAxis.java
    │   ├── filter/
    │   │   ├── DocumentNodeAxis.java
    │   │   ├── DupFilterAxis.java
    │   │   ├── PosFilter.java
    │   │   ├── SchemaAttributeFilter.java
    │   │   └── SchemaElementFilter.java
    │   ├── functions/
    │   │   ├── AbstractFunction.java
    │   │   ├── FuncDef.java
    │   │   ├── Function.java
    │   │   ├── FNNot.java
    │   │   ├── FNPosition.java
    │   │   ├── FNString.java
    │   │   └── sequences/
    │   │       ├── FNBoolean.java
    │   │       ├── FNCount.java
    │   │       └── FNSum.java
    │   ├── operators/
    │   │   ├── AbstractObAxis.java
    │   │   ├── AddOpAxis.java
    │   │   ├── DivOpAxis.java
    │   │   ├── IDivOpAxis.java
    │   │   ├── ModOpAxis.java
    │   │   ├── MulOpAxis.java
    │   │   └── SubOpAxis.java
    │   ├── parser/
    │   │   ├── TokenType.java
    │   │   ├── VariableXPathToken.java
    │   │   ├── XPathParser.java
    │   │   ├── XPathScanner.java
    │   │   └── XPathToken.java
    │   └── types/
    │       └── Type.java
    └── test/java/io/sirix/service/xml/xpath/
        └── (all ~48 test files, mirroring current structure)
```

**NOTE:** `UnionAxis` is NOT in this list — it stays in sirix-core at `io.sirix.axis.UnionAxis`.

### Step 2.2: Create `build.gradle` for sirix-xpath-legacy

```groovy
dependencies {
    implementation project(':sirix-core')

    testImplementation project(path: ':sirix-core', configuration: 'testArtifacts')
    testImplementation testLibraries.junitJupiterApi
    testImplementation testLibraries.junitJupiterEngine
    testImplementation testLibraries.junitJupiterParams
}

description = 'Legacy XPath 2.0 engine for SirixDB (deprecated — use sirix-query instead).'

test {
    useJUnitPlatform()

    jvmArgs = [
        '--add-opens', 'java.base/sun.nio.ch=ALL-UNNAMED',
        '--add-opens', 'java.base/java.nio=ALL-UNNAMED',
        '--add-modules', 'jdk.incubator.vector',
        '--enable-native-access=ALL-UNNAMED',
    ]
}
```

### Step 2.3: Register in `settings.gradle`

```groovy
include(':sirix-xpath-legacy')
project(':sirix-xpath-legacy').projectDir = file('bundles/sirix-xpath-legacy')
```

---

## Phase 3: Move Source Files

### Step 3.1: Move main sources

Move the entire directory tree:
```
bundles/sirix-core/src/main/java/io/sirix/service/xml/xpath/
→ bundles/sirix-xpath-legacy/src/main/java/io/sirix/service/xml/xpath/
```

**Except:** `UnionAxis.java` — already relocated in Phase 1 to `io.sirix.axis.UnionAxis`.

### Step 3.2: Move test sources

Move the entire test directory tree:
```
bundles/sirix-core/src/test/java/io/sirix/service/xml/xpath/
→ bundles/sirix-xpath-legacy/src/test/java/io/sirix/service/xml/xpath/
```

### Step 3.3: Move WikipediaImport (optional)

If `WikipediaImport` is the only non-xpath class depending on XPath:
```
bundles/sirix-core/src/main/java/io/sirix/service/xml/shredder/WikipediaImport.java
→ bundles/sirix-xpath-legacy/src/main/java/io/sirix/service/xml/shredder/WikipediaImport.java
```

### Step 3.4: Move test files that import XPath from outside the xpath package

Check for tests in sirix-core (outside `xpath/`) that import XPath classes (e.g.,
`PredicateFilterAxisTest`, `ConcurrentAxisTest`). These may need to:
- Move to the legacy module, OR
- Have their XPath-specific test methods removed/refactored

---

## Phase 4: Update Internal References

### Step 4.1: Fix `DupFilterAxis` import of `UnionAxis`

In the legacy module's `DupFilterAxis.java`:
```java
// Old:
import io.sirix.service.xml.xpath.expr.UnionAxis;
// New:
import io.sirix.axis.UnionAxis;
```

### Step 4.2: Fix `ExpressionSingle` import of `UnionAxis`

Same change.

### Step 4.3: Fix `PipelineBuilder` import of `UnionAxis`

Same change.

### Step 4.4: Fix `AbstractAxis` (xpath version)

The xpath `AbstractAxis` is a large base class. Verify it doesn't duplicate `io.sirix.axis.AbstractAxis`.
If there IS a separate `io.sirix.axis.AbstractAxis` in sirix-core:
- Consider having the xpath `AbstractAxis` extend the core one, OR
- Keep them separate (the xpath one likely has XPath-specific state management like `DupState`, `OrdState`)

### Step 4.5: Update sirix-query

In `SirixTranslator.java`:
```java
// Old:
import io.sirix.service.xml.xpath.expr.UnionAxis;
// New:
import io.sirix.axis.UnionAxis;
```

No new module dependency needed.

---

## Phase 5: Handle AtomicValue / ItemListImpl Decoupling

### Step 5.1: Create a `DefaultItemList` in sirix-core

Create `io.sirix.node.DefaultItemList<T extends Node>` that provides basic ArrayList-based item
storage (same logic as `ItemListImpl` but generic over any `Node`).

```java
package io.sirix.node;

import io.sirix.api.ItemList;
import io.sirix.node.interfaces.Node;
import java.util.ArrayList;
import java.util.List;

public final class DefaultItemList<T extends Node> implements ItemList<T> {
    private final List<T> items = new ArrayList<>();

    @Override
    public int addItem(final T item) {
        final int key = items.size();
        item.setNodeKey(key);
        final int itemKey = (key + 2) * (-1);
        item.setNodeKey(itemKey);
        items.add(item);
        return itemKey;
    }

    @Override
    public T getItem(final long key) {
        int index = (int) key;
        if (index < 0) index = index * (-1);
        index = index - 2;
        if (index >= 0 && index < items.size()) {
            return items.get(index);
        }
        return null;
    }

    @Override
    public int size() {
        return items.size();
    }
}
```

### Step 5.2: Refactor `XmlNodeReadOnlyTrx` interface

Change any method that returns `AtomicValue` to return `ValueNode` or `Node` instead. This is the
public API change.

Example:
```java
// Before:
AtomicValue getAtomicValue();
// After:
ValueNode getAtomicValue();
```

### Step 5.3: Refactor transaction implementations

In `XmlNodeReadOnlyTrxImpl.java`, `JsonNodeReadOnlyTrxImpl.java`, `AbstractNodeReadOnlyTrx.java`,
`ForwardingXmlNodeReadOnlyTrx.java`:

- Replace `new ItemListImpl()` → `new DefaultItemList<>()`
- Replace `AtomicValue` type references → `ValueNode` or `Node`
- Remove imports of `io.sirix.service.xml.xpath.AtomicValue` and `io.sirix.service.xml.xpath.ItemListImpl`

### Step 5.4: Keep `ItemListImpl` in legacy as-is

The legacy `ItemListImpl` can remain exactly as it is in the xpath-legacy module. It uses
`AtomicValue` which is also in that module.

---

## Phase 6: Verify & Clean Up

### Step 6.1: Compile sirix-core alone
```bash
./gradlew :sirix-core:compileJava
```
Must succeed with zero references to `io.sirix.service.xml.xpath.*`.

### Step 6.2: Compile sirix-xpath-legacy
```bash
./gradlew :sirix-xpath-legacy:compileJava
```

### Step 6.3: Compile sirix-query
```bash
./gradlew :sirix-query:compileJava
```
Must succeed — only change is `UnionAxis` import path.

### Step 6.4: Run all tests
```bash
./gradlew :sirix-core:test
./gradlew :sirix-xpath-legacy:test
./gradlew :sirix-query:test
```

### Step 6.5: Add `@Deprecated` annotation to key XPath entry points

In the legacy module, annotate:
- `XPathAxis` class
- `XPathParser` class
- Package-level `package-info.java`

```java
/**
 * @deprecated Use sirix-query (Brackit-based XQuery) instead.
 */
@Deprecated(since = "0.x.0", forRemoval = true)
```

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| `AbstractAxis` (xpath) vs `AbstractAxis` (core) conflict | Medium | Verify both exist; xpath one may need renaming or to extend core one |
| Test helper classes shared between modules | Medium | sirix-core exports `testArtifacts` configuration already used by sirix-query |
| Non-active modules (sirix-gui, sirix-jax-rx, etc.) break | Low | They're not in `settings.gradle`; document that they need `sirix-xpath-legacy` dep if re-enabled |
| API breakage from AtomicValue→ValueNode change | Medium | This is intentional cleanup; downstream code using `AtomicValue` should migrate to sirix-query |

---

## File Count Summary

| Category | Count |
|----------|-------|
| Java source files moving to legacy | ~68 |
| Java test files moving to legacy | ~48 |
| Files modified in sirix-core (refactoring) | ~8 |
| Files modified in sirix-query | 1 |
| New files created | ~3 (build.gradle, DefaultItemList, package-info.java) |

---

## Execution Order

1. **Phase 1** — Break coupling (refactor in sirix-core, compile after each step)
2. **Phase 2** — Create module skeleton
3. **Phase 3** — Move files (git mv for history preservation)
4. **Phase 4** — Fix internal references
5. **Phase 5** — AtomicValue/ItemListImpl decoupling
6. **Phase 6** — Verify everything compiles and tests pass
