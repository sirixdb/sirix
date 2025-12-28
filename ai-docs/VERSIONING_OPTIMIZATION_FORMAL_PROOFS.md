# Formal Correctness Proofs for Versioning Optimizations

## Notation and Definitions

### Sets and Functions

Let:
- **S** = {0, 1, ..., 1023} be the set of slot indices (|S| = NDP_NODE_COUNT = 1024)
- **M** = MemorySegment type (nullable byte sequences)
- **P** = Page type = S → M (partial function from slots to memory segments)
- **F** = [P₀, P₁, ..., Pₖ₋₁] ordered list of page fragments (newest first)
- **dom(P)** = {i ∈ S : P(i) ≠ null} (populated slots)
- **|P|** = |dom(P)| (page size/cardinality)

### Fragment Ordering

Fragments in F are ordered by revision (newest = index 0):
```
rev(P₀) > rev(P₁) > ... > rev(Pₖ₋₁)
```

### Page Combining Semantics

**Definition 1** (Combine): The combine operation ⊕ produces a complete page from fragments:
```
combine(F) = P where:
  ∀i ∈ S: P(i) = Pⱼ(i) where j = min{t : Pₜ(i) ≠ null}, or null if no such t exists
```

Informally: For each slot, take the value from the newest fragment that has that slot.

---

## Theorem 1: Bitmap Iteration Equivalence

### Statement

Let B: S → {0, 1} be a bitmap where B(i) = 1 ⟺ P(i) ≠ null.

**Claim**: Iterating slots via bitmap produces the same result set as full iteration.

### Formal Proof

**Define**:
- FullIter(P) = {i ∈ S : P(i) ≠ null}
- BitmapIter(B) = {i ∈ S : B(i) = 1}

**To prove**: FullIter(P) = BitmapIter(B)

**Proof**:

(→) Let i ∈ FullIter(P). Then P(i) ≠ null.
By bitmap definition: B(i) = 1 ⟺ P(i) ≠ null.
Since P(i) ≠ null, we have B(i) = 1.
Therefore i ∈ BitmapIter(B). ✓

(←) Let i ∈ BitmapIter(B). Then B(i) = 1.
By bitmap definition: B(i) = 1 ⟺ P(i) ≠ null.
Since B(i) = 1, we have P(i) ≠ null.
Therefore i ∈ FullIter(P). ✓

By double inclusion: FullIter(P) = BitmapIter(B). ∎

### Invariant Maintenance

**Lemma 1.1** (Bitmap Invariant): After any sequence of setSlot operations, B(i) = 1 ⟺ P(i) ≠ null.

**Proof by induction on operation sequence**:

*Base case*: Initial state has B(i) = 0 ∀i and P(i) = null ∀i. Invariant holds.

*Inductive step*: Assume invariant holds before operation.

Case setSlot(data, i) where data ≠ null:
- P(i) ← data (now P(i) ≠ null)
- B(i) ← 1
- Invariant maintained: B(i) = 1 and P(i) ≠ null ✓

Case setSlot(null, i):
- P(i) ← null
- B(i) ← 0
- Invariant maintained: B(i) = 0 and P(i) = null ✓

Other slots unchanged. Invariant preserved. ∎

---

## Theorem 2: Lazy View Semantic Equivalence

### Statement

**Claim**: CombinedPageView(F) ≡ combine(F) for all read operations.

Where ≡ denotes behavioral equivalence:
```
V ≡ P ⟺ ∀i ∈ S: V.getSlot(i) = P(i)
```

### Formal Proof

Let V = CombinedPageView(F) with fragments F = [P₀, ..., Pₖ₋₁].
Let P = combine(F).

**To prove**: ∀i ∈ S: V.getSlot(i) = P(i)

**Proof**:

Fix arbitrary i ∈ S.

**Case 1**: No fragment has slot i populated.
- ∀t ∈ [0, k): Pₜ(i) = null
- By Definition 1: P(i) = null
- V.getSlot(i) iterates all fragments, finds all null, returns null
- V.getSlot(i) = null = P(i) ✓

**Case 2**: At least one fragment has slot i populated.
- Let j = min{t : Pₜ(i) ≠ null}
- By Definition 1: P(i) = Pⱼ(i)
- V.getSlot(i) iterates P₀, P₁, ..., Pⱼ₋₁, Pⱼ
  - For t < j: Pₜ(i) = null (by minimality of j), continue
  - At t = j: Pⱼ(i) ≠ null, return Pⱼ(i)
- V.getSlot(i) = Pⱼ(i) = P(i) ✓

In both cases V.getSlot(i) = P(i). Since i was arbitrary: V ≡ P. ∎

### Temporal Safety

**Lemma 2.1** (Fragment Lifetime): During V's lifetime, all fragments Pᵢ remain valid.

**Proof**:
1. V acquires guards on all fragments at construction: ∀i: Pᵢ.guardCount++
2. guardCount > 0 prevents cache eviction (by eviction policy)
3. V releases guards at close: ∀i: Pᵢ.guardCount--
4. Therefore fragments valid during V's entire lifetime ∎

---

## Theorem 3: Bulk Copy Correctness

### Statement

Let slots s₁, s₂, ..., sₙ have contiguous offsets in source memory:
```
offset(sᵢ₊₁) = offset(sᵢ) + length(sᵢ)  for i ∈ [1, n-1]
```

**Claim**: Single bulk copy ≡ sequential per-slot copies.

### Formal Proof

**Define operations**:

PerSlotCopy():
```
dst_offset ← 0
for i ∈ 1..n:
    copy(src[offset(sᵢ)..offset(sᵢ)+length(sᵢ)] → dst[dst_offset..dst_offset+length(sᵢ)])
    dst_offset ← dst_offset + length(sᵢ)
```

BulkCopy():
```
total ← Σᵢ length(sᵢ)
copy(src[offset(s₁)..offset(s₁)+total] → dst[0..total])
```

**To prove**: After both operations, dst contains identical bytes.

**Proof**:

By contiguity assumption:
```
offset(sᵢ) = offset(s₁) + Σⱼ₌₁ⁱ⁻¹ length(sⱼ)
```

Consider byte position p in dst after PerSlotCopy().

Let p ∈ [dst_offset_before_i, dst_offset_after_i) for some i.
Then p = dst_offset_before_i + δ where 0 ≤ δ < length(sᵢ).

dst[p] was written from src[offset(sᵢ) + δ].

By contiguity:
```
offset(sᵢ) + δ = offset(s₁) + Σⱼ₌₁ⁱ⁻¹ length(sⱼ) + δ
               = offset(s₁) + dst_offset_before_i + δ
               = offset(s₁) + p
```

Therefore dst[p] = src[offset(s₁) + p].

After BulkCopy:
- dst[p] = src[offset(s₁) + p] for p ∈ [0, total)

This is identical to PerSlotCopy. ∎

---

## Theorem 4: SLIDING_SNAPSHOT Bitmap Equivalence

### Definitions

- **Window** W = {P₀, ..., P_{R-2}} (in-window fragments)
- **OutWindow** O = P_{R-1} if |F| = R, else ∅
- **InWindowSlots** = ⋃_{P ∈ W} dom(P)

### Statement

**Claim**: Using bitmap to track InWindowSlots produces identical modifyingPage as using tempPage.

### Formal Proof

**Original algorithm** (with tempPage T):
```
T ← newEmptyPage()
modifyingPage M ← newEmptyPage()

for P in W:
    for i in dom(P):
        T(i) ← P(i)

for i in dom(O):
    if T(i) = null:
        M(i) ← O(i)
```

**Optimized algorithm** (with bitmap B):
```
B ← [0]^S  (all zeros)
modifyingPage M ← newEmptyPage()

for P in W:
    for i in dom(P):
        B(i) ← 1

for i in dom(O):
    if B(i) = 0:
        M(i) ← O(i)
```

**To prove**: M_original = M_optimized

**Key invariant**: After processing window W:
```
B(i) = 1 ⟺ i ∈ InWindowSlots ⟺ T(i) ≠ null
```

**Proof of invariant**:

(B(i) = 1 ⟺ i ∈ InWindowSlots):
- B(i) = 1 iff ∃P ∈ W: i ∈ dom(P) (by construction)
- = i ∈ ⋃_{P ∈ W} dom(P) = InWindowSlots ✓

(T(i) ≠ null ⟺ i ∈ InWindowSlots):
- T is updated for all i ∈ dom(P) for each P ∈ W
- T(i) ≠ null iff some P ∈ W had i ∈ dom(P)
- = i ∈ InWindowSlots ✓

**Main proof**:

For any i ∈ dom(O):

In original: M(i) ← O(i) iff T(i) = null
In optimized: M(i) ← O(i) iff B(i) = 0

By invariant: T(i) = null ⟺ i ∉ InWindowSlots ⟺ B(i) = 0

Therefore the condition is equivalent, and M_original(i) = M_optimized(i) for all i. ∎

---

## Theorem 5: Master Correctness Theorem

### Statement

For versioning type V ∈ {DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT}:
```
∀F, R, T: V.combineOptimized(F, R, T) ≡ V.combineOriginal(F, R, T)
```

### Proof

The optimized implementations use only:
1. Bitmap iteration (Theorem 1) - produces same iteration set
2. Lazy views (Theorem 2) - produces same slot values
3. Bulk copies (Theorem 3) - produces same memory contents
4. Bitmap tracking (Theorem 4) - produces same conditions

Each optimization is proven equivalent to original behavior.
Composition of equivalent operations preserves equivalence.

Therefore optimized ≡ original. ∎

---

## Complexity Analysis

### Slot Iteration Complexity

| Method | Time Complexity | Space Complexity |
|--------|----------------|------------------|
| Full iteration | O(1024) | O(1) |
| Bitmap iteration | O(k + 16) | O(1) |

Where k = |dom(P)| (populated slots)

**Improvement**: 1024 / (k + 16), typically 10-100x for k ∈ [10, 100]

### Memory Allocation

| Approach | Allocation | Notes |
|----------|-----------|-------|
| Original combine | 64KB | Always allocates new page |
| Lazy view | ~8KB | References only |
| SLIDING_SNAPSHOT temp | 64KB → 128B | Bitmap vs page |

### Total Combining Complexity

| Algorithm | Original | Optimized |
|-----------|----------|-----------|
| DIFFERENTIAL | O(2 × 1024) | O(2 × k) |
| INCREMENTAL | O(R × 1024) | O(R × k) |
| SLIDING_SNAPSHOT | O(R × 1024) + 64KB temp | O(R × k) + 128B |

---

## Appendix: Proof Verification Tests

```java
/**
 * Verifies Theorem 1: Bitmap iteration equivalence
 */
@Test
void verifyTheorem1_BitmapIterationEquivalence() {
    for (int trial = 0; trial < 1000; trial++) {
        KeyValueLeafPage page = createRandomPage();
        
        Set<Integer> fullIterResult = new HashSet<>();
        for (int i = 0; i < 1024; i++) {
            if (page.getSlot(i) != null) fullIterResult.add(i);
        }
        
        Set<Integer> bitmapResult = page.populatedSlots().boxed()
            .collect(Collectors.toSet());
        
        assertEquals(fullIterResult, bitmapResult, 
            "Theorem 1 violated: bitmap ≠ full iteration");
    }
}

/**
 * Verifies Theorem 2: Lazy view equivalence
 */
@Test  
void verifyTheorem2_LazyViewEquivalence() {
    for (int fragmentCount = 1; fragmentCount <= 5; fragmentCount++) {
        List<KeyValueLeafPage> fragments = createRandomFragments(fragmentCount);
        
        KeyValueLeafPage eagerCombined = combineEager(fragments);
        CombinedPageView lazyView = new CombinedPageView(fragments);
        
        for (int i = 0; i < 1024; i++) {
            assertEquals(eagerCombined.getSlot(i), lazyView.getSlot(i),
                "Theorem 2 violated at slot " + i);
        }
    }
}

/**
 * Verifies Theorem 4: SLIDING_SNAPSHOT bitmap equivalence
 */
@Test
void verifyTheorem4_SlidingSnapshotBitmapEquivalence() {
    for (int trial = 0; trial < 100; trial++) {
        List<KeyValueLeafPage> fragments = createRandomFragments(3);
        PageReference ref = new PageReference();
        TransactionIntentLog log = new TransactionIntentLog();
        
        PageContainer original = SLIDING_SNAPSHOT_ORIGINAL
            .combineRecordPagesForModification(fragments, 3, trx, ref, log);
        
        PageContainer optimized = SLIDING_SNAPSHOT_OPTIMIZED
            .combineRecordPagesForModification(fragments, 3, trx, ref, log);
        
        assertPagesEqual(original.getModified(), optimized.getModified(),
            "Theorem 4 violated: modifyingPage differs");
    }
}
```

---

*Mathematical notation follows standard set theory and algorithm analysis conventions.*  
*∎ denotes end of proof*  
*⟺ denotes logical equivalence*  
*∀ denotes universal quantification*  
*∃ denotes existential quantification*

