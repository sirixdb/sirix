/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.utils;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe {@link java.util.Set} that holds its elements WEAKLY and compares them by IDENTITY.
 *
 * <p>Registries used for leak detection need both properties at once, and the JDK offers neither
 * combination: {@code IdentityHashMap} gives identity but holds strong references — so registering an
 * object makes it immortal, and any detector that fires when an object is collected can never fire.
 * {@code WeakHashMap} gives weakness but compares with {@code equals}/{@code hashCode}, so two
 * distinct instances that compare equal collapse into one entry, which for pages means an instance
 * silently vanishing from the census.</p>
 *
 * <p>Elements disappear from this set once nothing else references them, so what remains is exactly
 * the set of objects still reachable somewhere — which is the useful definition of "still live" for a
 * leak report, and leaves any GC-time detector free to observe the rest.</p>
 *
 * <p>Iteration is over a snapshot of the currently reachable elements, so it never sees a cleared
 * entry and never throws {@code ConcurrentModificationException}.</p>
 *
 * @param <T> the element type
 */
public final class WeakIdentitySet<T> extends AbstractSet<T> {

  private final ConcurrentHashMap<WeakKey, Boolean> map = new ConcurrentHashMap<>();
  private final ReferenceQueue<Object> cleared = new ReferenceQueue<>();

  /**
   * A weak reference keyed by the referent's identity.
   *
   * <p>The identity hash is captured at construction because it must stay usable AFTER the referent
   * has been collected — that is the only way an enqueued, already-cleared key can still find its own
   * map entry to remove.</p>
   */
  private static final class WeakKey extends WeakReference<Object> {

    private final int hash;

    WeakKey(final Object referent, final ReferenceQueue<Object> queue) {
      super(referent, queue);
      this.hash = System.identityHashCode(referent);
    }

    /** Lookup-only key: never enqueued, so it needs no queue. */
    WeakKey(final Object referent) {
      super(referent);
      this.hash = System.identityHashCode(referent);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object other) {
      // Self-identity FIRST: a cleared key has no referent left to compare, and expunging relies on
      // the enqueued key matching the very entry it came from.
      if (this == other) {
        return true;
      }
      if (!(other instanceof WeakKey key)) {
        return false;
      }
      final Object mine = get();
      return mine != null && mine == key.get();
    }
  }

  /** Drop entries whose referent has been collected. Cheap; runs on every mutating operation. */
  private void expunge() {
    for (Reference<?> reference = cleared.poll(); reference != null; reference = cleared.poll()) {
      map.remove(reference);
    }
  }

  @Override
  public boolean add(final T element) {
    expunge();
    return map.put(new WeakKey(element, cleared), Boolean.TRUE) == null;
  }

  @Override
  public boolean remove(final Object element) {
    expunge();
    return map.remove(new WeakKey(element)) != null;
  }

  @Override
  public boolean contains(final Object element) {
    expunge();
    return map.containsKey(new WeakKey(element));
  }

  @Override
  public int size() {
    expunge();
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public void clear() {
    expunge();
    map.clear();
  }

  @Override
  public Iterator<T> iterator() {
    return snapshot().iterator();
  }

  /** The currently reachable elements. Strong references, so callers can work with them safely. */
  @SuppressWarnings("unchecked")
  private List<T> snapshot() {
    expunge();
    final List<T> alive = new ArrayList<>(map.size());
    for (final WeakKey key : map.keySet()) {
      final Object element = key.get();
      if (element != null) {
        alive.add((T) element);
      }
    }
    return alive;
  }
}
