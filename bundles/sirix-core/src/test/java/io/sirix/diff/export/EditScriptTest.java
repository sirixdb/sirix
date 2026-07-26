package io.sirix.diff.export;

import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.diff.DiffTuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for {@link EditScript} (issue #1074, item 3).
 * <p>
 * Before the fix, {@code hasNext()} used {@code index < size() - 1}, so the idiomatic
 * {@code while (hasNext()) next()} loop dropped the final change, and {@code add(change)}
 * returned {@code null} for changes with previously unseen node keys.
 */
public final class EditScriptTest {

  // Node keys above the Long autobox cache (|key| > 127) to exercise realistic keys.
  private static final long INSERTED_NODE_KEY = 1_000L;

  private static final long DELETED_NODE_KEY = 2_000L;

  @Test
  public void testIterationYieldsAllChanges() {
    final EditScript script = new EditScript();
    final DiffTuple firstChange = new DiffTuple(DiffType.INSERTED, INSERTED_NODE_KEY, 0, null);
    final DiffTuple secondChange = new DiffTuple(DiffType.DELETED, 0, DELETED_NODE_KEY, null);

    script.add(firstChange);
    script.add(secondChange);
    assertEquals(2, script.size());

    // Idiomatic iterator loop: pre-fix hasNext() returned false while the LAST element was
    // still pending, yielding only one tuple.
    final List<DiffTuple> consumed = new ArrayList<>(2);
    while (script.hasNext()) {
      consumed.add(script.next());
    }

    assertEquals("while (hasNext()) next() must yield every added change", 2, consumed.size());
    assertSame(firstChange, consumed.get(0));
    assertSame(secondChange, consumed.get(1));
    assertFalse(script.hasNext());
  }

  @Test
  public void testAddReturnsTheChangePassedIn() {
    final EditScript script = new EditScript();
    final DiffTuple firstChange = new DiffTuple(DiffType.INSERTED, INSERTED_NODE_KEY, 0, null);
    final DiffTuple secondChange = new DiffTuple(DiffType.DELETED, 0, DELETED_NODE_KEY, null);

    // Pre-fix add() returned null for changes with new node keys.
    assertSame("add() must return the change for a new node key", firstChange, script.add(firstChange));
    assertSame("add() must return the change for a new node key", secondChange, script.add(secondChange));

    // Adding a change for an already-tracked node key must return the passed change as well and
    // must not register a duplicate.
    final DiffTuple duplicateKeyChange = new DiffTuple(DiffType.UPDATED, INSERTED_NODE_KEY, 0, null);
    assertSame(duplicateKeyChange, script.add(duplicateKeyChange));
    assertEquals(2, script.size());

    assertTrue(script.containsNode(INSERTED_NODE_KEY));
    assertTrue(script.containsNode(DELETED_NODE_KEY));
    assertSame(firstChange, script.get(INSERTED_NODE_KEY));
    assertSame(secondChange, script.get(DELETED_NODE_KEY));
  }
}
