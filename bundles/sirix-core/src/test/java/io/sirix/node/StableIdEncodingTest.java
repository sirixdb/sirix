package io.sirix.node;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pins the stable on-disk id contracts: every persisted enum id must round-trip through its
 * lookup, unknown ids must fail fast with a descriptive error (never NPE/AIOOBE or a silently
 * wrong constant), and the id assignments themselves must not drift — a change here is an
 * on-disk format break.
 */
public final class StableIdEncodingTest {

  @Test
  public void everyNodeKindIdRoundTrips() {
    for (final NodeKind kind : NodeKind.values()) {
      assertSame(kind, NodeKind.getKind(kind.getId()));
    }
  }

  @Test
  public void unknownNodeKindIdFailsFast() {
    final IllegalStateException inRange =
        assertThrows(IllegalStateException.class, () -> NodeKind.getKind((byte) 100));
    assertTrue(inRange.getMessage().contains("100"));
    // Ids >= 128 arrive as negative bytes; before the guard this was an AIOOBE.
    assertThrows(IllegalStateException.class, () -> NodeKind.getKind((byte) 0xFE));
  }

  @Test
  public void hotIndirectNodeTypeIdsAreStable() {
    assertEquals(0, HOTIndirectPage.NodeType.SPAN_NODE.getID());
    assertEquals(1, HOTIndirectPage.NodeType.MULTI_NODE.getID());
    for (final HOTIndirectPage.NodeType type : HOTIndirectPage.NodeType.values()) {
      assertSame(type, HOTIndirectPage.NodeType.fromID(type.getID()));
    }
    assertThrows(IllegalStateException.class, () -> HOTIndirectPage.NodeType.fromID((byte) 2));
    assertThrows(IllegalStateException.class, () -> HOTIndirectPage.NodeType.fromID((byte) -1));
  }

  @Test
  public void hotIndirectLayoutTypeIdsAreStable() {
    assertEquals(0, HOTIndirectPage.LayoutType.SINGLE_MASK.getID());
    assertEquals(1, HOTIndirectPage.LayoutType.MULTI_MASK.getID());
    for (final HOTIndirectPage.LayoutType type : HOTIndirectPage.LayoutType.values()) {
      assertSame(type, HOTIndirectPage.LayoutType.fromID(type.getID()));
    }
    assertThrows(IllegalStateException.class, () -> HOTIndirectPage.LayoutType.fromID((byte) 2));
    assertThrows(IllegalStateException.class, () -> HOTIndirectPage.LayoutType.fromID((byte) -1));
  }

  @Test
  public void everyIndexTypeIdRoundTrips() {
    for (final IndexType type : IndexType.values()) {
      assertSame(type, IndexType.getType(type.getID()));
    }
    assertThrows(IllegalStateException.class, () -> IndexType.getType((byte) 120));
    assertThrows(IllegalStateException.class, () -> IndexType.getType((byte) -1));
  }
}
