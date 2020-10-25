package org.sirix.node;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.xml.AttributeNode;

import java.io.*;

import static org.junit.Assert.*;

public final class RevisionReferencesNodeTest {

  private PageTrx pageTrx;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.openResourceManager(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    final var node = new RevisionReferencesNode(1, new int[] { 3, 7, 8 } );
    assertArrayEquals(new int[] { 3, 7, 8 }, node.getRevisions());
    node.addRevision(13);
    checkNode(node);

    // Serialize and deserialize node.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    node.getKind().serialize(new DataOutputStream(out), node, pageTrx);
    final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    final RevisionReferencesNode node2 =
        (RevisionReferencesNode) node.getKind().deserialize(new DataInputStream(in), node.getNodeKey(), null, pageTrx);
    checkNode(node2);
  }

  private void checkNode(RevisionReferencesNode node) {
    assertEquals(NodeKind.REVISION_REFERENCES_NODE, node.getKind());
    assertEquals(1, node.getNodeKey());
    assertArrayEquals(new int[] { 3, 7, 8, 13 }, node.getRevisions());

    final var otherNode = new RevisionReferencesNode(1, new int[] { 3, 7, 8, 13});
    assertTrue(node.equals(otherNode));
    assertTrue(otherNode.equals(node));

    final var otherUnequalNodeDueToValue = new RevisionReferencesNode(1, new int[] { 3, 7, 8 });
    assertFalse(node.equals(otherUnequalNodeDueToValue));
    assertFalse(otherUnequalNodeDueToValue.equals(node));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetRevisionMethodOperationNotSupportedException() {
    final var node = new RevisionReferencesNode(1, new int[] {});

    node.getRevision();
  }
}
