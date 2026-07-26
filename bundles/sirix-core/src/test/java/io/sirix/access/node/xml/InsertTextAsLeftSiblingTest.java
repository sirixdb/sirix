package io.sirix.access.node.xml;

import io.brackit.query.atomic.QNm;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for {@code XmlNodeTrxImpl#insertTextAsLeftSibling(String)} (issue #1058).
 * <p>
 * Before the fix, the anchor node's value was unconditionally appended to the text to insert:
 * an ELEMENT anchor took the {@code setValue} branch and threw a {@code SirixUsageException},
 * while a COMMENT (or PI) anchor silently rewrote its own value instead of inserting a new
 * text node. Only a TEXT anchor may merge values.
 */
public final class InsertTextAsLeftSiblingTest {

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testInsertTextAsLeftSiblingOfElement() {
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final long rootKey = wtx.insertElementAsFirstChild(new QNm("root")).getNodeKey();
      final long childKey = wtx.insertElementAsFirstChild(new QNm("child")).getNodeKey();

      // Cursor is on the "child" element which has the "root" element as parent.
      wtx.insertTextAsLeftSibling("txt");

      // The cursor must be located on the freshly inserted TEXT node.
      assertEquals(NodeKind.TEXT, wtx.getKind());
      assertEquals("txt", wtx.getValue());
      final long textKey = wtx.getNodeKey();

      // The text node is the LEFT sibling of the element anchor.
      assertTrue(wtx.moveTo(childKey));
      assertTrue(wtx.hasLeftSibling());
      assertTrue(wtx.moveToLeftSibling());
      assertEquals(textKey, wtx.getNodeKey());
      assertEquals(NodeKind.TEXT, wtx.getKind());
      assertEquals("txt", wtx.getValue());

      // The parent element now has two children: the text node and the element.
      assertTrue(wtx.moveToParent());
      assertEquals(rootKey, wtx.getNodeKey());
      assertEquals(2, wtx.getChildCount());

      wtx.commit();
    }
  }

  @Test
  public void testInsertTextAsLeftSiblingOfCommentDoesNotRewriteComment() {
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final long rootKey = wtx.insertElementAsFirstChild(new QNm("root")).getNodeKey();
      final long commentKey = wtx.insertCommentAsFirstChild("my comment").getNodeKey();

      // Cursor is on the comment node.
      assertEquals(NodeKind.COMMENT, wtx.getKind());
      wtx.insertTextAsLeftSibling("txt");

      // A new TEXT node must have been created (pre-fix the insert was dropped and the
      // comment's value was rewritten instead).
      assertEquals(NodeKind.TEXT, wtx.getKind());
      assertEquals("txt", wtx.getValue());
      final long textKey = wtx.getNodeKey();

      // The comment itself must be untouched.
      assertTrue(wtx.moveTo(commentKey));
      assertEquals(NodeKind.COMMENT, wtx.getKind());
      assertEquals("my comment", wtx.getValue());

      // And the text node is its left sibling.
      assertTrue(wtx.moveToLeftSibling());
      assertEquals(textKey, wtx.getNodeKey());
      assertEquals(NodeKind.TEXT, wtx.getKind());
      assertEquals("txt", wtx.getValue());

      assertTrue(wtx.moveToParent());
      assertEquals(rootKey, wtx.getNodeKey());
      assertEquals(2, wtx.getChildCount());

      wtx.commit();
    }
  }

  @Test
  public void testInsertTextAsLeftSiblingOfTextMergesValues() {
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final long rootKey = wtx.insertElementAsFirstChild(new QNm("root")).getNodeKey();
      final long textKey = wtx.insertTextAsFirstChild("world").getNodeKey();

      // Cursor is on the TEXT node: adjacent text must be merged, no new node created.
      wtx.insertTextAsLeftSibling("hello ");

      assertEquals(NodeKind.TEXT, wtx.getKind());
      assertEquals("hello world", wtx.getValue());
      assertEquals("the merge must reuse the anchor text node", textKey, wtx.getNodeKey());
      assertFalse(wtx.hasLeftSibling());
      assertFalse(wtx.hasRightSibling());

      assertTrue(wtx.moveToParent());
      assertEquals(rootKey, wtx.getNodeKey());
      assertEquals(1, wtx.getChildCount());

      wtx.commit();
    }
  }
}
