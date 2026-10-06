package io.sirix.query.compiler.translator;

import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.trx.node.xml.ForwardingXmlNodeReadOnlyTrx;
import io.sirix.api.Axis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.NestedAxis;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.xml.XmlNameFilter;
import io.sirix.io.StorageType;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderedUnionAxisTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mergeResetsAndSharesItsCursorWithPendingOperands(final boolean storeDeweyIds) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection =
          store.create("collection", new DocumentParser("<r><a><b><hit><hit/></hit></b><hit/><b><hit/></b></a>"
              + "<a><hit/><b><hit/></b></a><a><hit/></a><a><b><hit/></b></a><a/></r>"));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToFirstChild());
        final LongArrayList contexts = collect(new ChildAxis(rtx));
        assertTrue(rtx.moveTo(contexts.getLong(0)));
        final Axis direct = children(rtx, "hit");
        final Axis nested = new NestedAxis(children(rtx, "b"), children(rtx, "hit"));
        final Axis deep =
            new NestedAxis(new NestedAxis(children(rtx, "b"), children(rtx, "hit")), children(rtx, "hit"));
        final Axis merge = new OrderedUnionAxis(rtx, new OrderedUnionAxis(rtx, direct, nested), deep);
        for (final long contextKey : contexts) {
          assertTrue(rtx.moveTo(contextKey));
          final LongArrayList expected =
              collect(new FilterAxis<>(new DescendantAxis(rtx), new XmlNameFilter(rtx, new QNm("hit"))));
          merge.reset(contextKey);
          if (!expected.isEmpty()) {
            assertTrue(merge.hasNext());
            assertTrue(merge.hasNext());
            rtx.moveToDocumentRoot();
            assertEquals(expected.getLong(0), merge.nextLong());
          }
          merge.reset(contextKey);
          final LongArrayList actual = new LongArrayList();
          while (merge.hasNext()) {
            assertTrue(merge.hasNext());
            rtx.moveToDocumentRoot();
            actual.add(merge.nextLong());
          }
          assertEquals(expected, actual);
          assertFalse(merge.hasNext());
          assertEquals(contextKey, rtx.getNodeKey());
        }
        assertTrue(rtx.moveTo(contexts.getLong(0)));
        final Axis duplicateMerge = new OrderedUnionAxis(rtx, children(rtx, "hit"), children(rtx, "hit"));
        final LongArrayList expected = collect(children(rtx, "hit"));
        duplicateMerge.reset(contexts.getLong(0));
        assertEquals(expected, collect(duplicateMerge));
      }
    }
  }

  @Test
  void structuralMergeScansWideSiblingListsOnlyForward() {
    final int pairs = 256;
    final String document = "<r>" + "<b><hit/></b><hit/>".repeat(pairs) + "</r>";
    try (final BasicXmlDBStore store = configuredStore(false)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser(document));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx delegate = session.beginNodeReadOnlyTrx()) {
        assertTrue(delegate.moveToFirstChild());
        final long startKey = delegate.getNodeKey();
        final LongArrayList expected =
            collect(new FilterAxis<>(new DescendantAxis(delegate), new XmlNameFilter(delegate, new QNm("hit"))));
        assertEquals(2 * pairs, expected.size());
        assertTrue(delegate.moveTo(startKey));
        final CountingTrx rtx = new CountingTrx(delegate);
        final Axis merge =
            new OrderedUnionAxis(rtx, children(rtx, "hit"), new NestedAxis(children(rtx, "b"), children(rtx, "hit")));
        assertTrue(merge.hasNext());
        assertEquals(0, rtx.siblingMoves);
        assertEquals(expected, collect(merge));
        assertTrue(rtx.siblingMoves > 0);
        assertTrue(rtx.siblingMoves <= 2 * pairs, "sibling moves: " + rtx.siblingMoves);
      }
    }
  }

  private BasicXmlDBStore configuredStore(final boolean storeDeweyIds) {
    return BasicXmlDBStore.newBuilder()
                          .location(directory)
                          .storageType(StorageType.FILE_CHANNEL)
                          .storeDeweyIds(storeDeweyIds)
                          .build();
  }

  private static Axis children(final XmlNodeReadOnlyTrx rtx, final String name) {
    return new FilterAxis<>(new ChildAxis(rtx), new XmlNameFilter(rtx, new QNm(name)));
  }

  private static LongArrayList collect(final Axis axis) {
    final LongArrayList keys = new LongArrayList();
    while (axis.hasNext()) {
      keys.add(axis.nextLong());
    }
    return keys;
  }

  private static final class CountingTrx implements ForwardingXmlNodeReadOnlyTrx {
    private final XmlNodeReadOnlyTrx delegate;
    private int siblingMoves;

    private CountingTrx(final XmlNodeReadOnlyTrx delegate) {
      this.delegate = delegate;
    }

    @Override
    public XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
      return delegate;
    }

    @Override
    public boolean moveToRightSibling() {
      siblingMoves++;
      return delegate.moveToRightSibling();
    }
  }
}
