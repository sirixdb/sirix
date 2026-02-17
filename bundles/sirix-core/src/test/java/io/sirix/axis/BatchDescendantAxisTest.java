package io.sirix.axis;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.Axis;
import io.sirix.api.Filter;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.filter.AbstractFilter;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.exception.SirixException;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;

public class BatchDescendantAxisTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testMatchesDescendantAxisOrderDifferentBatchSizes() {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.moveToDocumentRoot();

    final var expected = collectAll(new DescendantAxis(rtx));

    // Try a few batch sizes including tiny and larger.
    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.NO, 1));
    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.NO, 7));
    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.NO, 128));
    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.NO, 4096));
  }

  @Test
  public void testIncludeSelfMatchesDescendantAxis() {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.moveToDocumentRoot();

    final var expected = collectAll(new DescendantAxis(rtx, IncludeSelf.YES));

    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.YES, 13));
    assertEquals(expected, collectAllBatched(rtx, IncludeSelf.YES, 1024));
    assertEquals(expected, collectAllCallback(rtx, IncludeSelf.YES, 17));
  }

  @Test
  public void testFilteredMatchesFilterAxis() {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.moveToDocumentRoot();

    final Filter<XmlNodeReadOnlyTrx> evenKeyFilter = new AbstractFilter<>(rtx) {
      @Override
      public boolean filter() {
        // purely structural/flyweight-friendly predicate
        return (getTrx().getNodeKey() & 1L) == 0L;
      }
    };

    // Expected: existing FilterAxis on top of DescendantAxis.
    final var expected = collectAll(new FilterAxis<>(new DescendantAxis(rtx, IncludeSelf.YES), evenKeyFilter));

    // Actual: batch traversal using the same Filter implementation.
    rtx.moveToDocumentRoot();
    final var axis = new BatchDescendantAxis(rtx, IncludeSelf.YES);
    final var actual = new ArrayList<Long>();
    while (axis.forEachNextFiltered(3, actual::add, evenKeyFilter) > 0) {
      // keep draining
    }

    assertEquals(expected, actual);
  }

  @Test
  public void testResetAndSubtree() {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.moveTo(9L);

    final var expected = collectAll(new DescendantAxis(rtx, IncludeSelf.YES));

    final var axis = new BatchDescendantAxis(rtx, IncludeSelf.YES);
    axis.reset(9L);

    final var out = new LongArrayList();
    final var actual = new ArrayList<Long>();
    while (axis.nextBatch(out, 2) > 0) {
      for (int i = 0; i < out.size(); i++) {
        actual.add(out.getLong(i));
      }
    }

    assertEquals(expected, actual);
    // Like AbstractAxis, we reset the cursor to the start key when done.
    assertEquals(9L, rtx.getNodeKey());
  }

  private static List<Long> collectAll(final Axis axis) {
    final var result = new ArrayList<Long>();
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      if (key == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        break;
      }
      result.add(key);
    }
    return result;
  }

  private List<Long> collectAllBatched(final XmlNodeReadOnlyTrx rtx, final IncludeSelf includeSelf,
      final int batchSize) {
    rtx.moveToDocumentRoot();
    final var axis = new BatchDescendantAxis(rtx, includeSelf);
    final var out = new LongArrayList(batchSize);

    final var result = new ArrayList<Long>();
    while (axis.nextBatch(out, batchSize) > 0) {
      for (int i = 0; i < out.size(); i++) {
        result.add(out.getLong(i));
      }
    }
    return result;
  }

  private List<Long> collectAllCallback(final XmlNodeReadOnlyTrx rtx, final IncludeSelf includeSelf,
      final int batchSize) {
    rtx.moveToDocumentRoot();
    final var axis = new BatchDescendantAxis(rtx, includeSelf);

    final var result = new ArrayList<Long>();
    final LongConsumer consumer = result::add;

    while (axis.forEachNext(batchSize, consumer) > 0) {
      // keep draining
    }
    return result;
  }
}


