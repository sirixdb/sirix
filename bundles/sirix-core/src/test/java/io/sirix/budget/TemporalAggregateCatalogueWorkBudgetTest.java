package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.node.Node;
import io.sirix.access.trx.node.IndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.aggregate.TemporalAggregateSpec;
import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unrelated catalogue access does no aggregate decoding; an actual consumer decodes once. */
@Isolated
final class TemporalAggregateCatalogueWorkBudgetTest {
  private static final String CATALOGUE = """
      <indexes><index type="TEMPORAL_AGGREGATE" dbType="JSON" id="0">
      <path>/[]</path><temporalAggregate projection="2">
      <group left="1"/><lane left="3" right="-1" operations="14"/>
      </temporalAggregate></index></indexes>
      """;

  private static Node<?> catalogue(final String xml) {
    return IndexController.deserialize(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))).getFirstChild();
  }

  @Test
  void copyingAndPublishingUnusedDeclarationsDecodesNothing() throws Exception {
    final long before = IndexDef.temporalAggregateDecodeCount();
    final Indexes parsed = new Indexes();
    parsed.init(catalogue(CATALOGUE));
    final Indexes reader = new Indexes();
    reader.initFrom(7, parsed.getIndexDefsInOrder());
    final Indexes roundTrip = new Indexes();
    roundTrip.init(reader.materialize());
    assertEquals(1, reader.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
    assertFalse(reader.differsFromPersisted());
    assertEquals(before, IndexDef.temporalAggregateDecodeCount(), "catalogue access decoded unused aggregate lanes");

    final IndexDef declaration = roundTrip.getIndexDef(0, IndexType.TEMPORAL_AGGREGATE);
    final Callable<TemporalAggregateSpec> read = declaration::getTemporalAggregateSpec;
    try (var executor = Executors.newFixedThreadPool(4)) {
      final List<Future<TemporalAggregateSpec>> reads = executor.invokeAll(Collections.nCopies(32, read));
      final TemporalAggregateSpec spec = reads.getFirst().get();
      assertEquals(new TemporalAggregateSpec(2, List.of(1), List.of(new Lane(3, -1, 14))), spec);
      for (final Future<TemporalAggregateSpec> result : reads) {
        assertSame(spec, result.get());
      }
    }
    assertEquals(before + 1, IndexDef.temporalAggregateDecodeCount(), "first use must decode exactly once");
  }

  @Test
  void deferredDefinitionOwnsItsSnapshotAndStillRejectsInvalidOperands() {
    final Node<?> source = catalogue(CATALOGUE);
    final Indexes parsed = new Indexes();
    parsed.init(source);
    final Node<?> aggregate = source.getFirstChild().getFirstChild().getNextSibling();
    aggregate.deleteAttribute(new QNm("projection"));
    aggregate.setAttribute(new QNm("projection"), new Str("99"));
    final IndexDef original = parsed.getIndexDef(0, IndexType.TEMPORAL_AGGREGATE);
    final Node<?> published = parsed.materialize();
    published.getFirstChild().getFirstChild().getNextSibling().deleteAttribute(new QNm("projection"));
    assertEquals(2, original.getTemporalAggregateSpec().projectionId());

    final Indexes different = new Indexes();
    different.init(source);
    assertFalse(original.hasSameDefinition(different.getIndexDef(0, IndexType.TEMPORAL_AGGREGATE)));
    final Indexes invalid = new Indexes();
    invalid.init(catalogue(CATALOGUE.replace("operations=\"14\"", "operations=\"16\"")));
    assertThrows(IllegalArgumentException.class,
        () -> invalid.getIndexDef(0, IndexType.TEMPORAL_AGGREGATE).getTemporalAggregateSpec());
  }
}
