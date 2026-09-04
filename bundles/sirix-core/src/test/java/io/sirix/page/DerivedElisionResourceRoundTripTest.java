/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.XmlTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;

import java.lang.foreign.MemorySegment;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end witness for the derived elision sections: whole revisions, through commit and reload.
 *
 * <p>
 * {@link DerivedElisionSectionTest} proves the derivation at the page frame. This proves it where a
 * user can see it — every shape whose structure the derivation could plausibly be wrong about is
 * loaded twice, once with the per-slot tuples and once derived, and the two revisions must
 * serialize to the same JSON. The shapes are the ones the brief for this lever names: a deleted
 * middle field, a moved subtree, a nested object crossing a page boundary, an empty object, an
 * array of scalars, and a {@code SLIDING_SNAPSHOT} fragment with three modified slots.
 *
 * <p>
 * Each case also asserts that the two forms really are different on the wire (via
 * {@link DerivedElisionSectionTest}'s page-level guard, and here by the resource's own byte size),
 * so an agreement cannot come from the lever never engaging.
 */
@DisplayName("Derived elision sections, whole resources")
final class DerivedElisionResourceRoundTripTest {

  private boolean derivedElisionBefore;

  @BeforeAll
  static void requireTheInstrumentIsOn() {
    assertTrue(PageKind.sectionDiagEnabled(),
        "the section diagnostic is off: run with -Dsirix.pageSectionDiag=true. The metadata counters "
            + "this suite compares the two forms with read zero when the gate is off, so a passing run "
            + "would prove nothing.");
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
  }

  @AfterEach
  void tearDown() {
    PageKind.DERIVED_ELISION_SECTIONS = derivedElisionBefore;
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("a deleted middle field")
  void deletedMiddleField() {
    assertSameRevision(wtx -> {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"a\":1,\"b\":\"two\",\"c\":true,\"d\":4,\"e\":\"five\"}"));
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      wtx.moveToRightSibling();
      wtx.moveToRightSibling();
      wtx.remove();
      wtx.commit();
    });
  }

  @Test
  @DisplayName("a moved subtree")
  void movedSubtree() {
    assertSameRevision(wtx -> {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "[{\"k\":1,\"v\":\"first\"},{\"k\":2,\"v\":\"second\"},{\"k\":3,\"v\":\"third\"}]"));
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      final long first = wtx.getNodeKey();
      wtx.moveToRightSibling();
      wtx.moveToRightSibling();
      wtx.moveSubtreeToRightSibling(first);
      wtx.commit();
    });
  }

  @Test
  @DisplayName("a nested object crossing a page boundary")
  void nestedObjectCrossingAPageBoundary() {
    // Well past the 1,024 records a leaf holds, so the object's fields — and the region tags they
    // carry — are split across several pages with a nested object straddling the seam.
    final StringBuilder json = new StringBuilder("{\"head\":{\"x\":1},\"body\":{");
    for (int i = 0; i < 1_400; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("\"f").append(i).append("\":");
      switch (i % 4) {
        case 0 -> json.append(i);
        case 1 -> json.append('"').append("value-").append(i).append('"');
        case 2 -> json.append(i % 2 == 0);
        default -> json.append(i).append(".5");
      }
    }
    json.append("},\"tail\":{\"y\":\"z\"}}");
    assertSameRevision(wtx -> {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
      wtx.commit();
    });
  }

  @Test
  @DisplayName("a timestamp column on the TEMPORAL lane, whose elided slots are RENDERED, not copied")
  void temporalLaneColumnWithElidedSlots() {
    // A temporal tag stores no value bytes at all, so an elided slot under one cannot be filled by
    // copying from the page -- the value is rendered back from the packed number. That render is the
    // only route those slots have, and it runs once per elided slot on the read path.
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 1_400; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(i)
          .append(",\"ts\":\"2013-07-15 12:00:")
          .append(i % 60 < 10
              ? "0"
              : "")
          .append(i % 60)
          .append("\",\"day\":\"2013-07-")
          .append(i % 28 + 1 < 10
              ? "0"
              : "")
          .append(i % 28 + 1)
          .append("\",\"note\":\"row-")
          .append(i)
          .append("\"}");
    }
    json.append(']');
    StringRegion.setTemporalLaneEnabled(true);
    try {
      assertSameRevision(wtx -> {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
        wtx.commit();
      });
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
    // Taken with the write override CLEARED and against the PERSISTED region only. resolveTemporal
    // is ALL-OR-NOTHING: one entry it cannot encode and the whole tag keeps its bytes,
    // injectTemporalString is never called, and the round trip above would agree for a reason that
    // has nothing to do with the render.
    assertTrue(pagesWithAPersistedTemporalTag() > 0,
        "no page took the temporal lane, so no elided slot was RENDERED and this case proves nothing");
  }

  /**
   * Leaf pages of the surviving resource whose PERSISTED string region carries a tag that took the
   * temporal lane.
   *
   * <p>
   * Deliberately {@link KeyValueLeafPage#getStringRegionPayload()} and not
   * {@code getStringRegionHeader()}: the latter falls back to re-deriving the region from the slotted
   * page when none was persisted, and a derive is an ENCODE, so it consults
   * {@code temporalLaneEnabled()} at read time. Asking for the payload does not trigger that
   * fallback.
   * </p>
   *
   * <p>
   * The payload accessor alone is not a guarantee of provenance — a derive installs its result into
   * the same region table, so a page some earlier call already derived would answer from that. What
   * closes it here is that the caller takes this witness with the write override CLEARED: a derive
   * under a disarmed switch cannot produce a temporal tag, so this count can only be raised by tags
   * that were genuinely written.
   * </p>
   */
  private static int pagesWithAPersistedTemporalTag() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = rtx.getStorageEngineReader();
      final long pages = (rtx.getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
      final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, rtx.getRevisionNumber());
      int converted = 0;
      for (long pk = 0; pk < pages; pk++) {
        final var res = reader.getRecordPage(key.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) {
          continue;
        }
        final MemorySegment payload = kv.getStringRegionPayload();
        if (payload == null || payload.byteSize() == 0) {
          continue;
        }
        final StringRegion.Header header = new StringRegion.Header().parseInto(payload);
        for (int tag = 0; tag < header.parentDictSize; tag++) {
          if (header.tagTemporal[tag]) {
            converted++;
            break;
          }
        }
      }
      return converted;
    }
  }

  @Test
  @DisplayName("an empty object and an array of scalars")
  void emptyObjectAndArrayOfScalars() {
    assertSameRevision(wtx -> {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"empty\":{},\"scalars\":[1,2,3,\"four\",true,null,6.5],\"emptyArray\":[]}"));
      wtx.commit();
    });
  }

  @Test
  @DisplayName("a SLIDING_SNAPSHOT fragment with three modified slots")
  void slidingSnapshotFragmentWithThreeModifiedSlots() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                              .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                              .build();
    assertSameRevision(config, wtx -> {
      final StringBuilder json = new StringBuilder("{");
      for (int i = 0; i < 60; i++) {
        if (i > 0) {
          json.append(',');
        }
        json.append("\"f").append(i).append("\":").append(i);
      }
      json.append('}');
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
      wtx.commit();
      // Three slots of the page change; every other slot is carried forward by the fragment, which is
      // exactly the shape whose elided values a mis-derived rank would shift.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      wtx.setNumberValue(1_000);
      wtx.moveToRightSibling();
      wtx.moveToRightSibling();
      wtx.setNumberValue(2_000);
      wtx.moveToRightSibling();
      wtx.moveToRightSibling();
      wtx.setNumberValue(3_000);
      wtx.commit();
    });
  }

  @Test
  @DisplayName("an XML page with attributes")
  void xmlPageWithAttributes() {
    // XML records carry no fused OBJECT_NAMED_* slot, so neither elision section applies to them at
    // all — which is exactly why the shape is here: the reader's new dispatch must leave a page that
    // states neither form completely alone.
    XmlTestHelper.deleteEverything();
    try {
      final String xml =
          "<root a=\"1\" b=\"two\"><child c=\"3\">text</child>" + "<child c=\"4\" d=\"four\">more text</child></root>";
      PageKind.DERIVED_ELISION_SECTIONS = false;
      final String tupleXml = buildAndSerializeXml(xml);
      XmlTestHelper.deleteEverything();
      Databases.getGlobalBufferManager().clearAllCaches();
      PageKind.DERIVED_ELISION_SECTIONS = true;
      final String derivedXml = buildAndSerializeXml(xml);
      assertTrue(tupleXml.contains("<child"), "the fixture must actually produce a document to compare");
      assertEquals(tupleXml, derivedXml, "an XML page must round-trip identically under either form");
    } finally {
      XmlTestHelper.deleteEverything();
    }
  }

  private String buildAndSerializeXml(final String xml) {
    try (final Database<XmlResourceSession> database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      try (final XmlNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(xml));
        wtx.commit();
      }
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      XmlSerializer.newBuilder(session, out).emitXMLDeclaration().build().call();
      return out.toString(StandardCharsets.UTF_8);
    }
  }

  // ──────────────────────────────────────────────────────────────── helpers

  private void assertSameRevision(final Consumer<JsonNodeTrx> build) {
    assertSameRevision(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).build(), build);
  }

  /**
   * Build the same resource twice — once with the per-slot tuples, once derived — and require the two
   * to reconstruct the same revision on strictly fewer metadata bytes.
   *
   * <p>
   * The elision-metadata counters are what makes the comparison mean something: they say how many
   * bytes each form actually staged across the whole load, so an agreement cannot come from a lever
   * that never engaged. The last step re-reads the derived resource with the switch OFF, which is the
   * witness that the reader dispatches on the page's own flag rather than on this process's setting.
   */
  private void assertSameRevision(final ResourceConfiguration config, final Consumer<JsonNodeTrx> build) {
    PageKind.DERIVED_ELISION_SECTIONS = false;
    final long tupleMetaBefore = elisionMetadataBytes();
    final String tupleJson = buildAndSerialize(config, build);
    final long tupleMeta = elisionMetadataBytes() - tupleMetaBefore;

    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();

    PageKind.DERIVED_ELISION_SECTIONS = true;
    final long derivedMetaBefore = elisionMetadataBytes();
    final String derivedJson = buildAndSerialize(config, build);
    final long derivedMeta = elisionMetadataBytes() - derivedMetaBefore;

    assertTrue(tupleJson.length() > 2, "the fixture must actually produce a revision to compare");
    assertEquals(tupleJson, derivedJson, "the derived sections must reconstruct the same revision");
    // Small shapes elide little or nothing — an array of scalars holds no fused OBJECT_NAMED_* slot at
    // all — so they are correctness fixtures, not byte witnesses. Where the lever does engage, it has
    // to move the number, and on a page-crossing load it has to move it down.
    if (tupleMeta > 0 || derivedMeta > 0) {
      assertNotEquals(tupleMeta, derivedMeta,
          "the two forms must stage different metadata, or this shape witnesses nothing about the lever");
    }
    if (tupleMeta > 1_000) {
      assertTrue(derivedMeta < tupleMeta,
          "the derived form must stage fewer metadata bytes — " + derivedMeta + " vs " + tupleMeta);
    }

    // A page states its own form, so a reader configured for the other one must still read it.
    PageKind.DERIVED_ELISION_SECTIONS = false;
    Databases.getGlobalBufferManager().clearAllCaches();
    assertEquals(derivedJson, serializeExisting(),
        "a resource written with the derived sections must read back with the switch off");
  }

  /** Bytes both elision sections staged across the whole load, as the writer counted them. */
  private static long elisionMetadataBytes() {
    return PageSectionDiag.stagedValueElisionMetaBytes() + PageSectionDiag.stagedNameKeyElisionMetaBytes();
  }

  private String serializeExisting() {
    // Deliberately NOT through the helper's instance cache: the resource is on disk and this has to be
    // a fresh open, or the pages would come back out of a buffer that never re-read them.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final StringWriter writer = new StringWriter();
      JsonSerializer.newBuilder(session, writer).build().call();
      return writer.toString();
    }
  }

  private String buildAndSerialize(final ResourceConfiguration config, final Consumer<JsonNodeTrx> build) {
    try (
        final Database<JsonResourceSession> database =
            JsonTestHelper.getDatabaseWithResourceConfig(PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        build.accept(wtx);
      }
      final StringWriter writer = new StringWriter();
      JsonSerializer.newBuilder(session, writer).build().call();
      return writer.toString();
    }
  }

}
