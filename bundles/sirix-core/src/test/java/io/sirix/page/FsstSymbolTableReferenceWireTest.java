/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A page names its FSST symbol table by id — it never carries the table itself.
 *
 * <p>The symbol table is identical across every page of a revision and runs to a couple of
 * kilobytes, so embedding it charged that much per page — megabytes across a large resource — on
 * top of rebuilding it per page and re-parsing it per page on read. The page stores a
 * {@code varLong} id naming a record in the dictionary trie; there is deliberately no inline
 * fallback (no databases predate the dictionary), so a page holding table bytes without an id is
 * refused at serialization. That refusal is load-bearing: such a page's compressed slots would
 * reach disk with no trace of which symbols they were encoded against — readable in this process,
 * silent garbage after a reopen.
 */
@DisplayName("A page references its FSST symbol table")
public final class FsstSymbolTableReferenceWireTest {

  private static final long PAGE_KEY = 3L;

  /** Opaque to the page — only that it is non-empty matters here. */
  private static final byte[] ORPHAN_TABLE =
      "a-symbol-table-with-no-dictionary-id".getBytes(StandardCharsets.UTF_8);

  @Test
  @DisplayName("an id survives a serialize/deserialize round trip")
  void referenceRoundTrips() {
    final long id = 4242L;
    final KeyValueLeafPage read = roundTrip(page -> page.setFsstSymbolTableId(id));

    assertEquals(id, read.getFsstSymbolTableId(), "the page lost its symbol-table reference");
    assertNull(read.getFsstSymbolTable(),
        "the table must stay unresolved until a reader with a revision to look it up in asks for "
            + "a string — deserialization has no storage-engine reader, and most pages are never "
            + "asked for one");
  }

  /**
   * Table bytes without an id cannot reach disk.
   *
   * <p>The bytes-only state exists legitimately in memory — the writer hands pages the table to
   * encode with before it assigns the id — but serializing it would strand the page's compressed
   * strings: nothing on disk would say which symbols they were encoded against, so they would
   * read back as garbage after a reopen, with no error anywhere. The write is the last moment the
   * mistake is cheap, so that is where it must fail.
   */
  @Test
  @DisplayName("a table without an id is refused at serialization")
  void tableWithoutAnIdIsRefused() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("wire").build();
    final KeyValueLeafPage page = newPage(config);
    page.setFsstSymbolTable(ORPHAN_TABLE);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    assertThrows(SirixIOException.class,
        () -> PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA),
        "a page with table bytes but no dictionary id must not serialize");
  }

  @Test
  @DisplayName("a page with neither reports neither")
  void noTableAtAll() {
    final KeyValueLeafPage read = roundTrip(page -> { });

    assertNull(read.getFsstSymbolTable());
    assertEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, read.getFsstSymbolTableId());
  }

  /**
   * The reference must cost next to nothing — that is the entire point of not embedding.
   */
  @Test
  @DisplayName("a reference costs only a few bytes over no table at all")
  void aReferenceCostsAFewBytes() {
    final long withReference = serializedSize(page -> page.setFsstSymbolTableId(4242L));
    final long withoutTable = serializedSize(page -> { });

    assertTrue(withReference - withoutTable <= 10,
        "a reference costs " + (withReference - withoutTable) + " bytes over an unreferenced page;"
            + " a varLong id should cost a handful");
  }

  private static KeyValueLeafPage roundTrip(final Consumer<KeyValueLeafPage> setUp) {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("wire").build();
    final KeyValueLeafPage page = newPage(config);
    setUp.accept(page);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    final var source = sink.bytesForRead();
    source.readByte(); // page-kind id, consumed by the caller in production too
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
        .deserializePage(config, source, SerializationType.DATA);
  }

  private static long serializedSize(final Consumer<KeyValueLeafPage> setUp) {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("wire").build();
    final KeyValueLeafPage page = newPage(config);
    setUp.accept(page);
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    return sink.writePosition();
  }

  private static KeyValueLeafPage newPage(final ResourceConfiguration config) {
    Allocators.getInstance().init(64L * 1024 * 1024);
    return new KeyValueLeafPage(PAGE_KEY, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), Allocators.getInstance().allocate(4096), null, -1);
  }
}
