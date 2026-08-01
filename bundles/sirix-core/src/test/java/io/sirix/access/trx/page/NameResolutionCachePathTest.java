/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.node.NodeKind;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.shredder.XmlShredder;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A reader resolves a name through the revision's cached dictionary when it can, instead of
 * deserializing the {@code NamePage} to get at it — {@code NamePage} is on the page cache's
 * index-root exclusion list, so resolving it costs a page read on the first name of every
 * transaction.
 *
 * <p>Which dictionary a kind belongs to is then decided in two places that must agree:
 * {@code NamePage.dictionaryOffset}, used to build the cache key, and the switches inside
 * {@code NamePage.getName}/{@code getRawName}, used to build the dictionary in the first place. A
 * disagreement resolves names against the WRONG dictionary — silently, and only once the cache is
 * warm, which is the worst shape a bug can have. XML is where it would show, because its four name
 * kinds live in four different dictionaries; JSON has only one.
 *
 * <p>So each test reads every name twice: once through a transaction that necessarily built the
 * dictionary itself, and again through transactions opened afterwards, which take the cached path.
 * The two must agree for every name of every kind.
 *
 * @author Johannes Lichtenberger
 */
final class NameResolutionCachePathTest {

  private static final String XML_RESOURCE = "nameResolutionXmlResource";

  private static final String JSON_RESOURCE = "nameResolutionJsonResource";

  /** Carries all four XML name kinds: elements, a namespace prefix, attributes and a PI target. */
  private static final String XML_DOCUMENT = """
      <ns:root xmlns:ns="http://example.org/ns" id="7" kind="root-element">\
      <?render fast?>\
      <ns:child attribute="v" other="w">text</ns:child>\
      <plain nested="deep"><leaf final="yes"/></plain>\
      </ns:root>""";

  /** Distinct member names, so a mis-resolved key cannot accidentally match its neighbour. */
  private static final String JSON_DOCUMENT = """
      {"alpha":1,"beta":[2,3],"gamma":{"delta":"d","epsilon":null},"zeta":true,"eta":"tail"}""";

  @TempDir
  Path tempDir;

  private Database<XmlResourceSession> xmlDatabase;

  private Database<JsonResourceSession> jsonDatabase;

  @AfterEach
  void tearDown() {
    if (xmlDatabase != null) {
      xmlDatabase.close();
    }
    if (jsonDatabase != null) {
      jsonDatabase.close();
    }
  }

  /**
   * The shortcut may only engage for kinds whose names genuinely come from a dictionary.
   *
   * <p>{@code NamePage.getName} answers {@code ARRAY} and {@code OBJECT} with the synthetic
   * {@code __array__} / {@code __object__} literals and consults no dictionary at all — the path
   * summary asks it for precisely those — and {@code getRawName} does not accept them. So the three
   * sets are deliberately different, and {@code dictionaryOffset} has to decline rather than throw
   * or guess. It threw once; it took 53 test failures to say so.
   */
  @Test
  void kindsWithoutADictionaryDecline() {
    assertEquals(NamePage.NO_DICTIONARY, NamePage.dictionaryOffset(NodeKind.ARRAY));
    assertEquals(NamePage.NO_DICTIONARY, NamePage.dictionaryOffset(NodeKind.OBJECT));
    assertEquals(NamePage.NO_DICTIONARY, NamePage.dictionaryOffset(NodeKind.JSON_DOCUMENT));
    assertEquals(NamePage.NO_DICTIONARY, NamePage.dictionaryOffset(NodeKind.STRING_VALUE));
    assertEquals(NamePage.NO_DICTIONARY, NamePage.dictionaryOffset(NodeKind.TEXT));

    // …and must engage for every kind that does have one.
    assertTrue(NamePage.dictionaryOffset(NodeKind.ELEMENT) >= 0);
    assertTrue(NamePage.dictionaryOffset(NodeKind.ATTRIBUTE) >= 0);
    assertTrue(NamePage.dictionaryOffset(NodeKind.NAMESPACE) >= 0);
    assertTrue(NamePage.dictionaryOffset(NodeKind.PROCESSING_INSTRUCTION) >= 0);
    for (final NodeKind named : new NodeKind[] {NodeKind.OBJECT_NAMED_OBJECT, NodeKind.OBJECT_NAMED_ARRAY,
        NodeKind.OBJECT_NAMED_BOOLEAN, NodeKind.OBJECT_NAMED_NUMBER, NodeKind.OBJECT_NAMED_STRING,
        NodeKind.OBJECT_NAMED_NULL}) {
      assertTrue(NamePage.dictionaryOffset(named) >= 0, named + " must resolve through a dictionary");
    }
  }

  /**
   * XML routes its four name kinds to four different dictionaries, so this is the case where a
   * kind sent to the wrong one would come back with someone else's name — or with none.
   */
  @Test
  void xmlNamesAgreeBetweenTheBuildingAndTheCachedPath() {
    final Path dbPath = tempDir.resolve("name-resolution-xml-db");
    Databases.createXmlDatabase(new DatabaseConfiguration(dbPath));
    xmlDatabase = Databases.openXmlDatabase(dbPath);
    xmlDatabase.createResource(ResourceConfiguration.newBuilder(XML_RESOURCE).build());

    try (final XmlResourceSession session = xmlDatabase.beginResourceSession(XML_RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(XML_DOCUMENT));
        wtx.commit();
      }

      // Pass one: this transaction is the one that builds the dictionaries.
      final Long2ObjectLinkedOpenHashMap<String> expected = new Long2ObjectLinkedOpenHashMap<>();
      final Long2ObjectLinkedOpenHashMap<NodeKind> kinds = new Long2ObjectLinkedOpenHashMap<>();
      try (final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        final DescendantAxis axis = new DescendantAxis(rtx, IncludeSelf.YES);
        while (axis.hasNext()) {
          final long nodeKey = axis.nextLong();
          recordXmlName(rtx, nodeKey, expected, kinds);
          if (rtx.getKind() == NodeKind.ELEMENT) {
            for (int i = 0, attributes = rtx.getAttributeCount(); i < attributes; i++) {
              assertTrue(rtx.moveToAttribute(i));
              recordXmlName(rtx, rtx.getNodeKey(), expected, kinds);
              assertTrue(rtx.moveTo(nodeKey));
            }
            for (int i = 0, namespaces = rtx.getNamespaceCount(); i < namespaces; i++) {
              assertTrue(rtx.moveToNamespace(i));
              recordXmlName(rtx, rtx.getNodeKey(), expected, kinds);
              assertTrue(rtx.moveTo(nodeKey));
            }
          }
        }
      }

      // The fixture must actually reach all four dictionaries, or the test proves nothing about
      // the mapping it exists to guard.
      assertTrue(expected.values().stream().anyMatch(n -> n.contains("root")), "an element name was expected");
      assertTrue(expected.values().stream().anyMatch(n -> n.contains("id")), "an attribute name was expected");
      assertTrue(expected.values().stream().anyMatch(n -> n.contains("render")), "a PI target was expected");
      assertTrue(kinds.values().stream().anyMatch(k -> k == NodeKind.ELEMENT), "an element was expected");
      assertTrue(kinds.values().stream().anyMatch(k -> k == NodeKind.ATTRIBUTE), "an attribute was expected");
      assertTrue(kinds.values().stream().anyMatch(k -> k == NodeKind.NAMESPACE), "a namespace was expected");
      assertTrue(kinds.values().stream().anyMatch(k -> k == NodeKind.PROCESSING_INSTRUCTION),
          "a processing instruction was expected");

      // Pass two: every one of these transactions opens AFTER the dictionaries exist, so each
      // takes the cached path — including a fresh one per name, which is the shape a
      // request-per-transaction API produces.
      for (final Map.Entry<Long, String> entry : expected.entrySet()) {
        try (final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertTrue(rtx.moveTo(entry.getKey()));
          assertEquals(entry.getValue(), String.valueOf(rtx.getName()),
              "name of " + kinds.get((long) entry.getKey()) + " node " + entry.getKey());
        }
      }
    }
  }

  private static void recordXmlName(final XmlNodeReadOnlyTrx rtx, final long nodeKey,
      final Long2ObjectLinkedOpenHashMap<String> expected, final Long2ObjectLinkedOpenHashMap<NodeKind> kinds) {
    final NodeKind kind = rtx.getKind();
    if (kind != NodeKind.ELEMENT && kind != NodeKind.ATTRIBUTE && kind != NodeKind.NAMESPACE
        && kind != NodeKind.PROCESSING_INSTRUCTION) {
      return;
    }
    final var name = rtx.getName();
    if (name == null) {
      return;
    }
    expected.put(nodeKey, String.valueOf(name));
    kinds.put(nodeKey, kind);
  }

  /** JSON has one dictionary, but it is the one the serializer walks on every named node. */
  @Test
  void jsonObjectKeysAgreeBetweenTheBuildingAndTheCachedPath() {
    final Path dbPath = tempDir.resolve("name-resolution-json-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    jsonDatabase = Databases.openJsonDatabase(dbPath);
    jsonDatabase.createResource(ResourceConfiguration.newBuilder(JSON_RESOURCE).build());

    try (final JsonResourceSession session = jsonDatabase.beginResourceSession(JSON_RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(JSON_DOCUMENT));
        wtx.commit();
      }

      final Long2ObjectLinkedOpenHashMap<String> expected = new Long2ObjectLinkedOpenHashMap<>();
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        final DescendantAxis axis = new DescendantAxis(rtx, IncludeSelf.YES);
        while (axis.hasNext()) {
          final long nodeKey = axis.nextLong();
          if (rtx.isObjectKey() || rtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT
              || rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY) {
            expected.put(nodeKey, rtx.getName().getLocalName());
          }
        }
      }

      assertFalse(expected.isEmpty(), "the fixture document must carry named members");
      assertTrue(expected.values().contains("delta"), "a nested member name was expected");

      for (final Map.Entry<Long, String> entry : expected.entrySet()) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertTrue(rtx.moveTo(entry.getKey()));
          assertEquals(entry.getValue(), rtx.getName().getLocalName(), "name of node " + entry.getKey());
          // The raw-bytes path the serializer actually uses must agree with getName().
          final byte[] rawName = rtx.getNameBytes();
          assertEquals(entry.getValue(), new String(rawName, java.nio.charset.StandardCharsets.UTF_8),
              "raw name of node " + entry.getKey());
        }
      }
    }
  }
}
