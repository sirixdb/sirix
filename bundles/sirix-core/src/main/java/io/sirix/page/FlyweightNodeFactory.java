package io.sirix.page;

import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import net.openhft.hashing.LongHashFunction;

import java.lang.foreign.MemorySegment;

/**
 * Factory for creating flyweight node shells and binding them to a unified page MemorySegment.
 *
 * <p>This factory creates minimal node instances (binding shells) that are immediately
 * bound to a record in the page heap. After binding, all getters/setters operate
 * directly on page memory via the per-record offset table.</p>
 */
public final class FlyweightNodeFactory {

  private FlyweightNodeFactory() {
    throw new AssertionError("Utility class");
  }

  /**
   * Create a flyweight node shell and bind it to a record in the unified page.
   *
   * @param page         the unified page MemorySegment
   * @param slotIndex    the slot index (0 to 1023)
   * @param nodeKey      the node key for this record
   * @param hashFunction the hash function from resource config
   * @return the bound flyweight node
   * @throws IllegalArgumentException if the nodeKindId is not a known flyweight type
   */
  public static FlyweightNode createAndBind(final MemorySegment page, final int slotIndex,
      final long nodeKey, final LongHashFunction hashFunction) {
    final int heapOffset = PageLayout.getDirHeapOffset(page, slotIndex);
    final int nodeKindId = PageLayout.getDirNodeKindId(page, slotIndex);
    final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);

    final FlyweightNode node = createShell(nodeKindId, nodeKey, hashFunction);
    node.bind(page, recordBase, nodeKey, slotIndex);
    return node;
  }

  /**
   * Create a minimal flyweight node shell for binding.
   * The shell has only nodeKey and hashFunction initialized.
   * All other fields will be read from page memory after bind().
   */
  private static FlyweightNode createShell(final int nodeKindId, final long nodeKey,
      final LongHashFunction hashFunction) {
    return switch (nodeKindId) {
      case 24 -> new ObjectNode(nodeKey, hashFunction);           // OBJECT
      case 25 -> new ArrayNode(nodeKey, hashFunction);            // ARRAY
      case 26 -> new ObjectKeyNode(nodeKey, hashFunction);        // OBJECT_KEY
      case 27 -> new BooleanNode(nodeKey, hashFunction);          // BOOLEAN_VALUE
      case 28 -> new NumberNode(nodeKey, hashFunction);           // NUMBER_VALUE
      case 29 -> new NullNode(nodeKey, hashFunction);             // NULL_VALUE
      case 30 -> new StringNode(nodeKey, hashFunction);           // STRING_VALUE
      case 31 -> new JsonDocumentRootNode(nodeKey, hashFunction); // JSON_DOCUMENT
      case 40 -> new ObjectStringNode(nodeKey, hashFunction);     // OBJECT_STRING_VALUE
      case 41 -> new ObjectBooleanNode(nodeKey, hashFunction);    // OBJECT_BOOLEAN_VALUE
      case 42 -> new ObjectNumberNode(nodeKey, hashFunction);     // OBJECT_NUMBER_VALUE
      case 43 -> new ObjectNullNode(nodeKey, hashFunction);       // OBJECT_NULL_VALUE
      default -> throw new IllegalArgumentException(
          "Unknown flyweight node kind ID: " + nodeKindId);
    };
  }
}
