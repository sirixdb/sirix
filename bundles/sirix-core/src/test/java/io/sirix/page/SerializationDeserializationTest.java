package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.KeyValueLeafPagePool;
import io.sirix.index.IndexType;
import io.sirix.node.NodeSerializerImpl;
import io.sirix.settings.Constants;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class SerializationDeserializationTest {

  private static final int NUM_RANDOM_INSERTIONS = 1000; // Number of random insertions

  private final KeyValueLeafPagePool pagePool = KeyValueLeafPagePool.getInstance();

  private ResourceConfiguration config;

  @BeforeEach
  public void setUp() {
    pagePool.init(1 << 30); // Initialize the segment allocator with a max size
    config = createResourceConfiguration();
  }

  @AfterEach
  public void tearDown() {
    // Clean up resources if necessary
    pagePool.free();
  }

  // Helper method to create a ResourceConfiguration
  private ResourceConfiguration createResourceConfiguration() {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
  }

  // Helper method to create a ResourceConfiguration with Dewey IDs enabled
  private ResourceConfiguration createResourceConfigurationWithDeweyIDs() {
    // Assuming ResourceConfiguration has methods to enable Dewey IDs.
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).useDeweyIDs(true).build();
  }

  @Test
  public void testPermutedSlotInsertions() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(10_000),
                                                         null,
                                                         0,
                                                         0);

    // Predefined slot numbers for permutation
    int[] slotNumbers = { 0, 5, 10, 15, 20 };

    // Expected data for verification after serialization and deserialization
    byte[][] expectedData = new byte[Constants.NDP_NODE_COUNT][];

    // Insert data into slots
    for (int slotNumber : slotNumbers) {
      byte[] data = new byte[] { 1, 2, 3 }; // Example data
      originalPage.setSlot(data, slotNumber);
      expectedData[slotNumber] = data;
    }

    // Overwrite some slots with different data sizes
    byte[] newData = new byte[] { 4, 5, 6, 7 }; // New data for slot 5
    originalPage.setSlot(newData, 5);
    expectedData[5] = newData;

    newData = new byte[] { 8 }; // New data for slot 15
    originalPage.setSlot(newData, 15);
    expectedData[15] = newData;

    // Serialize the page
    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    // Deserialize the page
    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    // Verify the deserialized data matches the original
    for (int slotNumber : slotNumbers) {
      assertArrayEquals(expectedData[slotNumber],
                        deserializedPage.getSlotAsByteArray(slotNumber),
                        "Mismatch at slot " + slotNumber);
    }

    // Verify the overwritten slots
    assertArrayEquals(new byte[] { 4, 5, 6, 7 },
                      deserializedPage.getSlotAsByteArray(5),
                      "Mismatch at overwritten slot 5");
    assertArrayEquals(new byte[] { 8 }, deserializedPage.getSlotAsByteArray(15), "Mismatch at overwritten slot 15");
  }

  @RepeatedTest(100)
  public void testRandomSlotInsertions() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(110_000),
                                                         null,
                                                         -1,
                                                         -1);
    Random random = new Random();

    byte[][] expectedData = new byte[Constants.NDP_NODE_COUNT][];

    // Insert random data into random slots
    for (int i = 0; i < NUM_RANDOM_INSERTIONS; i++) {
      int slotNumber = random.nextInt(Constants.NDP_NODE_COUNT);
      int dataSize = random.nextInt(100) + 1; // Data size between 1 and 100 bytes
      byte[] data = new byte[dataSize];
      random.nextBytes(data);

      originalPage.setSlot(data, slotNumber);
      expectedData[slotNumber] = data; // Keep track of the expected data for verification

      // Randomly overwrite some slots with different sizes
      if (random.nextBoolean()) {
        dataSize = random.nextInt(100) + 1; // New data size between 1 and 100 bytes
        data = new byte[dataSize];
        random.nextBytes(data);

        originalPage.setSlot(data, slotNumber);
        expectedData[slotNumber] = data; // Update the expected data
      }
    }

    // Serialize the page
    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    // Deserialize the page
    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    // Verify the deserialized data matches the original
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      assertArrayEquals(expectedData[i], deserializedPage.getSlotAsByteArray(i), "Mismatch at slot " + i);
    }
  }

  @Test
  public void testBasicSerialization() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(1),
                                                         null,
                                                         -1,
                                                         -1);

    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    assertEquals(originalPage.getPageKey(), deserializedPage.getPageKey());
    assertEquals(originalPage.getRevision(), deserializedPage.getRevision());
    assertEquals(originalPage.getIndexType(), deserializedPage.getIndexType());
  }

  @Test
  public void testSlotsSerialization() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(1000),
                                                         null,
                                                         -1,
                                                         -1);
    originalPage.setSlot(new byte[] { 1, 2, 3 }, 1);
    originalPage.setSlot(new byte[] { 4, 5, 6 }, 10);
    originalPage.setSlot(new byte[] { 7, 8, 9 }, 100);

    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    assertArrayEquals(originalPage.getSlotAsByteArray(1), deserializedPage.getSlotAsByteArray(1));
    assertArrayEquals(originalPage.getSlotAsByteArray(10), deserializedPage.getSlotAsByteArray(10));
    assertArrayEquals(originalPage.getSlotAsByteArray(100), deserializedPage.getSlotAsByteArray(100));
  }

  @Test
  public void testDeweyIdSerialization() {
    ResourceConfiguration configWithDeweyIDs = createResourceConfigurationWithDeweyIDs();
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         configWithDeweyIDs,
                                                         true,
                                                         new NodeSerializerImpl(),
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(1000),
                                                         pagePool.getSegmentAllocator().allocate(1000),
                                                         -1,
                                                         -1);
    originalPage.setDeweyId(new byte[] { 0, 1, 2 }, 2);
    originalPage.setDeweyId(new byte[] { 3, 4, 5 }, 4);

    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(configWithDeweyIDs, sink, originalPage, type);

    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(configWithDeweyIDs, source, type);

    assertArrayEquals(originalPage.getDeweyIdAsByteArray(2), deserializedPage.getDeweyIdAsByteArray(2));
    assertArrayEquals(originalPage.getDeweyIdAsByteArray(4), deserializedPage.getDeweyIdAsByteArray(4));
  }

  @Test
  public void testEmptyPageSerialization() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(1),
                                                         null,
                                                         -1,
                                                         -1);

    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    assertEquals(originalPage.getPageKey(), deserializedPage.getPageKey());
    assertEquals(originalPage.getRevision(), deserializedPage.getRevision());
    assertEquals(originalPage.getIndexType(), deserializedPage.getIndexType());
  }

  @Test
  public void testMaxSlotsSerialization() {
    KeyValueLeafPage originalPage = new KeyValueLeafPage(1,
                                                         0,
                                                         IndexType.DOCUMENT,
                                                         config,
                                                         false,
                                                         null,
                                                         new LinkedHashMap<>(),
                                                         pagePool.getSegmentAllocator().allocate(10000),
                                                         null,
                                                         0,
                                                         0);

    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      originalPage.setSlot(new byte[] { (byte) i }, i);
    }

    BytesOut<?> sink = Bytes.elasticByteBuffer();
    SerializationType type = SerializationType.DATA;
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, originalPage, type);

    BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    KeyValueLeafPage deserializedPage =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, type);

    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      assertArrayEquals(originalPage.getSlotAsByteArray(i), deserializedPage.getSlotAsByteArray(i));
    }
  }
}