package io.sirix.query.bench.bitemporal;

import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

/** Generates the canonical, deterministic Supply History 1 event stream. */
public final class BitemporalGenerateMain {

  private static final byte PIPE = (byte) '|';
  private static final Path REQUIRED_ROOT = Path.of("/var/tmp/sirix-bitemporal");

  private BitemporalGenerateMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: BitemporalGenerateMain <development|t25k|t100k> <output-directory>");
      System.exit(2);
      return;
    }
    final Tier tier = Tier.parse(args[0]);
    final Path output = requireCampaignPath(Path.of(args[1]));
    Files.createDirectories(output);
    final Path events = output.resolve("events.jsonl");
    final Path manifestPath = output.resolve("manifest.json");
    if (Files.exists(events) || Files.exists(manifestPath)) {
      throw new IllegalArgumentException("refusing to overwrite an existing SH1 stream under " + output);
    }
    final Generator generator = new Generator(tier, events);
    final Manifest manifest = generator.generate();
    Files.writeString(manifestPath, manifest.toJson(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    System.out.printf("generated tier=%s events=%d puts=%d deletes=%d sha256=%s path=%s%n", tier.label(),
        manifest.events(), manifest.puts(), manifest.deletes(), manifest.sha256(), events);
  }

  private static Path requireCampaignPath(final Path path) {
    final Path normalized = path.toAbsolutePath().normalize();
    if (!normalized.startsWith(REQUIRED_ROOT)) {
      throw new IllegalArgumentException("output directory must be under " + REQUIRED_ROOT + ": " + normalized);
    }
    return normalized;
  }

  record Manifest(Tier tier, long events, long puts, long deletes, String sha256) {
    String toJson() {
      return "{\"format\":\"sh1-events-v1\",\"seed\":" + BitemporalSchema.SEED + ",\"tier\":\"" + tier.label()
          + "\",\"contracts\":" + tier.contracts() + ",\"products\":" + tier.products() + ",\"suppliers\":"
          + tier.suppliers() + ",\"publications\":" + BitemporalSchema.PUBLICATIONS + ",\"events\":" + events
          + ",\"puts\":" + puts + ",\"deletes\":" + deletes + ",\"sha256\":\"" + sha256 + "\"}\n";
    }
  }

  private static final class Generator {
    private final Tier tier;
    private final Path eventsPath;
    private final StableHash hash;
    private final MessageDigest streamDigest;
    private long eventCount;
    private long putCount;
    private long deleteCount;

    Generator(final Tier tier, final Path eventsPath) throws NoSuchAlgorithmException {
      this.tier = tier;
      this.eventsPath = eventsPath;
      hash = new StableHash();
      streamDigest = MessageDigest.getInstance("SHA-256");
    }

    Manifest generate() throws IOException {
      try (BufferedWriter writer = Files.newBufferedWriter(eventsPath, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
        writeInitial(writer);
        for (int epoch = 1; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
          writeEpoch(writer, epoch);
        }
      }
      final long expected =
          (long) tier.contracts() + tier.products() + tier.suppliers() + 24L * ((tier.contracts() + 19L) / 20L)
              + 24L * ((tier.products() + 99L) / 100L) + 24L * ((tier.suppliers() + 99L) / 100L) + 4L;
      if (eventCount != expected) {
        throw new IllegalStateException("event count " + eventCount + " != expected " + expected);
      }
      if (tier == Tier.DEVELOPMENT && eventCount != 4_776L) {
        throw new IllegalStateException("development stream must contain 4,776 business events");
      }
      if (tier == Tier.T100K && eventCount != 234_884L) {
        throw new IllegalStateException("T100k stream must contain 234,884 business events");
      }
      return new Manifest(tier, eventCount, putCount, deleteCount, HexFormat.of().formatHex(streamDigest.digest()));
    }

    private void writeInitial(final BufferedWriter writer) throws IOException {
      for (int id = 1; id <= tier.contracts(); id++) {
        writeContract(writer, 0, id, 0, BitemporalSchema.HORIZON_DAYS, initialProduct(id), initialSupplier(id),
            initialContractCost(id), initialContractQty(id),
            mod(hash.value(BitemporalSchema.CONTRACTS, 0, id, "grade"), 4));
      }
      for (int id = 1; id <= tier.products(); id++) {
        writeProduct(writer, 0, id, 0, BitemporalSchema.HORIZON_DAYS,
            mod(hash.value(BitemporalSchema.PRODUCTS, 0, id, "category"), 16),
            150_000 + mod(hash.value(BitemporalSchema.PRODUCTS, 0, id, "retail"), 50_001));
      }
      for (int id = 1; id <= tier.suppliers(); id++) {
        writeSupplier(writer, 0, id, 0, BitemporalSchema.HORIZON_DAYS,
            mod(hash.value(BitemporalSchema.SUPPLIERS, 0, id, "region"), 8),
            1 + mod(hash.value(BitemporalSchema.SUPPLIERS, 0, id, "tier"), 3));
      }
    }

    private void writeEpoch(final BufferedWriter writer, final int epoch) throws IOException {
      if (epoch == 6) {
        writeContract(writer, epoch, 1, 90, 210, initialProduct(1), initialSupplier(1), initialContractCost(1) + 100,
            initialContractQty(1), initialContractGrade(1));
      } else if (epoch == 12) {
        writeContract(writer, epoch, 1, 150, 180, initialProduct(1), initialSupplier(1), initialContractCost(1) + 200,
            initialContractQty(1), initialContractGrade(1));
      } else if (epoch == 18) {
        writeDelete(writer, epoch, BitemporalSchema.CONTRACTS, 1, 160, 170);
      } else if (epoch == 20) {
        writeContract(writer, epoch, 1, 164, 168, initialProduct(1), initialSupplier(1), initialContractCost(1) + 300,
            initialContractQty(1), initialContractGrade(1));
      }

      final int[] contracts =
          select(BitemporalSchema.CONTRACTS, epoch, 2, tier.contracts(), (tier.contracts() + 19) / 20);
      for (final int id : contracts) {
        final int from = intervalStart(BitemporalSchema.CONTRACTS, epoch, id);
        final int to = intervalEnd(BitemporalSchema.CONTRACTS, epoch, id, from);
        if (mod(hash.value(BitemporalSchema.CONTRACTS, epoch, id, "op"), 5) == 0) {
          writeDelete(writer, epoch, BitemporalSchema.CONTRACTS, id, from, to);
        } else {
          writeContract(writer, epoch, id, from, to, initialProduct(id), initialSupplier(id),
              10_000 + mod(hash.value(BitemporalSchema.CONTRACTS, epoch, id, "cost"), 90_001),
              1 + mod(hash.value(BitemporalSchema.CONTRACTS, epoch, id, "qty"), 1_000), initialContractGrade(id));
        }
      }

      final int[] products = select(BitemporalSchema.PRODUCTS, epoch, 1, tier.products(), (tier.products() + 99) / 100);
      for (final int id : products) {
        final int from = intervalStart(BitemporalSchema.PRODUCTS, epoch, id);
        writeProduct(writer, epoch, id, from, intervalEnd(BitemporalSchema.PRODUCTS, epoch, id, from),
            mod(hash.value(BitemporalSchema.PRODUCTS, epoch, id, "category"), 16),
            150_000 + mod(hash.value(BitemporalSchema.PRODUCTS, epoch, id, "retail"), 50_001));
      }

      final int[] suppliers =
          select(BitemporalSchema.SUPPLIERS, epoch, 1, tier.suppliers(), (tier.suppliers() + 99) / 100);
      for (final int id : suppliers) {
        final int from = intervalStart(BitemporalSchema.SUPPLIERS, epoch, id);
        writeSupplier(writer, epoch, id, from, intervalEnd(BitemporalSchema.SUPPLIERS, epoch, id, from),
            mod(hash.value(BitemporalSchema.SUPPLIERS, epoch, id, "region"), 8),
            1 + mod(hash.value(BitemporalSchema.SUPPLIERS, epoch, id, "tier"), 3));
      }
    }

    private int initialProduct(final int id) {
      return 1 + (id - 1) % tier.products();
    }

    private int initialSupplier(final int id) {
      return 1 + (17 * (id - 1)) % tier.suppliers();
    }

    private int initialContractCost(final int id) {
      return 10_000 + mod(hash.value(BitemporalSchema.CONTRACTS, 0, id, "cost"), 90_001);
    }

    private int initialContractQty(final int id) {
      return 1 + mod(hash.value(BitemporalSchema.CONTRACTS, 0, id, "qty"), 1_000);
    }

    private int initialContractGrade(final int id) {
      return mod(hash.value(BitemporalSchema.CONTRACTS, 0, id, "grade"), 4);
    }

    private int intervalStart(final String table, final int epoch, final int id) {
      if (mod(hash.value(table, epoch, id, "direction"), 5) == 0) {
        return Math.min(365, 15 * epoch + 1 + mod(hash.value(table, epoch, id, "lag"), 30));
      }
      return Math.max(0, 15 * epoch - 1 - mod(hash.value(table, epoch, id, "lag"), 120));
    }

    private int intervalEnd(final String table, final int epoch, final int id, final int from) {
      return Math.min(BitemporalSchema.HORIZON_DAYS, from + 1 + mod(hash.value(table, epoch, id, "length"), 90));
    }

    private int[] select(final String table, final int epoch, final int firstId, final int lastId, final int count) {
      final UnsignedMaxHeap heap = new UnsignedMaxHeap(count);
      for (int id = firstId; id <= lastId; id++) {
        heap.offer(hash.value(table, epoch, id, "pick"), id);
      }
      final int[] ids = heap.ids();
      Arrays.sort(ids);
      return ids;
    }

    private void writeContract(final BufferedWriter writer, final int epoch, final int id, final int from, final int to,
        final int pid, final int sid, final int cost, final int qty, final int grade) throws IOException {
      write(writer,
          "{\"epoch\":" + epoch + ",\"table\":\"contracts\",\"op\":\"put\",\"id\":" + id + ",\"a\":" + from + ",\"b\":"
              + to + ",\"pid\":" + pid + ",\"sid\":" + sid + ",\"cost\":" + cost + ",\"qty\":" + qty + ",\"grade\":"
              + grade + "}\n",
          true);
    }

    private void writeProduct(final BufferedWriter writer, final int epoch, final int id, final int from, final int to,
        final int category, final int retail) throws IOException {
      write(writer, "{\"epoch\":" + epoch + ",\"table\":\"products\",\"op\":\"put\",\"id\":" + id + ",\"a\":" + from
          + ",\"b\":" + to + ",\"category\":" + category + ",\"retail\":" + retail + "}\n", true);
    }

    private void writeSupplier(final BufferedWriter writer, final int epoch, final int id, final int from, final int to,
        final int region, final int supplierTier) throws IOException {
      write(writer, "{\"epoch\":" + epoch + ",\"table\":\"suppliers\",\"op\":\"put\",\"id\":" + id + ",\"a\":" + from
          + ",\"b\":" + to + ",\"region\":" + region + ",\"tier\":" + supplierTier + "}\n", true);
    }

    private void writeDelete(final BufferedWriter writer, final int epoch, final String table, final int id,
        final int from, final int to) throws IOException {
      write(writer, "{\"epoch\":" + epoch + ",\"table\":\"" + table + "\",\"op\":\"delete\",\"id\":" + id + ",\"a\":"
          + from + ",\"b\":" + to + "}\n", false);
    }

    private void write(final BufferedWriter writer, final String line, final boolean put) throws IOException {
      writer.write(line);
      streamDigest.update(line.getBytes(StandardCharsets.UTF_8));
      eventCount++;
      if (put) {
        putCount++;
      } else {
        deleteCount++;
      }
    }
  }

  private static int mod(final long value, final int divisor) {
    return (int) Long.remainderUnsigned(value, divisor);
  }

  /** Reuses one digest and one decimal buffer; hash evaluation is the generator's hot path. */
  private static final class StableHash {
    private static final byte[] SEED_PREFIX = "20260920|".getBytes(StandardCharsets.UTF_8);
    private final MessageDigest digest;
    private final byte[] decimal = new byte[20];

    StableHash() throws NoSuchAlgorithmException {
      digest = MessageDigest.getInstance("SHA-256");
    }

    long value(final String table, final int epoch, final int id, final String purpose) {
      digest.reset();
      digest.update(SEED_PREFIX);
      digest.update(table.getBytes(StandardCharsets.UTF_8));
      digest.update(PIPE);
      updateDecimal(epoch);
      digest.update(PIPE);
      updateDecimal(id);
      digest.update(PIPE);
      digest.update(purpose.getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(digest.digest(), 0, Long.BYTES).getLong();
    }

    private void updateDecimal(final int value) {
      int cursor = decimal.length;
      int remaining = value;
      do {
        decimal[--cursor] = (byte) ('0' + remaining % 10);
        remaining /= 10;
      } while (remaining != 0);
      digest.update(decimal, cursor, decimal.length - cursor);
    }
  }

  /** Fixed-capacity primitive heap retaining the smallest unsigned (hash,id) pairs. */
  private static final class UnsignedMaxHeap {
    private final long[] hashes;
    private final int[] ids;
    private int size;

    UnsignedMaxHeap(final int capacity) {
      if (capacity < 1) {
        throw new IllegalArgumentException("capacity must be positive");
      }
      hashes = new long[capacity];
      ids = new int[capacity];
    }

    void offer(final long hash, final int id) {
      if (size < hashes.length) {
        hashes[size] = hash;
        ids[size] = id;
        siftUp(size++);
      } else if (less(hash, id, hashes[0], ids[0])) {
        hashes[0] = hash;
        ids[0] = id;
        siftDown(0);
      }
    }

    int[] ids() {
      if (size != ids.length) {
        throw new IllegalStateException("selection heap was not filled");
      }
      return ids.clone();
    }

    private void siftUp(int child) {
      while (child > 0) {
        final int parent = (child - 1) >>> 1;
        if (!less(hashes[parent], ids[parent], hashes[child], ids[child])) {
          return;
        }
        swap(parent, child);
        child = parent;
      }
    }

    private void siftDown(int parent) {
      while (true) {
        final int left = (parent << 1) + 1;
        if (left >= size) {
          return;
        }
        final int right = left + 1;
        int greater = left;
        if (right < size && less(hashes[left], ids[left], hashes[right], ids[right])) {
          greater = right;
        }
        if (!less(hashes[parent], ids[parent], hashes[greater], ids[greater])) {
          return;
        }
        swap(parent, greater);
        parent = greater;
      }
    }

    private void swap(final int first, final int second) {
      final long hash = hashes[first];
      hashes[first] = hashes[second];
      hashes[second] = hash;
      final int id = ids[first];
      ids[first] = ids[second];
      ids[second] = id;
    }

    private static boolean less(final long leftHash, final int leftId, final long rightHash, final int rightId) {
      final int comparison = Long.compareUnsigned(leftHash, rightHash);
      return comparison < 0 || comparison == 0 && leftId < rightId;
    }
  }
}
