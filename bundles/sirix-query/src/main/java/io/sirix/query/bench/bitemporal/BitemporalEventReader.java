package io.sirix.query.bench.bitemporal;

import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Ordered, single-record publication/resource window; advancing overwrites all input state. */
final class BitemporalEventReader implements AutoCloseable {
  private final Tokens reader;
  private final Tier tier;
  private long count;
  private int previousEpoch = -1;
  private int previousTable = -1;
  private int previousId;
  private boolean available;
  private long parseNanos;
  int epoch;
  String table;
  boolean put;
  int id;
  int from;
  int to;
  final int[] payload = new int[5];

  BitemporalEventReader(final Path path, final Tier tier) throws IOException {
    this(Files.newBufferedReader(path, StandardCharsets.UTF_8), tier);
  }

  BitemporalEventReader(final Reader input, final Tier tier) {
    reader = new Tokens(input);
    this.tier = tier;
  }

  long parseNanos() {
    return parseNanos;
  }

  long count() {
    return count;
  }

  boolean matches(final int publication, final String resource) {
    return available && epoch == publication && table.equals(resource);
  }

  boolean advance() throws IOException {
    final long start = System.nanoTime();
    try {
      return read();
    } finally {
      parseNanos += System.nanoTime() - start;
    }
  }

  private boolean read() throws IOException {
    available = false;
    if (reader.peek() < 0) {
      final long expected =
          (long) tier.contracts() + tier.products() + tier.suppliers() + 24L * ((tier.contracts() + 19L) / 20L)
              + 24L * ((tier.products() + 99L) / 100L) + 24L * ((tier.suppliers() + 99L) / 100L) + 4L;
      if (count != expected) {
        throw new IllegalArgumentException("event count " + count + " != tier expectation " + expected);
      }
      return false;
    }
    Arrays.fill(payload, 0);
    int fields = 0;
    int tableCode = -1;
    reader.expect('{');
    while (reader.peek() != '}') {
      if (fields != 0) {
        reader.expect(',');
      }
      final int field = reader.field();
      reader.expect(':');
      final int bit;
      switch (field) {
        case 0 -> {
          epoch = reader.nextInt();
          bit = 1;
        }
        case 1 -> {
          reader.string();
          if (reader.matches(BitemporalSchema.CONTRACTS)) {
            table = BitemporalSchema.CONTRACTS;
          } else if (reader.matches(BitemporalSchema.PRODUCTS)) {
            table = BitemporalSchema.PRODUCTS;
          } else if (reader.matches(BitemporalSchema.SUPPLIERS)) {
            table = BitemporalSchema.SUPPLIERS;
          } else {
            throw new IllegalArgumentException("unknown event table");
          }
          tableCode = table == BitemporalSchema.CONTRACTS
              ? 0
              : table == BitemporalSchema.PRODUCTS
                  ? 1
                  : 2;
          bit = 2;
        }
        case 2 -> {
          reader.string();
          put = reader.matches("put");
          if (!put && !reader.matches("delete")) {
            throw new IllegalArgumentException("unknown event operation");
          }
          bit = 4;
        }
        case 3 -> {
          id = reader.nextInt();
          bit = 8;
        }
        case 4 -> {
          from = reader.nextInt();
          bit = 16;
        }
        case 5 -> {
          to = reader.nextInt();
          bit = 32;
        }
        case 6 -> {
          payload[0] = reader.nextInt();
          bit = 64;
        }
        case 7 -> {
          payload[1] = reader.nextInt();
          bit = 128;
        }
        case 8 -> {
          payload[2] = reader.nextInt();
          bit = 256;
        }
        case 9 -> {
          payload[3] = reader.nextInt();
          bit = 512;
        }
        case 10 -> {
          payload[4] = reader.nextInt();
          bit = 1024;
        }
        case 11 -> {
          payload[0] = reader.nextInt();
          bit = 2048;
        }
        case 12 -> {
          payload[1] = reader.nextInt();
          bit = 4096;
        }
        case 13 -> {
          payload[0] = reader.nextInt();
          bit = 8192;
        }
        case 14 -> {
          payload[1] = reader.nextInt();
          bit = 16384;
        }
        default -> throw new IllegalArgumentException("unknown event field");
      }
      if ((fields & bit) != 0) {
        throw new IllegalArgumentException("duplicate event field: " + field);
      }
      fields |= bit;
    }
    reader.expect('}');
    final int required = 63 | (!put
        ? 0
        : switch (tableCode) {
          case 0 -> 1984;
          case 1 -> 6144;
          case 2 -> 24576;
          default -> 0;
        });
    if (fields != required || epoch < 0 || epoch >= BitemporalSchema.PUBLICATIONS || id < 1 || from < 0 || from >= to
        || to > BitemporalSchema.HORIZON_DAYS) {
      throw new IllegalArgumentException("invalid event at record " + (count + 1));
    }
    final int identities = switch (tableCode) {
      case 0 -> tier.contracts();
      case 1 -> tier.products();
      case 2 -> tier.suppliers();
      default -> throw new IllegalArgumentException("missing event table");
    };
    if (id > identities || epoch < previousEpoch
        || epoch == previousEpoch && (tableCode < previousTable || tableCode == previousTable && id <= previousId)) {
      throw new IllegalArgumentException("event stream is not strictly ordered/in range at record " + (count + 1));
    }
    previousEpoch = epoch;
    previousTable = tableCode;
    previousId = id;
    count++;
    available = true;
    return true;
  }

  void requireEnd() {
    if (available) {
      throw new IllegalStateException("unconsumed input at record " + count);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  /** Canonical SH1 uses bounded ASCII names and integer values; tokens reuse fixed buffers. */
  private static final class Tokens implements AutoCloseable {
    private static final String[] FIELDS = {"epoch", "table", "op", "id", "a", "b", "pid", "sid", "cost", "qty",
        "grade", "category", "retail", "region", "tier"};
    private final Reader input;
    private final char[] buffer = new char[16 * 1024];
    private final char[] token = new char[32];
    private int position;
    private int limit;
    private int tokenLength;

    Tokens(final Reader input) {
      this.input = input;
    }

    private int raw() throws IOException {
      if (position == limit) {
        limit = input.read(buffer);
        position = 0;
        if (limit < 0) {
          limit = 0;
          return -1;
        }
      }
      return buffer[position];
    }

    int peek() throws IOException {
      int value;
      while ((value = raw()) == ' ' || value == '\n' || value == '\r' || value == '\t') {
        position++;
      }
      return value;
    }

    void expect(final int expected) throws IOException {
      if (peek() != expected) {
        throw new IllegalArgumentException("invalid event JSON: expected " + (char) expected);
      }
      position++;
    }

    int field() throws IOException {
      string();
      for (int field = 0; field < FIELDS.length; field++) {
        if (matches(FIELDS[field])) {
          return field;
        }
      }
      throw new IllegalArgumentException("unknown event field");
    }

    void string() throws IOException {
      expect('"');
      tokenLength = 0;
      int value;
      while ((value = raw()) != '"') {
        if (value < 32 || tokenLength == token.length) {
          throw new IllegalArgumentException("invalid or oversized event string");
        }
        position++;
        if (value == '\\') {
          value = raw();
          position++;
          if (value == 'u') {
            value = 0;
            for (int digit = 0; digit < 4; digit++) {
              final int hex = Character.digit(raw(), 16);
              if (hex < 0) {
                throw new IllegalArgumentException("invalid unicode escape");
              }
              position++;
              value = (value << 4) | hex;
            }
          } else if (value != '"' && value != '\\' && value != '/') {
            throw new IllegalArgumentException("invalid event name escape");
          }
        }
        token[tokenLength++] = (char) value;
      }
      position++;
    }

    boolean matches(final String expected) {
      if (expected.length() != tokenLength) {
        return false;
      }
      for (int i = 0; i < tokenLength; i++) {
        if (token[i] != expected.charAt(i)) {
          return false;
        }
      }
      return true;
    }

    int nextInt() throws IOException {
      int value = peek();
      final boolean negative = value == '-';
      if (negative) {
        position++;
        value = raw();
      }
      if (value < '0' || value > '9') {
        throw new IllegalArgumentException("integer expected");
      }
      final boolean zero = value == '0';
      long result = 0;
      int digits = 0;
      do {
        result = result * 10 + value - '0';
        if (++digits > 10 || result > (negative
            ? 2147483648L
            : Integer.MAX_VALUE) || zero && digits > 1) {
          throw new IllegalArgumentException("invalid event integer");
        }
        position++;
        value = raw();
      } while (value >= '0' && value <= '9');
      return (int) (negative
          ? -result
          : result);
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

}
