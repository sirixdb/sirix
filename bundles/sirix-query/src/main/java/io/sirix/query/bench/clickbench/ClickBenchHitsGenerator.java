package io.sirix.query.bench.clickbench;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Reader} over a synthetic ClickBench {@code hits} dataset, emitted as one JSON array of
 * objects in the encoding {@link ClickBenchSchema} defines — all 105 columns, always present, in
 * {@code create.sql} order.
 *
 * <p>
 * It exists so the whole port runs offline: the official dataset is a 14 GB parquet download, and a
 * benchmark that cannot be reproduced without it is a benchmark nobody re-runs. The generator is
 * <em>not</em> a claim about ClickBench numbers — the shipped results must come from the real data
 * — but it is the dataset the correctness harness, the differential tests and every smoke run use.
 *
 * <h2>Determinism</h2> The record text for row {@code i} is a pure function of {@code (seed, i)}:
 * every row seeds its own splitmix64 stream from those two values, so
 * <ul>
 * <li>the same {@code (firstRow, rowCount, seed)} always produces byte-identical output, and</li>
 * <li>rows {@code [0, n)} generated in one go hold exactly the records of {@code [0, k)} followed
 * by those of {@code [k, n)} with the same seed — the framing brackets differ, the record bodies do
 * not.</li>
 * </ul>
 * That is what lets a loader shard the ingest across threads or processes and still get the dataset
 * a single-threaded run would have produced.
 *
 * <h2>Planted literals</h2> Three ClickBench queries select a hard-coded 64-bit literal that only
 * exists in the real data. Without planting them, those queries return the empty result on
 * synthetic input and quietly stop measuring anything:
 * <ul>
 * <li>Q19 — {@code UserID} = {@value #PLANTED_USER_ID}, on 1 row in
 * {@value #PLANTED_USER_ID_ONE_IN};</li>
 * <li>Q40 — {@code RefererHash} = {@value #PLANTED_REFERER_HASH}, on 1 row in
 * {@value #PLANTED_REFERER_HASH_ONE_IN};</li>
 * <li>Q41 — {@code URLHash} = {@value #PLANTED_URL_HASH}, on 1 row in
 * {@value #PLANTED_URL_HASH_ONE_IN}.</li>
 * </ul>
 *
 * <h2>Shape of the data</h2> The distributions are chosen so the 43 queries do real work rather
 * than folding a constant:
 * <ul>
 * <li>{@code CounterID} is {@value #MAIN_COUNTER_ID} on ~35 % of the rows (Q36..Q42 filter on it);
 * a quarter of the rows go uniformly to sixteen big counters and the remainder is a square-skewed
 * draw over the rest of {@value #COUNTER_ID_SPACE}, so about twenty counters clear Q27's
 * {@code HAVING COUNT(*) > 100000} at 10M rows instead of three.</li>
 * <li>{@code EventDate} covers {@value #FIRST_EVENT_DATE}..{@value #LAST_EVENT_DATE} with
 * 2013-07-14 and 2013-07-15 over-represented (~14 % each) for Q42's two-day window.
 * {@code EventTime} is that same day plus a uniform time of day, and
 * {@code ClientEventTime}/{@code LocalEventTime} sit within minutes of it.</li>
 * <li>{@code UserID} is a cube-skewed draw over {@value #USER_ID_SPACE} ranks: a handful of heavy
 * hitters (the top rank takes ~1 % of all rows) over a long tail, which is what makes the
 * {@code GROUP BY UserID} queries and the {@code COUNT(DISTINCT UserID)} ones non-trivial.</li>
 * <li>{@code URL}/{@code Referer} are built from {@value #HOST_COUNT} hosts and
 * {@value #PATH_COUNT} paths; ~15 % of the URLs contain {@code google} and a subset of those
 * contain {@code .google.} (Q20..Q22 need both to be true of a non-trivial slice).
 * {@code URLHash}/{@code RefererHash} are hashes of those components, so equal locations hash
 * equally and Q40's {@code GROUP BY URLHash, EventDate} sees repeated keys.</li>
 * <li>{@code SearchPhrase} is empty on ~85 % of rows, otherwise a skewed draw over
 * {@value #SEARCH_PHRASE_COUNT} phrases; {@code MobilePhoneModel} is empty on ~95 %.</li>
 * </ul>
 * One query is scale-dependent: Q41 pages past 10 000 {@code (WindowClientWidth,
 * WindowClientHeight)} groups inside the slice a single {@code URLHash} selects. The window-size
 * domain has ~20 000 distinct pairs, so the query needs a dataset big enough for that slice to
 * cover more than 10 000 of them — around 100M rows. Below that it returns the empty result,
 * exactly as a paging query past the end of its group list should.
 *
 * <h2>Performance</h2> The generator sits on the ingest path, so the steady state allocates
 * nothing: one reused {@link StringBuilder} per instance, pooled strings pre-escaped once at
 * class-initialisation time, primitive locals throughout, no {@code String.format}, and numbers
 * written straight into the builder by {@code append(long)} (which is also what guarantees exact
 * {@code int64} digits instead of a float's scientific notation).
 *
 * <p>
 * Instances are not thread-safe; give each thread its own generator over its own row range.
 */
public final class ClickBenchHitsGenerator extends Reader {

  /** The {@code UserID} Q19 selects; planted on ~1 in {@value #PLANTED_USER_ID_ONE_IN} rows. */
  public static final long PLANTED_USER_ID = 435090932899640449L;

  /** How rare {@link #PLANTED_USER_ID} is: one row in this many carries it. */
  public static final int PLANTED_USER_ID_ONE_IN = 5000;

  /** The {@code RefererHash} Q40 selects; planted on ~1 in {@value #PLANTED_REFERER_HASH_ONE_IN}. */
  public static final long PLANTED_REFERER_HASH = 3594120000172545465L;

  /** How rare {@link #PLANTED_REFERER_HASH} is: one row in this many carries it. */
  public static final int PLANTED_REFERER_HASH_ONE_IN = 1000;

  /** The {@code URLHash} Q41 selects; planted on ~1 in {@value #PLANTED_URL_HASH_ONE_IN} rows. */
  public static final long PLANTED_URL_HASH = 2868770270353813622L;

  /** How rare {@link #PLANTED_URL_HASH} is: one row in this many carries it. */
  public static final int PLANTED_URL_HASH_ONE_IN = 500;

  /** The counter Q36..Q42 filter on. */
  public static final int MAIN_COUNTER_ID = 62;

  /** Share of rows carrying {@link #MAIN_COUNTER_ID}, in per mille. */
  public static final int MAIN_COUNTER_PER_MILLE = 350;

  /** First {@code EventDate} in the generated month, inclusive. */
  public static final String FIRST_EVENT_DATE = "2013-07-01";

  /** Last {@code EventDate} in the generated month, inclusive. */
  public static final String LAST_EVENT_DATE = "2013-07-31";

  /**
   * Number of distinct {@code UserID} ranks the skewed draw picks from. The reference scale is 10M
   * rows, where a cube-skewed draw over this many ranks realises roughly {@code rowCount / 8}
   * distinct users; beyond ~10M rows the distinct count saturates here instead of growing with the
   * dataset, which is the one place the synthetic data is knowingly less rich than the real one.
   */
  public static final int USER_ID_SPACE = 1 << 20;

  /** Number of distinct {@code CounterID}s besides {@link #MAIN_COUNTER_ID}. */
  public static final int COUNTER_ID_SPACE = 512;

  /**
   * How many counters below {@link #MAIN_COUNTER_ID} are drawn uniformly rather than from the skewed
   * tail. Without this tier the skew is so steep that only three counters clear Q27's
   * {@code HAVING COUNT(*) > 100000} at 10M rows, and a {@code LIMIT 25} would return three rows.
   */
  private static final int COUNTER_ID_BIG_TIER = 16;

  /** Share of rows drawn from the big-counter tier, in per mille. */
  private static final int COUNTER_ID_BIG_TIER_PER_MILLE = 250;

  // ── the JSON framing ──────────────────────────────────────────────────────────────────────────

  /** Between two records; the newline keeps the file greppable without changing the JSON. */
  private static final String RECORD_SEPARATOR = ",\n";

  /** Records are generated until the buffer holds at least this many chars, then handed out. */
  private static final int REFILL_TARGET_CHARS = 1 << 15;

  /**
   * Builder capacity: the refill target plus room for the record that crosses it, so it never grows.
   */
  private static final int RECORD_BUFFER_CHARS = REFILL_TARGET_CHARS + 8192;

  // ── string pools, escaped once ────────────────────────────────────────────────────────────────

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /** Two-digit decimal pairs "00".."99" as one char array, for zero-padded time fields. */
  private static final char[] TWO_DIGITS = twoDigitTable();

  /** The escaped body of the empty string, i.e. the empty body of {@code ""}. */
  private static final String EMPTY = "";

  private static final String SCHEME_HTTP = "http://";
  private static final String SCHEME_HTTPS = "https://";
  private static final String QUERY_ID = "?id=";

  /** No {@code ?id=} suffix on this URL. */
  private static final int NO_ID = -1;

  /** Upper bound (exclusive) of the {@code ?id=} suffix. */
  private static final int URL_ID_SPACE = 1000;

  private static final int SECONDS_PER_DAY = 86_400;

  /** Hosts whose name contains {@code google}; four of them also contain {@code .google.}. */
  private static final String[] GOOGLE_HOSTS = escapeAll("google.com", "www.google.de", "shop.google.com",
      "mail.google.com", "images.google.ru", "google.co.uk", "googleusercontent.com", "news.google.fr");

  /** Everything else; a plain mix of portals, shops and news sites. */
  private static final String[] OTHER_HOSTS = escapeAll("yandex.ru", "www.yandex.ru", "market.yandex.ru", "mail.ru",
      "www.mail.ru", "vk.com", "m.vk.com", "ok.ru", "avito.ru", "www.avito.ru", "auto.ru", "lenta.ru", "rbc.ru",
      "ria.ru", "gazeta.ru", "kinopoisk.ru", "rutube.ru", "youtube.com", "www.youtube.com", "facebook.com",
      "twitter.com", "instagram.com", "wikipedia.org", "ru.wikipedia.org", "en.wikipedia.org", "amazon.com",
      "www.amazon.de", "ebay.com", "aliexpress.com", "booking.com", "tripadvisor.com", "github.com",
      "stackoverflow.com", "reddit.com", "bbc.co.uk", "cnn.com", "nytimes.com", "spiegel.de", "zeit.de", "heise.de",
      "golem.de", "chip.de", "otto.de", "zalando.de", "idealo.de", "check24.de", "mobile.de", "autoscout24.de",
      "web.de", "gmx.net", "t-online.de", "wetter.com", "bild.de", "focus.de", "welt.de", "sueddeutsche.de");

  /** All hosts, google ones first; {@link #pickHost()} weights the two blocks. */
  private static final String[] HOSTS = concat(GOOGLE_HOSTS, OTHER_HOSTS);

  /** Size of the host pool. */
  public static final int HOST_COUNT = 64;

  /** Share of rows whose URL host contains {@code google}, in per mille. */
  private static final int GOOGLE_HOST_PER_MILLE = 150;

  /** Size of the path pool. */
  public static final int PATH_COUNT = 32;

  private static final String[] PATHS = requireSize(escapeAll("/", "/index.html", "/catalog/", "/catalog/phones",
      "/catalog/tv", "/news/", "/news/politics", "/news/sport", "/search", "/search/results", "/product/",
      "/product/detail", "/cart", "/checkout", "/login", "/account/orders", "/blog/", "/blog/post", "/forum/thread",
      "/forum/topic", "/images/photo", "/video/watch", "/maps/place", "/help/faq", "/about", "/contact", "/download",
      "/api/v1/items", "/promo/summer", "/promo/winter", "/tag/travel", "/tag/tech"), PATH_COUNT, "paths");

  private static final String[] TITLES = buildTitles();

  private static final String[] GOOGLE_TITLES = escapeAll("Google", "Google Maps", "Google Play - Apps",
      "Search results - Google", "Google Images", "News - Google", "Google Translate", "Buy it on Google Shopping");

  private static final String[] SEARCH_PHRASES = buildSearchPhrases();

  /** Size of the search-phrase pool. */
  public static final int SEARCH_PHRASE_COUNT = 200;

  private static final String[] MOBILE_PHONE_MODELS = escapeAll("iPhone", "iPhone 4", "iPhone 5", "iPad", "iPad 2",
      "iPad mini", "Galaxy S", "Galaxy S2", "Galaxy S3", "Galaxy Note", "Galaxy Tab", "Nexus 4", "Nexus 7", "Lumia 800",
      "Lumia 920", "Xperia Z", "Xperia S", "HTC One", "HTC Desire", "Ascend P1", "Optimus G", "Razr", "Bada", "Explay",
      "Fly IQ", "Philips W", "Prestigio", "Highscreen", "Sony Tablet", "Kindle Fire");

  private static final String[] FLASH_MINOR2 = escapeAll("0", "800", "202", "500", "300", "");
  private static final String[] USER_AGENT_MINOR = escapeAll("0", "d3", "a2", "b1", "x7", "s4");
  private static final String[] PAGE_CHARSETS = escapeAll("utf-8", "windows-1251", "iso-8859-1", "");
  private static final String[] HIT_COLORS = escapeAll("5", "a", "b", "c", "d", "e", "f", "0");
  private static final String[] BROWSER_LANGUAGES =
      escapeAll("ru", "en", "de", "fr", "es", "it", "uk", "tr", "pl", "pt", "zh", "ja");
  private static final String[] BROWSER_COUNTRIES =
      escapeAll("ru", "us", "de", "gb", "fr", "ua", "by", "kz", "tr", "pl");
  private static final String[] SOCIAL_NETWORKS = escapeAll("vkontakte", "facebook", "odnoklassniki", "twitter");
  private static final String[] SOCIAL_ACTIONS = escapeAll("share", "like", "post");
  private static final String[] SOCIAL_SOURCE_PAGES = escapeAll("/wall", "/feed", "/group");
  private static final String[] PARAMS = escapeAll("utm_source=direct", "from=widget", "ref=main");
  private static final String[] PARAM_CURRENCIES = escapeAll("RUR", "USD", "EUR");
  private static final String[] OPENSTAT = escapeAll("direct", "market", "partner");
  private static final String[] UTM_SOURCES = escapeAll("google", "yandex", "newsletter", "partner");
  private static final String[] UTM_MEDIUMS = escapeAll("cpc", "organic", "email", "banner");
  private static final String[] UTM_CAMPAIGNS = escapeAll("summer", "winter", "brand", "retarget");
  private static final String[] UTM_CONTENTS = escapeAll("text", "image", "video");
  private static final String[] UTM_TERMS = escapeAll("cheap", "buy", "review");
  private static final String[] FROM_TAGS = escapeAll("main", "widget", "app", "mail");
  private static final String[] ORDER_IDS = escapeAll("A-1001", "A-1002", "B-2001", "B-2002", "C-3001");

  /** {@code "2013-07-01"} .. {@code "2013-07-31"}, indexed by day of month minus one. */
  private static final String[] EVENT_DATES = buildEventDates();

  // ── numeric pools ─────────────────────────────────────────────────────────────────────────────

  private static final int[] RESOLUTION_WIDTHS =
      {1024, 1024, 1152, 1280, 1280, 1366, 1366, 1440, 1600, 1680, 1920, 1920, 2048, 2560};
  private static final int[] RESOLUTION_HEIGHTS =
      {600, 720, 768, 768, 800, 864, 900, 900, 1024, 1050, 1080, 1080, 1200, 1440};
  /**
   * Chrome differs from the screen by the window decoration, so the client area is the resolution
   * minus one of these. Sixteen of them (rather than a handful) push the number of distinct
   * {@code (WindowClientWidth, WindowClientHeight)} pairs past 20 000, which is what gives Q41's
   * {@code OFFSET 10000} something to page into once the dataset is large enough to cover them.
   */
  private static final int[] WINDOW_DELTAS = {0, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96, 112, 128, 160};
  private static final int[] RESOLUTION_DEPTHS = {24, 24, 24, 32, 16};
  private static final int[] OS_CODES = {2, 44, 45, 50, 51, 52, 10, 1, 0};
  private static final int[] USER_AGENT_CODES = {2, 3, 4, 7, 1, 0};
  private static final int[] TRAFIC_SOURCE_IDS = {-1, 0, 1, 2, 3, 6};
  private static final int[] SEARCH_ENGINE_IDS = {2, 3, 7, 12, 22, 38, 45, 62};
  private static final int[] REFERER_CATEGORY_IDS = {0, 3, 8, 10, 16, 142, 1005, 9911};
  private static final int[] CLIENT_TIME_ZONES = {-60, 0, 60, 120, 180, 240, 300, 360, 480, 600};
  private static final int[] AGES = {0, 16, 22, 26, 31, 36, 45, 55};
  private static final int[] HTTP_ERRORS = {404, 403, 500, 502};
  private static final int[] PARAM_CURRENCY_IDS = {643, 840, 978};
  private static final int[] CODE_VERSIONS = {1601, 1602, 1657, 1701, 1702};

  /** Region codes; the first few are the real heavy ones, the rest are plausible filler. */
  private static final int[] REGION_IDS = buildRegionIds();

  /** Counter ids other than {@link #MAIN_COUNTER_ID}, drawn with a square skew. */
  private static final int[] COUNTER_IDS = buildCounterIds();

  /** Client IPs that recur, so {@code GROUP BY ClientIP} has groups bigger than one. */
  private static final int[] HOT_CLIENT_IPS = buildHotInts(4096, 0x51ED270BL);

  /** Watch ids that recur, so Q32's {@code GROUP BY WatchID, ClientIP} is not all singletons. */
  private static final long[] HOT_WATCH_IDS = buildHotLongs(1024, 0x2545F4914F6CDD1DL);

  /** {@code {"WatchID":}, {,"JavaEnable":}, ...} — derived from the schema, so key order matches. */
  private static final String[] PREFIXES = buildPrefixes();

  // ── instance state ────────────────────────────────────────────────────────────────────────────

  private final long firstRow;
  private final long endRow;
  private final long seed;
  private final StringBuilder buffer = new StringBuilder(RECORD_BUFFER_CHARS);

  /** The next row to generate; {@code endRow} once every row has been emitted. */
  private long nextRow;

  /** Read position inside {@link #buffer}. */
  private int position;

  /** Index of the next column to emit inside the current record. */
  private int column;

  /** splitmix64 state, re-seeded per row from {@code (seed, row)}. */
  private long random;

  private boolean closingBracketWritten;
  private boolean closed;

  /**
   * @param firstRow index of the first row to emit; row indices seed the data, so two generators with
   *        disjoint ranges and the same seed emit disjoint slices of one dataset
   * @param rowCount how many rows to emit; {@code 0} yields the empty array {@code []}
   * @param seed dataset seed
   * @throws IllegalArgumentException if {@code firstRow} or {@code rowCount} is negative, or if
   *         {@code firstRow + rowCount} overflows
   */
  public ClickBenchHitsGenerator(final long firstRow, final long rowCount, final long seed) {
    if (firstRow < 0) {
      throw new IllegalArgumentException("firstRow must be >= 0: " + firstRow);
    }
    if (rowCount < 0) {
      throw new IllegalArgumentException("rowCount must be >= 0: " + rowCount);
    }
    final long endRow = firstRow + rowCount;
    if (endRow < firstRow) {
      throw new IllegalArgumentException("firstRow + rowCount overflows: " + firstRow + " + " + rowCount);
    }
    this.firstRow = firstRow;
    this.endRow = endRow;
    this.seed = seed;
    this.nextRow = firstRow;
  }

  // ── Reader ────────────────────────────────────────────────────────────────────────────────────

  @Override
  public int read() throws IOException {
    ensureOpen();
    if (position == buffer.length() && !refill()) {
      return -1;
    }
    return buffer.charAt(position++);
  }

  @Override
  public int read(final char[] cbuf, final int off, final int len) throws IOException {
    Objects.requireNonNull(cbuf, "cbuf");
    if (off < 0 || len < 0 || len > cbuf.length - off) {
      throw new IndexOutOfBoundsException("off=" + off + " len=" + len + " length=" + cbuf.length);
    }
    ensureOpen();
    if (len == 0) {
      return 0;
    }
    int written = 0;
    while (written < len) {
      if (position == buffer.length() && !refill()) {
        break;
      }
      final int n = Math.min(len - written, buffer.length() - position);
      buffer.getChars(position, position + n, cbuf, off + written);
      position += n;
      written += n;
    }
    return written == 0
        ? -1
        : written;
  }

  @Override
  public boolean ready() throws IOException {
    ensureOpen();
    return true;
  }

  @Override
  public void close() {
    closed = true;
    buffer.setLength(0);
    position = 0;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("stream closed");
    }
  }

  /**
   * Refills {@link #buffer} with the next chunk: the opening bracket plus the first record, a
   * separator plus the next record, or the closing bracket.
   *
   * @return {@code false} once the array has been closed and nothing is left to emit
   */
  private boolean refill() {
    buffer.setLength(0);
    position = 0;
    while (nextRow < endRow && buffer.length() < REFILL_TARGET_CHARS) {
      buffer.append(nextRow == firstRow
          ? "["
          : RECORD_SEPARATOR);
      appendRecord(nextRow);
      nextRow++;
    }
    if (buffer.length() > 0) {
      return true;
    }
    if (!closingBracketWritten) {
      closingBracketWritten = true;
      buffer.append(nextRow == firstRow
          ? "[]"
          : "]");
      return true;
    }
    return false;
  }

  // ── record generation ─────────────────────────────────────────────────────────────────────────

  /**
   * Appends the JSON object for one row: one statement per column, in {@code create.sql} order. Every
   * column is emitted through {@link #num(long)}, {@link #text(String)}, {@link #locationOrEmpty} or
   * {@link #timestamp}, each of which consumes the next entry of {@link #PREFIXES}; the count check
   * at the end therefore also checks the order.
   *
   * <p>
   * Each column's distribution is named — {@link #flag(int)}, {@link #emptyOr(int, String[])},
   * {@link #zeroOrSkewed(int, int[])} and the rest — rather than spelled out here, which is what
   * keeps this method's branch count at one. The draws a later column needs, or that two columns
   * share, are taken up front. Draw <em>order</em> is the dataset: moving one draw across another
   * rewrites every value after it, so the statement order below is as much a part of the encoding
   * contract as the column order is.
   *
   * <p>
   * The 105 emissions deliberately stay in one method. Splitting them into per-group helpers — the
   * obvious way to shorten this — costs ~19 % throughput on the GraalVM JIT this project runs on
   * (445k → 360k rows/s, interleaved A/B at 1M rows): the inliner spends its budget on the first
   * group or two and leaves the rest out of line, and the record path is a straight line of ~700
   * appends with nothing to amortise a partial inline against. Under C2 the same split costs ~2 %.
   * Measure before re-splitting.
   *
   * @param row the row index, which together with the seed determines every value
   */
  private void appendRecord(final long row) {
    seedRow(row);
    column = 0;

    // Draws used by more than one column, or needed before the column that emits them.
    final int day = pickDay();
    final int secondOfDay = nextInt(SECONDS_PER_DAY);
    final long userId = pickUserId();
    final long watchId = pickWatchId();
    final int counterId = pickCounterId();
    final int clientIp = pickClientIp();
    final int regionId = REGION_IDS[skewedSquare(REGION_IDS.length)];
    final int resolutionWidth = RESOLUTION_WIDTHS[nextInt(RESOLUTION_WIDTHS.length)];
    final int resolutionHeight = RESOLUTION_HEIGHTS[nextInt(RESOLUTION_HEIGHTS.length)];
    final boolean isMobile = nextInt(100) < 6;
    final String mobilePhoneModel = pickMobilePhoneModel(isMobile);
    final int mobilePhone = pickMobilePhone(mobilePhoneModel);
    final int searchEngineId = pickSearchEngineId();
    final String searchPhrase = pickSearchPhrase();
    final int advEngineId = zeroOrFrom(90, 1, 20);
    final String title = pickTitle();

    final boolean urlEmpty = nextInt(1000) < 20;
    final int urlHost = pickHost();
    final int urlPath = nextInt(PATHS.length);
    final int urlId = pickLocationId();
    final boolean urlSecure = nextInt(5) == 0;
    final boolean refererEmpty = nextInt(1000) < 120;
    final int refererHost = pickHost();
    final int refererPath = nextInt(PATHS.length);
    final int refererId = pickLocationId();
    final boolean refererSecure = nextInt(5) == 0;
    final long urlHash = pickLocationHash(PLANTED_URL_HASH_ONE_IN, PLANTED_URL_HASH, urlEmpty, urlHost, urlPath, urlId);
    final long refererHash = pickLocationHash(PLANTED_REFERER_HASH_ONE_IN, PLANTED_REFERER_HASH, refererEmpty,
        refererHost, refererPath, refererId);

    // 1..10 WatchID..UserID — what was hit, when, and by whom.
    num(watchId);
    num(flag(95));
    text(title);
    num(1);
    timestamp(day, secondOfDay);
    text(EVENT_DATES[day - 1]);
    num(counterId);
    num(clientIp);
    num(regionId);
    num(userId);

    // 11..13 CounterClass..UserAgent — the platform the hit came from.
    num(zeroOr(90, 3));
    num(OS_CODES[nextInt(OS_CODES.length)]);
    num(USER_AGENT_CODES[nextInt(USER_AGENT_CODES.length)]);

    // 14..20 URL..URLRegionID — the two locations, and how each one is classified.
    locationOrEmpty(urlEmpty, urlHost, urlPath, urlId, urlSecure);
    locationOrEmpty(refererEmpty, refererHost, refererPath, refererId, refererSecure);
    num(flag(8));
    num(REFERER_CATEGORY_IDS[skewedSquare(REFERER_CATEGORY_IDS.length)]);
    num(zeroOrSkewed(70, REGION_IDS));
    num(REFERER_CATEGORY_IDS[skewedSquare(REFERER_CATEGORY_IDS.length)]);
    num(zeroOrSkewed(70, REGION_IDS));

    // 21..30 ResolutionWidth..UserAgentMinor — the screen, and the plugin/browser versions.
    num(resolutionWidth);
    num(resolutionHeight);
    num(RESOLUTION_DEPTHS[nextInt(RESOLUTION_DEPTHS.length)]);
    num(zeroOrFrom(30, 10, 6));
    num(nextInt(10));
    text(FLASH_MINOR2[nextInt(FLASH_MINOR2.length)]);
    num(0);
    num(0);
    num(nextInt(31));
    text(USER_AGENT_MINOR[nextInt(USER_AGENT_MINOR.length)]);

    // 31..40 CookieEnable..SearchPhrase — browser capabilities, the mobile columns, the search.
    num(flag(99));
    num(flag(98));
    num(flagOf(isMobile));
    num(mobilePhone);
    text(mobilePhoneModel);
    text(emptyOr(98, PARAMS));
    num((int) (nextLong() >>> 40));
    num(TRAFIC_SOURCE_IDS[nextInt(TRAFIC_SOURCE_IDS.length)]);
    num(searchEngineId);
    text(searchPhrase);

    // 41..50 AdvEngineID..SilverlightVersion4 — the ad engine, the client window and its clock.
    num(advEngineId);
    num(flagPerMille(5));
    num(resolutionWidth - WINDOW_DELTAS[nextInt(WINDOW_DELTAS.length)]);
    num(resolutionHeight - WINDOW_DELTAS[nextInt(WINDOW_DELTAS.length)]);
    num(CLIENT_TIME_ZONES[nextInt(CLIENT_TIME_ZONES.length)]);
    timestamp(day, jitterSecond(secondOfDay, 300));
    num(0);
    num(0);
    num(pickSilverlightVersion3());
    num(0);

    // 51..60 PageCharset..IsEvent — encoding, tracker version, visit flags, the original URL.
    text(PAGE_CHARSETS[nextInt(PAGE_CHARSETS.length)]);
    num(CODE_VERSIONS[nextInt(CODE_VERSIONS.length)]);
    num(flag(10));
    num(flag(1));
    num(flag(30));
    num(mix64(userId ^ (day * 0x9E3779B97F4A7C15L)) >>> 4);
    // OriginalURL repeats this row's URL on ~3 % of the rows. The draw is taken first and
    // unconditionally, so a row without a URL advances the stream exactly like one with it.
    locationOrEmpty(nextInt(100) >= 3 || urlEmpty, urlHost, urlPath, urlId, urlSecure);
    num((int) (nextLong() >>> 40));
    num(0);
    num(flagPerMille(2));

    // 61..70 IsParameter..Robotness — the remaining hit flags, the local clock, demographics.
    num(flagPerMille(2));
    num(flag(3));
    num(flag(1));
    text(HIT_COLORS[nextInt(HIT_COLORS.length)]);
    timestamp(day, jitterSecond(secondOfDay, 60));
    num(AGES[nextInt(AGES.length)]);
    num(nextInt(3));
    num(nextInt(6));
    num(zeroOr(70, 4096));
    num(pickRobotness());

    // 71..79 RemoteIP..HTTPError — the second IP, the window handles, locale and social.
    num(pickClientIp());
    num(unsetOr(90, 4));
    num(unsetOr(95, 4));
    num(unsetOr(20, 31));
    text(BROWSER_LANGUAGES[skewedSquare(BROWSER_LANGUAGES.length)]);
    text(BROWSER_COUNTRIES[skewedSquare(BROWSER_COUNTRIES.length)]);
    text(emptyOr(97, SOCIAL_NETWORKS));
    text(emptyOr(97, SOCIAL_ACTIONS));
    num(pickHttpError());

    // 80..85 SendTiming..FetchTiming — the page-load phases, each an independent draw.
    num(timing(3000));
    num(timing(500));
    num(timing(1000));
    num(timing(3000));
    num(timing(5000));
    num(timing(5000));

    // 86..91 SocialSourceNetworkID..ParamCurrencyID — the social source and the order params.
    num(zeroOrFrom(97, 1, 4));
    text(emptyOr(97, SOCIAL_SOURCE_PAGES));
    num(pickParamPrice());
    text(emptyOrPerMille(5, ORDER_IDS));
    text(emptyOr(97, PARAM_CURRENCIES));
    num(pickParamCurrencyId());

    // 92..95 OpenstatServiceName..OpenstatSourceID — the four openstat tags.
    text(emptyOrPerMille(10, OPENSTAT));
    text(emptyOrPerMille(10, OPENSTAT));
    text(emptyOrPerMille(10, OPENSTAT));
    text(emptyOrPerMille(10, OPENSTAT));

    // 96..101 UTMSource..FromTag — the campaign tags.
    text(emptyOr(95, UTM_SOURCES));
    text(emptyOr(95, UTM_MEDIUMS));
    text(emptyOr(95, UTM_CAMPAIGNS));
    text(emptyOr(95, UTM_CONTENTS));
    text(emptyOr(95, UTM_TERMS));
    text(emptyOr(95, FROM_TAGS));

    // 102..105 HasGCLID..CLID — the click id, the hashes Q40/Q41 select on, and CLID.
    num(pickHasGclid());
    num(refererHash);
    num(urlHash);
    num(pickClid());

    if (column != PREFIXES.length) {
      throw new IllegalStateException("emitted " + column + " columns, expected " + PREFIXES.length);
    }
    buffer.append('}');
  }



  /** Emits {@code "Name":<digits>}; {@code append(long)} writes exact int64 digits, never a float. */
  private void num(final long value) {
    buffer.append(PREFIXES[column++]).append(value);
  }

  /** Emits {@code "Name":<digits>} for the columns that fit {@code int32}. */
  private void num(final int value) {
    buffer.append(PREFIXES[column++]).append(value);
  }

  /**
   * Emits {@code "Name":"<value>"}.
   *
   * @param escapedBody a string body that has already been through {@link #escapeBody(String)} —
   *        every pool in this class is escaped once at class-initialisation time, which keeps the
   *        per-record path free of the escape scan
   */
  private void text(final String escapedBody) {
    buffer.append(PREFIXES[column++]).append('"').append(escapedBody).append('"');
  }

  /** Emits a URL column, assembled from the pre-escaped host and path pools. */
  private void location(final int host, final int path, final int id, final boolean secure) {
    final StringBuilder sb = buffer;
    sb.append(PREFIXES[column++])
      .append('"')
      .append(secure
          ? SCHEME_HTTPS
          : SCHEME_HTTP)
      .append(HOSTS[host])
      .append(PATHS[path]);
    if (id != NO_ID) {
      sb.append(QUERY_ID).append(id);
    }
    sb.append('"');
  }

  /**
   * Emits a URL column as either the empty string or the assembled location — the shape {@code URL},
   * {@code Referer} and {@code OriginalURL} all take.
   *
   * @param empty whether this row leaves the column empty; callers evaluate their draw into this
   *        argument, so the row's stream advances identically on both branches
   */
  private void locationOrEmpty(final boolean empty, final int host, final int path, final int id,
      final boolean secure) {
    if (empty) {
      text(EMPTY);
    } else {
      location(host, path, id, secure);
    }
  }

  /** Emits {@code "Name":"YYYY-MM-DDTHH:MM:SS"} for the given day of July 2013 and second of day. */
  private void timestamp(final int day, final int secondOfDay) {
    final StringBuilder sb = buffer;
    sb.append(PREFIXES[column++]).append('"').append(EVENT_DATES[day - 1]).append('T');
    appendTwoDigits(sb, secondOfDay / 3600);
    sb.append(':');
    appendTwoDigits(sb, secondOfDay / 60 % 60);
    sb.append(':');
    appendTwoDigits(sb, secondOfDay % 60);
    sb.append('"');
  }

  private static void appendTwoDigits(final StringBuilder sb, final int value) {
    sb.append(TWO_DIGITS, value << 1, 2);
  }

  // ── column shapes ─────────────────────────────────────────────────────────────────────────────
  //
  // The handful of distributions the 105 columns are built from. Naming them keeps the record body
  // at one statement per column, and keeps the branch each column's distribution needs out of the
  // method that emits it. Each one consumes its draws in the order it is written, so replacing one
  // with an equivalent-looking expression that draws in another order changes the whole dataset.

  /** A flag column: {@code 1} on {@code percent} % of the rows, {@code 0} on the rest. */
  private int flag(final int percent) {
    return nextInt(100) < percent
        ? 1
        : 0;
  }

  /** A flag column that is {@code 1} on {@code perMille} rows in a thousand. */
  private int flagPerMille(final int perMille) {
    return nextInt(1000) < perMille
        ? 1
        : 0;
  }

  /** A boolean the schema stores as {@code 0}/{@code 1}. Consumes no draw. */
  private static int flagOf(final boolean value) {
    return value
        ? 1
        : 0;
  }

  /** Zero on {@code percent} % of the rows, otherwise a uniform draw in {@code [0, bound)}. */
  private int zeroOr(final int percent, final int bound) {
    return nextInt(100) < percent
        ? 0
        : nextInt(bound);
  }

  /**
   * Zero on {@code percent} % of the rows, otherwise a uniform draw in {@code [base, base + bound)}.
   */
  private int zeroOrFrom(final int percent, final int base, final int bound) {
    return nextInt(100) < percent
        ? 0
        : base + nextInt(bound);
  }

  /** Zero on {@code percent} % of the rows, otherwise a square-skewed pick from {@code pool}. */
  private int zeroOrSkewed(final int percent, final int[] pool) {
    return nextInt(100) < percent
        ? 0
        : pool[skewedSquare(pool.length)];
  }

  /**
   * {@code -1} — the "not set" the real data uses — on {@code percent} %, otherwise below
   * {@code bound}.
   */
  private int unsetOr(final int percent, final int bound) {
    return nextInt(100) < percent
        ? -1
        : nextInt(bound);
  }

  /** Empty on {@code percent} % of the rows, otherwise a uniform pick from {@code pool}. */
  private String emptyOr(final int percent, final String[] pool) {
    return nextInt(100) < percent
        ? EMPTY
        : pool[nextInt(pool.length)];
  }

  /**
   * Empty except on {@code perMille} rows in a thousand, which take a uniform pick from {@code pool}.
   */
  private String emptyOrPerMille(final int perMille, final String[] pool) {
    return nextInt(1000) < perMille
        ? pool[nextInt(pool.length)]
        : EMPTY;
  }

  // ── per-column draws ──────────────────────────────────────────────────────────────────────────

  /** Day of July 2013, with the 14th and the 15th over-represented for Q42's two-day window. */
  private int pickDay() {
    final int r = nextInt(1000);
    if (r < 120) {
      return 14;
    }
    if (r < 240) {
      return 15;
    }
    return 1 + nextInt(31);
  }

  private long pickUserId() {
    if (nextInt(PLANTED_USER_ID_ONE_IN) == 0) {
      return PLANTED_USER_ID;
    }
    return mix64(skewedCube(USER_ID_SPACE) * 0x2545F4914F6CDD1DL) >>> 4;
  }

  /** One row in 64 reuses a hot watch id, so Q32's {@code GROUP BY WatchID, ClientIP} has groups. */
  private long pickWatchId() {
    if (nextInt(64) == 0) {
      return HOT_WATCH_IDS[nextInt(HOT_WATCH_IDS.length)];
    }
    return nextLong() >>> 4;
  }

  /**
   * {@code MobilePhoneModel}, empty on ~95 % of all rows: only mobile rows can carry one, and only 85
   * % of those do. Non-mobile rows consume no draw here — the {@code &&} short-circuits.
   */
  private String pickMobilePhoneModel(final boolean isMobile) {
    if (isMobile && nextInt(100) < 85) {
      return MOBILE_PHONE_MODELS[nextInt(MOBILE_PHONE_MODELS.length)];
    }
    return EMPTY;
  }

  /** {@code MobilePhone}, non-zero exactly when the model is non-empty, and drawn only then. */
  private int pickMobilePhone(final String mobilePhoneModel) {
    if (mobilePhoneModel.isEmpty()) {
      return 0;
    }
    return 1 + nextInt(7);
  }

  private int pickSearchEngineId() {
    if (nextInt(100) < 80) {
      return 0;
    }
    return SEARCH_ENGINE_IDS[nextInt(SEARCH_ENGINE_IDS.length)];
  }

  /** Empty on ~85 % of the rows, so the queries that filter {@code SearchPhrase <> ''} select few. */
  private String pickSearchPhrase() {
    if (nextInt(100) < 85) {
      return EMPTY;
    }
    return SEARCH_PHRASES[skewedSquare(SEARCH_PHRASES.length)];
  }

  /**
   * Half the locations carry an {@code ?id=} suffix; the other half consume the draw all the same.
   */
  private int pickLocationId() {
    if (nextInt(2) == 0) {
      return nextInt(URL_ID_SPACE);
    }
    return NO_ID;
  }

  /**
   * The hash of one location, with its planted literal mixed in. The planted draw is taken first and
   * unconditionally, so a row spends the same draws whether or not the location is empty.
   *
   * @param oneIn one row in this many carries {@code planted} regardless of the location
   * @param planted the literal a ClickBench query selects on
   * @param empty whether the location column is empty, which hashes to {@code 0}
   */
  private long pickLocationHash(final int oneIn, final long planted, final boolean empty, final int host,
      final int path, final int id) {
    if (nextInt(oneIn) == 0) {
      return planted;
    }
    if (empty) {
      return 0L;
    }
    return locationHash(host, path, id);
  }

  private int pickCounterId() {
    final int r = nextInt(1000);
    if (r < MAIN_COUNTER_PER_MILLE) {
      return MAIN_COUNTER_ID;
    }
    if (r < MAIN_COUNTER_PER_MILLE + COUNTER_ID_BIG_TIER_PER_MILLE) {
      return COUNTER_IDS[nextInt(COUNTER_ID_BIG_TIER)];
    }
    return COUNTER_IDS[COUNTER_ID_BIG_TIER + skewedSquare(COUNTER_IDS.length - COUNTER_ID_BIG_TIER)];
  }

  private int pickClientIp() {
    if (nextInt(4) == 0) {
      return HOT_CLIENT_IPS[nextInt(HOT_CLIENT_IPS.length)];
    }
    return (int) (nextLong() >>> 33);
  }

  /** Google hosts land in {@value #GOOGLE_HOST_PER_MILLE} per mille of the rows. */
  private int pickHost() {
    if (nextInt(1000) < GOOGLE_HOST_PER_MILLE) {
      return nextInt(GOOGLE_HOSTS.length);
    }
    return GOOGLE_HOSTS.length + skewedSquare(OTHER_HOSTS.length);
  }

  private String pickTitle() {
    if (nextInt(100) < 3) {
      return EMPTY;
    }
    if (nextInt(100) < 10) {
      return GOOGLE_TITLES[nextInt(GOOGLE_TITLES.length)];
    }
    return TITLES[skewedSquare(TITLES.length)];
  }

  /** {@code SilverlightVersion3}: zero, or the single build number the real column is full of. */
  private int pickSilverlightVersion3() {
    return nextInt(100) < 95
        ? 0
        : 30729;
  }

  /** {@code Robotness}: zero unless the hit is one of the 3 in 1000 that look automated. */
  private int pickRobotness() {
    return nextInt(1000) < 3
        ? nextInt(100)
        : 0;
  }

  /** {@code HTTPError}: zero — the hit was served — on all but 5 rows in 1000. */
  private int pickHttpError() {
    return nextInt(1000) < 5
        ? HTTP_ERRORS[nextInt(HTTP_ERRORS.length)]
        : 0;
  }

  /**
   * {@code ParamPrice}: an order value in hundredths, which is why it needs the {@code long} range.
   */
  private long pickParamPrice() {
    return nextInt(1000) < 5
        ? 100L * (1 + nextInt(10_000))
        : 0L;
  }

  /** {@code ParamCurrencyID}: the ISO 4217 number of the currency, on the rows that carry a price. */
  private int pickParamCurrencyId() {
    return nextInt(100) < 97
        ? 0
        : PARAM_CURRENCY_IDS[nextInt(PARAM_CURRENCY_IDS.length)];
  }

  /** {@code HasGCLID}: set on the 1 % of rows that arrived with a Google click id. */
  private int pickHasGclid() {
    return nextInt(100) < 99
        ? 0
        : 1;
  }

  /** {@code CLID}: set on 5 rows in 1000, from the high bits of a fresh draw. */
  private int pickClid() {
    return nextInt(1000) < 5
        ? (int) (nextLong() >>> 41)
        : 0;
  }

  /** A timing column: zero on most rows, a plausible millisecond value otherwise. */
  private int timing(final int bound) {
    return nextInt(100) < 80
        ? 0
        : 1 + nextInt(bound);
  }

  /** A second of the same day, within {@code spread} seconds of {@code secondOfDay}. */
  private int jitterSecond(final int secondOfDay, final int spread) {
    final int shifted = secondOfDay + nextInt(2 * spread + 1) - spread;
    if (shifted < 0) {
      return 0;
    }
    return Math.min(shifted, SECONDS_PER_DAY - 1);
  }

  /** Equal locations hash equally, so {@code GROUP BY URLHash} groups the way the real data does. */
  private static long locationHash(final int host, final int path, final int id) {
    return mix64(((long) host << 40) ^ ((long) path << 20) ^ (id + 1L)) >>> 4;
  }

  // ── randomness ────────────────────────────────────────────────────────────────────────────────

  /**
   * Seeds this row's stream from {@code (seed, row)} alone — the property the whole split-equivalence
   * contract rests on. A single sequential PRNG would make row {@code i} depend on how many rows were
   * generated before it, which is exactly what a sharded ingest cannot reproduce.
   */
  private void seedRow(final long row) {
    random = mix64(seed + row * 0x9E3779B97F4A7C15L);
  }

  private long nextLong() {
    long z = (random += 0x9E3779B97F4A7C15L);
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  /**
   * @param bound exclusive upper bound, {@code > 0}
   * @return a value in {@code [0, bound)}, computed with a multiply-shift instead of a division
   */
  private int nextInt(final int bound) {
    return (int) ((nextLong() >>> 32) * bound >>> 32);
  }

  /**
   * A rank in {@code [0, space)} with a square skew: rank 0 takes {@code 1/sqrt(space)} of the mass.
   */
  private int skewedSquare(final int space) {
    final double u = (nextLong() >>> 32) * 0x1p-32;
    return (int) (space * (u * u));
  }

  /** A rank in {@code [0, space)} with a cube skew, for the long-tailed {@code UserID} space. */
  private int skewedCube(final int space) {
    final double u = (nextLong() >>> 32) * 0x1p-32;
    return (int) (space * (u * u * u));
  }

  /** splitmix64's finalizer: a bijection on {@code long}, so distinct inputs stay distinct. */
  private static long mix64(final long value) {
    long z = value;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  // ── pool construction ─────────────────────────────────────────────────────────────────────────

  private static String[] buildPrefixes() {
    final List<String> columns = ClickBenchSchema.COLUMNS;
    final String[] prefixes = new String[columns.size()];
    for (int i = 0; i < prefixes.length; i++) {
      prefixes[i] = (i == 0
          ? "{\""
          : ",\"") + escapeBody(columns.get(i)) + "\":";
    }
    return prefixes;
  }

  private static String[] buildEventDates() {
    final String[] dates = new String[31];
    final StringBuilder sb = new StringBuilder(16);
    for (int day = 1; day <= 31; day++) {
      sb.setLength(0);
      sb.append("2013-07-");
      appendTwoDigits(sb, day);
      dates[day - 1] = escapeBody(sb.toString());
    }
    if (!dates[0].equals(FIRST_EVENT_DATE) || !dates[30].equals(LAST_EVENT_DATE)) {
      throw new IllegalStateException("event date range is not " + FIRST_EVENT_DATE + ".." + LAST_EVENT_DATE);
    }
    return dates;
  }

  private static String[] buildTitles() {
    final String[] sites = {"Yandex", "Mail.Ru", "VK", "Avito", "Auto.ru", "Lenta.ru", "RBC", "Kinopoisk", "YouTube",
        "Wikipedia", "Amazon", "eBay", "Booking", "GitHub", "Spiegel Online", "Heise"};
    final String[] sections = {"Home", "News", "Sport", "Weather", "Catalog", "Forum", "Video", "Maps"};
    final String[] titles = new String[sites.length * sections.length];
    final StringBuilder sb = new StringBuilder(64);
    int index = 0;
    for (final String site : sites) {
      for (final String section : sections) {
        sb.setLength(0);
        sb.append(site).append(" - ").append(section);
        titles[index++] = escapeBody(sb.toString());
      }
    }
    return titles;
  }

  private static String[] buildSearchPhrases() {
    final String[] heads = {"buy", "cheap", "best", "used", "new", "review of", "price of", "photo of", "how to fix",
        "how to choose", "rent", "order", "download", "compare", "repair", "delivery of", "spare parts for",
        "insurance for", "manual for", "test of"};
    final String[] tails = {"laptop", "smartphone", "washing machine", "winter tyres", "flight tickets",
        "hotel in berlin", "used car", "running shoes", "coffee machine", "garden furniture"};
    final String[] phrases = new String[heads.length * tails.length];
    final StringBuilder sb = new StringBuilder(64);
    int index = 0;
    for (final String head : heads) {
      for (final String tail : tails) {
        sb.setLength(0);
        sb.append(head).append(' ').append(tail);
        phrases[index++] = escapeBody(sb.toString());
      }
    }
    if (phrases.length != SEARCH_PHRASE_COUNT) {
      throw new IllegalStateException("expected " + SEARCH_PHRASE_COUNT + " phrases, built " + phrases.length);
    }
    return phrases;
  }

  private static int[] buildRegionIds() {
    final int[] head = {225, 213, 2, 187, 149, 1, 51, 54, 47, 62};
    final int[] regions = new int[200];
    System.arraycopy(head, 0, regions, 0, head.length);
    for (int i = head.length; i < regions.length; i++) {
      regions[i] = 1000 + i;
    }
    return regions;
  }

  private static int[] buildCounterIds() {
    final int[] counters = new int[COUNTER_ID_SPACE];
    for (int i = 0; i < counters.length; i++) {
      // 3 + 17i never hits 62, so MAIN_COUNTER_ID keeps its exact 35 % share.
      counters[i] = 3 + i * 17;
    }
    return counters;
  }

  private static int[] buildHotInts(final int count, final long salt) {
    final int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = (int) (mix64(salt + i) >>> 33);
    }
    return values;
  }

  private static long[] buildHotLongs(final int count, final long salt) {
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = mix64(salt + i) >>> 4;
    }
    return values;
  }

  private static String[] concat(final String[] first, final String[] second) {
    final String[] all = new String[first.length + second.length];
    System.arraycopy(first, 0, all, 0, first.length);
    System.arraycopy(second, 0, all, first.length, second.length);
    return requireSize(all, HOST_COUNT, "hosts");
  }

  /** Keeps a pool's documented size honest: the javadoc quotes these counts. */
  private static String[] requireSize(final String[] pool, final int expected, final String what) {
    if (pool.length != expected) {
      throw new IllegalStateException("expected " + expected + " " + what + ", built " + pool.length);
    }
    return pool;
  }

  private static char[] twoDigitTable() {
    final char[] table = new char[200];
    for (int i = 0; i < 100; i++) {
      table[i << 1] = (char) ('0' + i / 10);
      table[(i << 1) + 1] = (char) ('0' + i % 10);
    }
    return table;
  }

  // ── JSON escaping ─────────────────────────────────────────────────────────────────────────────

  /**
   * Escapes one pool entry, once. Every string this class can emit goes through here — the pools at
   * class-initialisation time, and the composed URLs through their pre-escaped parts — so the record
   * path never has to rescan a string it already knows is safe.
   *
   * @param raw the unescaped text
   * @return the JSON string body (without the surrounding quotes); {@code raw} itself when nothing
   *         needed escaping
   */
  static String escapeBody(final String raw) {
    Objects.requireNonNull(raw, "raw");
    final int length = raw.length();
    int i = 0;
    while (i < length && !needsEscaping(raw.charAt(i))) {
      i++;
    }
    if (i == length) {
      return raw;
    }
    final StringBuilder sb = new StringBuilder(length + 16);
    sb.append(raw, 0, i);
    for (; i < length; i++) {
      appendEscaped(sb, raw.charAt(i));
    }
    return sb.toString();
  }

  private static String[] escapeAll(final String... raw) {
    final String[] escaped = new String[raw.length];
    for (int i = 0; i < raw.length; i++) {
      escaped[i] = escapeBody(raw[i]);
    }
    return escaped;
  }

  /** The characters RFC 8259 forbids raw inside a JSON string. */
  private static boolean needsEscaping(final char c) {
    return c == '"' || c == '\\' || c < 0x20;
  }

  private static void appendEscaped(final StringBuilder sb, final char c) {
    switch (c) {
      case '"' -> sb.append("\\\"");
      case '\\' -> sb.append("\\\\");
      case '\b' -> sb.append("\\b");
      case '\f' -> sb.append("\\f");
      case '\n' -> sb.append("\\n");
      case '\r' -> sb.append("\\r");
      case '\t' -> sb.append("\\t");
      default -> {
        if (c < 0x20) {
          sb.append("\\u00").append(HEX[(c >> 4) & 0xF]).append(HEX[c & 0xF]);
        } else {
          sb.append(c);
        }
      }
    }
  }
}
