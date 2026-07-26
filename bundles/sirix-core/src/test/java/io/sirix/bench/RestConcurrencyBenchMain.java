package io.sirix.bench;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Zero-dependency load generator for the SirixDB REST API (plain JDK {@link HttpClient} +
 * virtual threads, HTTP/1.1 forced so concurrency = real connections, not h2 multiplexing).
 * Validates the unordered-{@code executeBlocking} fix: with the old ordered worker queue every
 * blocking task of the verticle context executed strictly serially server-wide, so p95 latency
 * at concurrency C approached C × p95(1).
 *
 * <p>Modes:
 * <pre>
 *   seed  &lt;baseUrl&gt; &lt;db&gt; &lt;resource&gt; [numItems=20000] [extraRevisions=5]
 *       PUT a generated ~2 MB document {"hot":0,"data":[...]} and create extraRevisions
 *       additional revisions via single-field JSONiq update queries (auto-commit each).
 *   read  &lt;baseUrl&gt; &lt;db&gt; &lt;resource&gt; &lt;concurrency&gt; [warmupSec=5] [measureSec=30]
 *       GET /db/resource?maxLevel=4&amp;maxChildren=50&amp;withMetaData=nodeKeyAndChildCount in a
 *       closed loop from &lt;concurrency&gt; virtual threads; reports p50/p95/p99 + throughput.
 *   mixed &lt;baseUrl&gt; &lt;db&gt; &lt;resource&gt; [readers=16] [warmupSec=5] [measureSec=30]
 *       Same read loop from &lt;readers&gt; threads plus ONE writer thread committing a tiny
 *       single-field replace (each query = one commit); reader + writer stats reported.
 * </pre>
 *
 * Credentials are the rest-api test realm's {@code admin/admin} via {@code POST /token}.
 * Output: one human-readable line + one machine-readable {@code CSV,...} line per run.
 */
public final class RestConcurrencyBenchMain {

  private static final Pattern ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern FIRST_LONG = Pattern.compile("(-?\\d+)");
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);

  private RestConcurrencyBenchMain() {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("usage: seed|read|mixed <baseUrl> <db> <resource> [mode args...]");
      System.exit(2);
    }
    final String mode = args[0];
    final String base = args[1].replaceAll("/$", "");
    final String db = args[2];
    final String resource = args[3];

    final HttpClient client = HttpClient.newBuilder()
                                        .version(HttpClient.Version.HTTP_1_1)
                                        .connectTimeout(Duration.ofSeconds(10))
                                        .build();
    final String token = fetchToken(client, base);

    switch (mode) {
      case "seed" -> seed(client, base, token, db, resource,
                          args.length > 4 ? Integer.parseInt(args[4]) : 20_000,
                          args.length > 5 ? Integer.parseInt(args[5]) : 5);
      case "read" -> readBench(client, base, token, db, resource,
                               Integer.parseInt(args[4]),
                               args.length > 5 ? Integer.parseInt(args[5]) : 5,
                               args.length > 6 ? Integer.parseInt(args[6]) : 30,
                               0);
      case "mixed" -> readBench(client, base, token, db, resource,
                                args.length > 4 ? Integer.parseInt(args[4]) : 16,
                                args.length > 5 ? Integer.parseInt(args[5]) : 5,
                                args.length > 6 ? Integer.parseInt(args[6]) : 30,
                                1);
      default -> {
        System.err.println("unknown mode: " + mode);
        System.exit(2);
      }
    }
  }

  // ------------------------------------------------------------------ auth

  private static String fetchToken(final HttpClient client, final String base) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/token"))
        .header("Content-Type", "application/json")
        .timeout(REQUEST_TIMEOUT)
        .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
        .build();
    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException("token request failed: HTTP " + response.statusCode() + " " + response.body());
    }
    final Matcher matcher = ACCESS_TOKEN.matcher(response.body());
    if (!matcher.find()) {
      throw new IllegalStateException("no access_token in: " + response.body());
    }
    return matcher.group(1);
  }

  // ------------------------------------------------------------------ seed

  private static void seed(final HttpClient client, final String base, final String token,
                           final String db, final String resource, final int numItems,
                           final int extraRevisions) throws Exception {
    final StringBuilder doc = new StringBuilder(numItems * 120);
    doc.append("{\"hot\":0,\"data\":[");
    for (int i = 0; i < numItems; i++) {
      if (i > 0) {
        doc.append(',');
      }
      doc.append("{\"id\":").append(i)
         .append(",\"name\":\"item-").append(i)
         .append("\",\"value\":").append(i).append('.').append(i % 10)
         .append(",\"flags\":[\"a\",\"b\"],\"nested\":{\"x\":").append(i % 97)
         .append(",\"y\":").append(i % 31).append("}}");
    }
    doc.append("]}");
    final String body = doc.toString();
    System.out.printf(Locale.ROOT, "# seeding %s/%s: %.2f MB JSON, %d items%n",
                      db, resource, body.length() / (1024.0 * 1024.0), numItems);

    long t0 = System.nanoTime();
    final HttpResponse<String> put = client.send(
        HttpRequest.newBuilder(URI.create(base + "/" + db + "/" + resource))
                   .header("Authorization", "Bearer " + token)
                   .header("Content-Type", "application/json")
                   .timeout(REQUEST_TIMEOUT)
                   .PUT(HttpRequest.BodyPublishers.ofString(body))
                   .build(),
        HttpResponse.BodyHandlers.ofString());
    if (put.statusCode() != 200 && put.statusCode() != 201) {
      throw new IllegalStateException("seed PUT failed: HTTP " + put.statusCode() + " " + truncate(put.body()));
    }
    System.out.printf(Locale.ROOT, "# PUT shred done in %.1fs (HTTP %d)%n",
                      (System.nanoTime() - t0) / 1e9, put.statusCode());

    final long hotNodeKey = discoverHotNodeKey(client, base, token, db, resource);
    System.out.println("# nodeKey of .hot value: " + hotNodeKey);

    for (int i = 1; i <= extraRevisions; i++) {
      final HttpResponse<String> update = postQuery(client, base, token, replaceHotQuery(db, resource, hotNodeKey, i));
      if (update.statusCode() != 200) {
        throw new IllegalStateException("seed update " + i + " failed: HTTP " + update.statusCode()
                                            + " " + truncate(update.body()));
      }
    }
    System.out.printf(Locale.ROOT, "# created %d extra revisions via single-field updates%n", extraRevisions);
    System.out.println("SEED,ok,bytes=" + body.length() + ",revisions=" + (1 + extraRevisions)
                           + ",hotNodeKey=" + hotNodeKey);
  }

  private static String replaceHotQuery(final String db, final String resource, final long nodeKey, final int value) {
    return "replace json value of sdb:select-item(jn:doc('" + db + "','" + resource + "'), "
        + nodeKey + ") with " + value;
  }

  private static long discoverHotNodeKey(final HttpClient client, final String base, final String token,
                                         final String db, final String resource) throws Exception {
    final HttpResponse<String> response =
        postQuery(client, base, token, "sdb:nodekey(jn:doc('" + db + "','" + resource + "').hot)");
    if (response.statusCode() != 200) {
      throw new IllegalStateException("nodekey discovery failed: HTTP " + response.statusCode()
                                          + " " + truncate(response.body()));
    }
    final Matcher matcher = FIRST_LONG.matcher(response.body());
    if (!matcher.find()) {
      throw new IllegalStateException("no node key in: " + truncate(response.body()));
    }
    return Long.parseLong(matcher.group(1));
  }

  private static HttpResponse<String> postQuery(final HttpClient client, final String base, final String token,
                                                final String query) throws Exception {
    final String json = "{\"query\":\"" + query.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
    return client.send(HttpRequest.newBuilder(URI.create(base + "/"))
                                  .header("Authorization", "Bearer " + token)
                                  .header("Content-Type", "application/json")
                                  .timeout(REQUEST_TIMEOUT)
                                  .POST(HttpRequest.BodyPublishers.ofString(json))
                                  .build(),
                       HttpResponse.BodyHandlers.ofString());
  }

  // ------------------------------------------------------------------ read / mixed bench

  private static void readBench(final HttpClient client, final String base, final String token,
                                final String db, final String resource, final int readers,
                                final int warmupSec, final int measureSec, final int writers) throws Exception {
    final URI readUri = URI.create(base + "/" + db + "/" + resource
                                       + "?maxLevel=4&maxChildren=50&withMetaData=nodeKeyAndChildCount");
    final long hotNodeKey = writers > 0 ? discoverHotNodeKey(client, base, token, db, resource) : -1;

    final AtomicBoolean measuring = new AtomicBoolean(false);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger errors = new AtomicInteger();
    final AtomicReference<String> firstError = new AtomicReference<>();
    final List<List<Long>> readerLatencies = Collections.synchronizedList(new ArrayList<>());
    final List<Long> writerLatencies = Collections.synchronizedList(new ArrayList<>());

    try (final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      final List<Future<?>> futures = new ArrayList<>();
      for (int r = 0; r < readers; r++) {
        futures.add(pool.submit(() -> {
          final List<Long> mine = new ArrayList<>(1 << 14);
          readerLatencies.add(mine);
          final HttpRequest request = HttpRequest.newBuilder(readUri)
                                                 .header("Authorization", "Bearer " + token)
                                                 .header("Accept", "application/json")
                                                 .timeout(REQUEST_TIMEOUT)
                                                 .GET()
                                                 .build();
          while (!stop.get()) {
            final long t0 = System.nanoTime();
            try {
              final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
              final long elapsed = System.nanoTime() - t0;
              if (response.statusCode() == 200) {
                if (measuring.get()) {
                  mine.add(elapsed);
                }
              } else {
                errors.incrementAndGet();
                firstError.compareAndSet(null, "HTTP " + response.statusCode() + " " + truncate(response.body()));
              }
            } catch (final Exception e) {
              if (stop.get()) {
                break;
              }
              errors.incrementAndGet();
              firstError.compareAndSet(null, e.toString());
            }
          }
          return null;
        }));
      }
      for (int w = 0; w < writers; w++) {
        futures.add(pool.submit(() -> {
          int value = 1000;
          while (!stop.get()) {
            final long t0 = System.nanoTime();
            try {
              final HttpResponse<String> response =
                  postQuery(client, base, token, replaceHotQuery(db, resource, hotNodeKey, value++));
              final long elapsed = System.nanoTime() - t0;
              if (response.statusCode() == 200) {
                if (measuring.get()) {
                  writerLatencies.add(elapsed);
                }
              } else {
                errors.incrementAndGet();
                firstError.compareAndSet(null, "writer HTTP " + response.statusCode() + " " + truncate(response.body()));
              }
            } catch (final Exception e) {
              if (stop.get()) {
                break;
              }
              errors.incrementAndGet();
              firstError.compareAndSet(null, "writer " + e);
            }
          }
          return null;
        }));
      }

      Thread.sleep(warmupSec * 1000L);
      measuring.set(true);
      final long measureStart = System.nanoTime();
      Thread.sleep(measureSec * 1000L);
      measuring.set(false);
      final double actualSeconds = (System.nanoTime() - measureStart) / 1e9;
      stop.set(true);
      for (final Future<?> future : futures) {
        future.get();
      }

      final List<Long> all = new ArrayList<>();
      synchronized (readerLatencies) {
        for (final List<Long> list : readerLatencies) {
          all.addAll(list);
        }
      }
      final String label = writers > 0 ? "MIXED(r=" + readers + ",w=" + writers + ")" : "READ(c=" + readers + ")";
      printStats(label + " readers", all, actualSeconds, errors.get());
      if (writers > 0) {
        printStats(label + " writer", new ArrayList<>(writerLatencies), actualSeconds, -1);
      }
      if (firstError.get() != null) {
        System.out.println("# first error: " + firstError.get());
      }
    }
  }

  private static void printStats(final String label, final List<Long> latenciesNanos,
                                 final double seconds, final int errors) {
    if (latenciesNanos.isEmpty()) {
      System.out.println(label + ": NO SAMPLES");
      return;
    }
    Collections.sort(latenciesNanos);
    final int n = latenciesNanos.size();
    final double p50 = latenciesNanos.get((int) (n * 0.50)) / 1e6;
    final double p95 = latenciesNanos.get(Math.min(n - 1, (int) (n * 0.95))) / 1e6;
    final double p99 = latenciesNanos.get(Math.min(n - 1, (int) (n * 0.99))) / 1e6;
    final double max = latenciesNanos.get(n - 1) / 1e6;
    double sum = 0;
    for (final long nano : latenciesNanos) {
      sum += nano;
    }
    final double mean = sum / n / 1e6;
    final double throughput = n / seconds;
    System.out.printf(Locale.ROOT,
                      "%s: n=%d window=%.1fs thr=%.1f req/s mean=%.1fms p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms%s%n",
                      label, n, seconds, throughput, mean, p50, p95, p99, max,
                      errors >= 0 ? " errors=" + errors : "");
    System.out.printf(Locale.ROOT,
                      "CSV,%s,n=%d,thr=%.2f,mean_ms=%.2f,p50_ms=%.2f,p95_ms=%.2f,p99_ms=%.2f,max_ms=%.2f,errors=%d%n",
                      label.replace(',', ';'), n, throughput, mean, p50, p95, p99, max, Math.max(errors, 0));
  }

  private static String truncate(final String value) {
    return value == null ? "null" : value.length() > 300 ? value.substring(0, 300) + "…" : value;
  }
}
