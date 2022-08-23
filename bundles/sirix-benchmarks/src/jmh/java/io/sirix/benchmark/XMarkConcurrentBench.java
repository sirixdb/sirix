package io.sirix.benchmark;

import org.openjdk.jmh.annotations.*;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Axis;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.concurrent.ConcurrentAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.xml.XmlNameFilter;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.service.xml.xpath.XPathAxis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2G", "-Xmx2G" })
public class XMarkConcurrentBench {

  private static final Path XML = Paths.get("src", "jmh", "resources");

  public static final String USER_HOME = System.getProperty("user.home");

  public static final Path SIRIX_DATA_LOCATION = Paths.get(USER_HOME, "sirix-data");

  private static final Path DATABASE_PATH = SIRIX_DATA_LOCATION.resolve("xml-xmark-database");

  @State(Scope.Thread)
  public static class MyState {

    private Database<XmlResourceManager> database;
    private XmlResourceManager manager;
    private XmlNodeReadOnlyTrx rtx;
    private XmlNodeReadOnlyTrx firstConcurrRtx;
    private XmlNodeReadOnlyTrx secondConcurrRtx;
    private XmlNodeReadOnlyTrx thirdConcurrRtx;
    private XmlNodeReadOnlyTrx firstRtx;
    private XmlNodeReadOnlyTrx secondRtx;
    private XmlNodeReadOnlyTrx thirdRtx;

    @Setup(Level.Trial)
    public void doSetup() {
      final var pathToXmlFile = XML.resolve("10mb.xml");

      if (Files.exists(DATABASE_PATH))
        Databases.removeDatabase(DATABASE_PATH);

      final var dbConfig = new DatabaseConfiguration(DATABASE_PATH);
      Databases.createXmlDatabase(dbConfig);
      database = Databases.openXmlDatabase(DATABASE_PATH);
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .useTextCompression(false)
                                                   .useDeweyIDs(true)
                                                   .build());
      manager = database.openResourceManager("resource");

      try (final var wtx = manager.beginNodeTrx(); final var fis = new FileInputStream(pathToXmlFile.toFile())) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
        wtx.commit();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      rtx = manager.beginNodeReadOnlyTrx();
      firstConcurrRtx = manager.beginNodeReadOnlyTrx();
      secondConcurrRtx = manager.beginNodeReadOnlyTrx();
      thirdConcurrRtx = manager.beginNodeReadOnlyTrx();
      firstRtx = manager.beginNodeReadOnlyTrx();
      secondRtx = manager.beginNodeReadOnlyTrx();
      thirdRtx = manager.beginNodeReadOnlyTrx();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      rtx.close();
      firstConcurrRtx.close();
      secondConcurrRtx.close();
      thirdConcurrRtx.close();
      firstRtx.close();
      secondRtx.close();
      thirdRtx.close();
      manager.close();
      database.close();

      Databases.removeDatabase(DATABASE_PATH);
    }
  }

  /**
   * Test seriell.
   */
  @Benchmark
  public void testSeriellOld(MyState state) {
    final String query = "//regions/africa//location";
    final int resultNumber = 55;
    final Axis axis = new XPathAxis(state.rtx, query);
    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.nextLong();
    }
    assertEquals(false, axis.hasNext());
  }

  /**
   * Test seriell.
   */
  @Benchmark
  public void testSeriellNew(MyState state) {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var axis = new NestedAxis(
        new NestedAxis(new FilterAxis<>(new DescendantAxis(state.rtx, IncludeSelf.YES), new XmlNameFilter(state.rtx, "regions")),
                       new FilterAxis<>(new ChildAxis(state.rtx), new XmlNameFilter(state.rtx, "africa"))),
        new FilterAxis<>(new DescendantAxis(state.rtx, IncludeSelf.YES), new XmlNameFilter(state.rtx, "location")));

    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.nextLong();
    }
    assertEquals(false, axis.hasNext());
  }

  /**
   * Test concurrent.
   */
  @Benchmark
  public void testConcurrent(MyState state) {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final Axis axis = new NestedAxis(new NestedAxis(new ConcurrentAxis<>(state.firstConcurrRtx, new FilterAxis<>(
        new DescendantAxis(state.firstRtx, IncludeSelf.YES), new XmlNameFilter(state.firstRtx, "regions"))),
                                                    new ConcurrentAxis<>(state.secondConcurrRtx, new FilterAxis<>(
                                                        new ChildAxis(state.secondRtx),
                                                        new XmlNameFilter(state.secondRtx, "africa")))),
                                     new ConcurrentAxis<>(state.thirdConcurrRtx, new FilterAxis<>(
                                         new DescendantAxis(state.thirdRtx, IncludeSelf.YES),
                                         new XmlNameFilter(state.thirdRtx, "location"))));

    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.nextLong();
    }
    assertEquals(false, axis.hasNext());
  }

  /**
   * Test concurrent.
   */
  @Benchmark
  public void testPartConcurrentDescAxis1(MyState state) {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var axis = new NestedAxis(new NestedAxis(new ConcurrentAxis<>(state.firstConcurrRtx, new FilterAxis<>(
        new DescendantAxis(state.rtx, IncludeSelf.YES), new XmlNameFilter(state.rtx, "regions"))),
                                                   new FilterAxis<>(new ChildAxis(state.firstConcurrRtx),
                                                                    new XmlNameFilter(state.firstConcurrRtx,
                                                                                      "africa"))),
                                    new FilterAxis<>(new DescendantAxis(state.firstConcurrRtx, IncludeSelf.YES),
                                                     new XmlNameFilter(state.firstConcurrRtx, "location")));

    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.nextLong();
    }
    assertEquals(false, axis.hasNext());
  }

  /**
   * Test concurrent.
   */
  @Benchmark
  public void testPartConcurrentDescAxis2(MyState state) {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var axis = new NestedAxis(new NestedAxis(
        new FilterAxis<>(new DescendantAxis(state.firstConcurrRtx, IncludeSelf.YES),
                         new XmlNameFilter(state.firstConcurrRtx, "regions")),
        new FilterAxis<>(new ChildAxis(state.firstConcurrRtx), new XmlNameFilter(state.firstConcurrRtx, "africa"))),
                                    new ConcurrentAxis<>(state.firstConcurrRtx, new FilterAxis<>(
                                        new DescendantAxis(state.rtx, IncludeSelf.YES),
                                        new XmlNameFilter(state.rtx, "location"))));

    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.nextLong();
    }
    assertEquals(axis.hasNext(), false);
  }
}
