package io.sirix.benchmark;

import org.openjdk.jmh.annotations.*;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.StorageType;
import org.sirix.io.Writer;
import org.sirix.page.PageReference;
import org.sirix.service.xml.shredder.XmlShredder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Threads(4)
public class StorageBench {

  public static final Path DB_PATH = Paths.get(System.getProperty("user.home"), "sirix-data", "storage-db");
  public static final Path RESOURCES = Paths.get("src", "jmh", "resources", "xmark");

  @Benchmark
  public void readPage(BenchState state) {
    state.reader.read(state.uberPage, null);
  }

  @Benchmark
  public void writePage(BenchState state) {
    state.writer.write(state.uberPage);
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Benchmark
  public void commitMedium(BenchState state) {
    try (var wtx = state.manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(BenchState.MediumXMLString));
      wtx.commit();
    }
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Benchmark
  public void commitSmall(BenchState state) {
    try (var wtx = state.manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(BenchState.SmallXMLString));
      wtx.commit();
    }
  }

  @State(Scope.Thread)
  public static class BenchState {

    public Database<XmlResourceManager> db;
    public IOStorage storage;
    public Reader reader;

    public Writer writer;
    public XmlResourceManager manager;
    public PageReference uberPage;
    public Path dbPath;
    public ResourceConfiguration conf;
    public static String MediumXMLString = "";
    public static String SmallXMLString = "";

    @Param({"IN_MEMORY", "FILE"})
    StorageType storageType;

    @Setup(Level.Iteration)
    public void doTrialSetup() {
      dbPath = DB_PATH.resolveSibling(DB_PATH.getFileName() + "." + Thread.currentThread().getId());
      if (Databases.existsDatabase(dbPath)) {
        Databases.removeDatabase(dbPath);
      }

      var dbConf = new DatabaseConfiguration(dbPath);
      Databases.createXmlDatabase(dbConf);
      db = Databases.openXmlDatabase(dbPath);
      conf = ResourceConfiguration.newBuilder("resource")
          .useTextCompression(false)
          .build();

      db.createResource(conf);
      manager = db.openResourceManager("resource");
      try (var wtx = manager.beginNodeTrx()) {
        MediumXMLString = Files.readString(RESOURCES.resolve("auction.xml"));
        SmallXMLString = Files.readString(RESOURCES.resolve("mini.xml"));
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(MediumXMLString));
        wtx.commit();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      storage = storageType.getInstance(conf);
      reader = storage.createReader();
      writer = storage.createWriter();
      uberPage = reader.readUberPageReference();
      assert uberPage != null : "Uber page should not be null";
    }

    @TearDown(Level.Iteration)
    public void doTrialTearDown() {
      storage.close();
      reader.close();
      writer.close();
      manager.close();
      db.close();
      Databases.removeDatabase(dbPath);
    }
  }
}
