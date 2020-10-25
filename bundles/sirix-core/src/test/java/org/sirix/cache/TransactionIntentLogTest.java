package org.sirix.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.index.IndexType;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.SnappyCompressor;
import org.sirix.io.file.FileWriter;
import org.sirix.page.PageKind;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.UnorderedKeyValuePage;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TransactionIntentLogTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void integrationTest() throws FileNotFoundException {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      final var resourceManager = database.openResourceManager(JsonTestHelper.RESOURCE);
      final var pageReadOnlyTrx = resourceManager.beginPageReadOnlyTrx()) {
      final RandomAccessFile file = new RandomAccessFile(JsonTestHelper.PATHS.PATH2.getFile().toFile(), "rw");

      final FileWriter fileWriter =
          new FileWriter(file, null, new ByteHandlePipeline(new ByteHandlePipeline(new SnappyCompressor())),
                         SerializationType.TRANSACTION_INTENT_LOG, new PagePersister());

      final var persistentCache = new PersistentFileCache(fileWriter);
      final var trxIntentLog = new TransactionIntentLog(persistentCache, 1);

      final var firstCompletePage = new UnorderedKeyValuePage(1, IndexType.DOCUMENT, pageReadOnlyTrx);
      final var firstDeltaPage = new UnorderedKeyValuePage(1, IndexType.DOCUMENT, pageReadOnlyTrx);

      final var secondCompletePage = new UnorderedKeyValuePage(1, IndexType.DOCUMENT, pageReadOnlyTrx);
      final var secondDeltaPage = new UnorderedKeyValuePage(1, IndexType.DOCUMENT, pageReadOnlyTrx);

      final var firstPageReference = new PageReference();
      final var secondPageReference = new PageReference();

      final var firstPageContainer = PageContainer.getInstance(firstCompletePage, firstDeltaPage);
      final var secondPageContainer = PageContainer.getInstance(secondCompletePage, secondDeltaPage);

      trxIntentLog.put(firstPageReference, firstPageContainer);
      trxIntentLog.put(secondPageReference, secondPageContainer);

      assertEquals(firstPageContainer, trxIntentLog.get(firstPageReference, pageReadOnlyTrx));
      assertEquals(secondPageContainer, trxIntentLog.get(secondPageReference, pageReadOnlyTrx));
    }
  }
}
