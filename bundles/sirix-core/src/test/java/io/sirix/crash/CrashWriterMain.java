package io.sirix.crash;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Crash-injection WRITER: commits revisions in a tight loop and appends one fsync'd ack line per
 * DURABLE commit to an ack log. The parent test process kills this JVM with SIGKILL at a random
 * point and then verifies that every acknowledged commit survived intact. Run only by
 * {@link CrashRecoveryInjectionTest}.
 *
 * <p>Revision content is deterministic: revision 1 creates the root array; every commit inserts
 * one {@code {"i": N}} object as the array's first child. After the commit for revision R, the
 * array has exactly {@code R - 1} children (the creating commit is revision 1 with 0 children).
 * The ack line is {@code R <childCount>}, written and fsync'd strictly AFTER commit() returns.
 */
public final class CrashWriterMain {

  private CrashWriterMain() {
  }

  public static void main(final String[] args) throws Exception {
    final Path dbPath = Paths.get(args[0]);
    final Path ackPath = Paths.get(args[1]);
    final String resource = "crash-resource";

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
         final FileChannel ack = FileChannel.open(ackPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                                                  StandardOpenOption.APPEND)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .buildPathSummary(true)
                                                   .storeChildCount(true)
                                                   .hashKind(HashType.ROLLING)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());

      try (final JsonResourceSession session = database.beginResourceSession(resource);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // Revision 1: the root array.
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
        ackCommit(ack, wtx.getRevisionNumber() - 1, 0);

        // Every loop iteration: one new {"i":N} child + commit + fsync'd ack. The parent's
        // SIGKILL lands somewhere in here — possibly mid-commit, mid-fsync, or between
        // commit and ack.
        long n = 0;
        while (true) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // the array
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"i\":" + n + "}"),
                                        JsonNodeTrx.Commit.NO);
          wtx.commit();
          n++;
          ackCommit(ack, wtx.getRevisionNumber() - 1, n);
        }
      }
    }
  }

  private static void ackCommit(final FileChannel ack, final int revision, final long children) throws IOException {
    final byte[] line = (revision + " " + children + "\n").getBytes(StandardCharsets.US_ASCII);
    final ByteBuffer buf = ByteBuffer.wrap(line);
    while (buf.hasRemaining()) {
      ack.write(buf);
    }
    ack.force(true);
  }
}
