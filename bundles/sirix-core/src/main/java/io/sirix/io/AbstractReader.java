package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import net.openhft.chronicle.bytes.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public abstract class AbstractReader implements Reader {

  private final Bytes<byte[]> wrappedForRead = Bytes.allocateElasticOnHeap(10_000);

  protected final ByteHandler byteHandler;

  /**
   * The type of data to serialize.
   */
  protected final SerializationType type;

  /**
   * Used to serialize/deserialze pages.
   */
  protected final PagePersister pagePersister;

  private final byte[] bytes = new byte[130_000];

  public AbstractReader(ByteHandler byteHandler, PagePersister pagePersister, SerializationType type) {
    this.byteHandler = byteHandler;
    this.pagePersister = pagePersister;
    this.type = type;
  }

  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page, int uncompressedLength)
      throws IOException {
    // perform byte operations
    try (final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
      int bytesRead = 0;
      while (bytesRead < uncompressedLength) {
        int read = inputStream.read(bytes, bytesRead, uncompressedLength - bytesRead);
        if (read == -1) {
          throw new IOException("Unexpected end of stream while reading decompressed data.");
        }
        bytesRead += read;
      }
      assert bytesRead == uncompressedLength : "Read bytes mismatch: expected " + uncompressedLength + " but got " + bytesRead;
    }
    wrappedForRead.write(bytes, 0, uncompressedLength);
    final var deserializedPage = pagePersister.deserializePage(resourceConfiguration, wrappedForRead, type);
    wrappedForRead.clear();
    return deserializedPage;
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();
    uberPageReference.setKey(0);
    final UberPage page = (UberPage) read(uberPageReference, null);
    uberPageReference.setPage(page);
    return uberPageReference;
  }

  public ByteHandler getByteHandler() {
    return byteHandler;
  }
}
