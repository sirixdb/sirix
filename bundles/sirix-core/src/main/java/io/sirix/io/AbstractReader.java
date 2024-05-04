package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageReadOnlyTrx;
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

  public AbstractReader(ByteHandler byteHandler, PagePersister pagePersister, SerializationType type) {
    this.byteHandler = byteHandler;
    this.pagePersister = pagePersister;
    this.type = type;
  }

  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page) throws IOException {
    // perform byte operations
    byte[] bytes;
    try (final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
      bytes = inputStream.readAllBytes();
    }
    wrappedForRead.write(bytes);
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
