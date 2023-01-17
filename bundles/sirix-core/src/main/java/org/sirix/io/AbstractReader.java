package org.sirix.io;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public abstract class AbstractReader implements Reader {
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

  public Page deserialize(PageReadOnlyTrx pageReadTrx, byte[] page) throws IOException {
    // perform byte operations
    final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page));
    byte[] bytes = inputStream.readAllBytes();
    final Bytes<?> input = Bytes.elasticByteBuffer(10_000);
    BytesUtils.doWrite(input, bytes);
    final var deserializedPage = pagePersister.deserializePage(pageReadTrx, input, type);
    input.clear();
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
