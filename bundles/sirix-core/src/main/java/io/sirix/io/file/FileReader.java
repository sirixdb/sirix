/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.io.file;

import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.io.HashAlgorithm;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.PageUtils;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.io.Writer;
import io.sirix.node.BytesIn;
import io.sirix.node.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * File Reader. Used for {@link StorageEngineReader} to provide read only access on a
 * RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileReader implements Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

  /**
   * Secondary UberPage beacon offset. FileWriter writes the second beacon at offset 100,
   * which gets aligned to PAGE_FRAGMENT_BYTE_ALIGN (8) during write, resulting in offset 104.
   */
  private static final int SECONDARY_UBER_PAGE_OFFSET;

  static {
    int offset = 100;
    if (offset % Writer.PAGE_FRAGMENT_BYTE_ALIGN != 0) {
      offset += Writer.PAGE_FRAGMENT_BYTE_ALIGN - (offset % Writer.PAGE_FRAGMENT_BYTE_ALIGN);
    }
    SECONDARY_UBER_PAGE_OFFSET = offset;
  }

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * Data file.
   */
  private final RandomAccessFile dataFile;

  /**
   * Revisions offset file.
   */
  private final RandomAccessFile revisionsOffsetFile;

  /**
   * The type of data to serialize.
   */
  private final SerializationType serializationType;

  /**
   * Used to serialize/deserialze pages.
   */
  private final PagePersister pagePersiter;

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param dataFile the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param byteHandler {@link ByteHandler} instance
   * @throws SirixIOException if something bad happens
   */
  public FileReader(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final ByteHandler byteHandler, final SerializationType serializationType, final PagePersister pagePersister,
      final Cache<Integer, RevisionFileData> cache) {
    this.dataFile = requireNonNull(dataFile);

    this.revisionsOffsetFile = serializationType == SerializationType.DATA
        ? requireNonNull(revisionsOffsetFile)
        : null;
    this.byteHandler = requireNonNull(byteHandler);
    this.serializationType = requireNonNull(serializationType);
    this.pagePersiter = requireNonNull(pagePersister);
    this.cache = cache;
  }

  @Override
  public Page read(final PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    try {
      // Read page from file.
      dataFile.seek(reference.getKey());
      final int dataLength = dataFile.readInt();
      // reference.setLength(dataLength + FileReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];
      dataFile.read(page);

      // Verify checksum for non-KVLP pages (KVLP verified after decompression)
      verifyChecksumIfNeeded(page, reference, resourceConfiguration);

      return getPage(resourceConfiguration, page, reference);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Verify page checksum on compressed data (all page types).
   */
  private void verifyChecksumIfNeeded(byte[] compressedData, PageReference reference,
      ResourceConfiguration resourceConfig) {
    if (resourceConfig == null || !resourceConfig.verifyChecksumsOnRead) {
      return;
    }

    byte[] expectedHash = reference.getHash();
    if (expectedHash == null || expectedHash.length == 0) {
      return;
    }

    HashAlgorithm hashAlgorithm = resourceConfig.hashAlgorithm;
    if (!PageHasher.verify(compressedData, expectedHash, hashAlgorithm)) {
      byte[] actualHash = PageHasher.computeActualHash(compressedData, hashAlgorithm);
      throw new SirixCorruptionException(reference.getKey(), "compressed", expectedHash, actualHash);
    }
  }

  private Page getPage(ResourceConfiguration resourceConfiguration, byte[] page, PageReference reference)
      throws IOException {
    final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page));
    byte[] uncompressedBytes = inputStream.readAllBytes();

    final BytesIn<?> input = Bytes.wrapForRead(uncompressedBytes);
    final var deserializedPage = pagePersiter.deserializePage(resourceConfiguration, input, serializationType);

    // CRITICAL: Set database and resource IDs on all PageReferences in the deserialized page
    if (resourceConfiguration != null) {
      PageUtils.fixupPageReferenceIds(deserializedPage, resourceConfiguration.getDatabaseId(),
          resourceConfiguration.getID());
    }

    return deserializedPage;
  }

  @Override
  public PageReference readUberPageReference() {
    // Try primary beacon at offset 0
    try {
      final PageReference primaryRef = new PageReference();
      primaryRef.setKey(0);
      final UberPage page = (UberPage) read(primaryRef, null);
      primaryRef.setPage(page);
      return primaryRef;
    } catch (final Exception primaryException) {
      LOGGER.warn("Primary UberPage beacon at offset 0 is corrupt, attempting secondary beacon at offset {}",
          SECONDARY_UBER_PAGE_OFFSET, primaryException);

      // Fallback to secondary beacon
      try {
        final PageReference secondaryRef = new PageReference();
        secondaryRef.setKey(SECONDARY_UBER_PAGE_OFFSET);
        final UberPage page = (UberPage) read(secondaryRef, null);
        secondaryRef.setPage(page);
        LOGGER.info("Successfully recovered UberPage from secondary beacon at offset {}", SECONDARY_UBER_PAGE_OFFSET);
        return secondaryRef;
      } catch (final Exception secondaryException) {
        LOGGER.error("Both UberPage beacons are corrupt — primary at offset 0, secondary at offset {}",
            SECONDARY_UBER_PAGE_OFFSET, secondaryException);
        primaryException.addSuppressed(secondaryException);
        if (primaryException instanceof RuntimeException rte) {
          throw rte;
        }
        throw new SirixIOException(primaryException);
      }
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
    try {
      final long offsetIntoDataFile;

      if (cache != null) {
        offsetIntoDataFile = cache.get(revision, (_) -> getRevisionFileData(revision)).offset();
      } else {
        offsetIntoDataFile = getRevisionFileData(revision).offset();
      }

      dataFile.seek(offsetIntoDataFile);

      final int dataLength = dataFile.readInt();
      final byte[] page = new byte[dataLength];
      dataFile.read(page);

      // Perform byte operations.
      final BytesIn<?> input = Bytes.wrapForRead(page);

      // Return reader required to instantiate and deserialize page.
      return (RevisionRootPage) pagePersiter.deserializePage(resourceConfiguration, input, serializationType);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, _ -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    try {
      final var fileOffset = revision * 8 * 2 + IOStorage.FIRST_BEACON;
      revisionsOffsetFile.seek(fileOffset);
      final long offset = revisionsOffsetFile.readLong();
      revisionsOffsetFile.seek(fileOffset + 8);
      final var timestamp = Instant.ofEpochMilli(revisionsOffsetFile.readLong());
      return new RevisionFileData(offset, timestamp);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (revisionsOffsetFile != null) {
        revisionsOffsetFile.close();
      }
      dataFile.close();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
