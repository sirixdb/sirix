/*
 * Copyright (c) 2024, Sirix Contributors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of Sirix nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL COPYRIGHT HOLDER BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.io.memorymapped;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractForwardingReader;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Writer;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Memory-mapped file writer for providing efficient write access using mmap.
 * 
 * <p>This writer uses memory-mapped I/O for the data file, allowing direct
 * memory-to-memory copies without intermediate buffering through the kernel.</p>
 * 
 * <p>Key features:</p>
 * <ul>
 *   <li>Memory-mapped writes for data file - avoids syscall overhead</li>
 *   <li>Dynamic region mapping - extends file and remaps as needed</li>
 *   <li>Supports MemorySegment-based data for zero-copy paths</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 */
public final class MMFileWriter extends AbstractForwardingReader implements Writer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MMFileWriter.class);
    
    /** Minimum mapped region size: 64MB */
    private static final long MIN_MAPPED_SIZE = 64L * 1024 * 1024;
    
    /** The data file channel */
    private final FileChannel dataFileChannel;
    
    /** The revisions file channel */
    private final FileChannel revisionsFileChannel;
    
    /** The reader delegate */
    private final MMFileReader reader;
    
    /** The serialization type */
    private final SerializationType serializationType;
    
    /** The page persister */
    private final PagePersister pagePersister;
    
    /** The revision file data cache */
    private final AsyncCache<Integer, RevisionFileData> cache;
    
    /** Whether this is the first uber page */
    private boolean isFirstUberPage;
    
    /** Reusable buffer for page serialization */
    private final BytesOut<?> byteBufferBytes = Bytes.elasticOffHeapByteBuffer(1_000);
    
    /** Current mapped region for writes */
    private MemorySegment mappedRegion;
    
    /** Arena for the mapped region */
    private Arena mappedArena;
    
    /** Start offset of the mapped region in the file */
    private long mappedOffset;
    
    /** Current write position within the mapped region */
    private long writePositionInRegion;
    
    /**
     * Constructor.
     *
     * @param dataFileChannel       the data file channel
     * @param revisionsFileChannel  the revisions offset file channel
     * @param serializationType     the serialization type
     * @param pagePersister         the page persister
     * @param cache                 the revision file data cache
     * @param reader                the reader delegate
     */
    public MMFileWriter(final FileChannel dataFileChannel,
                        final FileChannel revisionsFileChannel,
                        final SerializationType serializationType,
                        final PagePersister pagePersister,
                        final AsyncCache<Integer, RevisionFileData> cache,
                        final MMFileReader reader) {
        this.dataFileChannel = requireNonNull(dataFileChannel);
        this.revisionsFileChannel = requireNonNull(revisionsFileChannel);
        this.serializationType = requireNonNull(serializationType);
        this.pagePersister = requireNonNull(pagePersister);
        this.cache = requireNonNull(cache);
        this.reader = requireNonNull(reader);
    }
    
    /**
     * Ensure we have a mapped region covering the specified range.
     */
    private void ensureMapped(long requiredOffset, long requiredSize) throws IOException {
        long requiredEnd = requiredOffset + requiredSize;
        
        // Check if current mapping covers the required range
        if (mappedRegion != null && 
            requiredOffset >= mappedOffset && 
            requiredEnd <= mappedOffset + mappedRegion.byteSize()) {
            return;
        }
        
        // Need to remap
        if (mappedArena != null) {
            // Force sync before unmapping
            mappedRegion.force();
            mappedArena.close();
        }
        
        // Extend file if needed
        long fileSize = dataFileChannel.size();
        if (requiredEnd > fileSize) {
            // Extend file with sufficient space
            long newSize = Math.max(requiredEnd, fileSize + MIN_MAPPED_SIZE);
            dataFileChannel.position(newSize - 1);
            dataFileChannel.write(ByteBuffer.allocate(1));
            LOGGER.trace("Extended data file to {} bytes", newSize);
        }
        
        // Calculate new mapping region
        mappedOffset = requiredOffset;
        long mappedSize = Math.max(requiredEnd - requiredOffset, MIN_MAPPED_SIZE);
        
        // Don't map past end of file
        long actualFileSize = dataFileChannel.size();
        if (mappedOffset + mappedSize > actualFileSize) {
            mappedSize = actualFileSize - mappedOffset;
        }
        
        // Create new mapping
        mappedArena = Arena.ofShared();
        mappedRegion = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, mappedOffset, mappedSize, mappedArena);
        writePositionInRegion = 0;
        
        LOGGER.trace("Mapped data file region: offset={}, size={}", mappedOffset, mappedSize);
    }
    
    /**
     * Write data to the mapped region.
     */
    private void writeToMappedRegion(long fileOffset, byte[] data) throws IOException {
        ensureMapped(fileOffset, data.length);
        
        long regionOffset = fileOffset - mappedOffset;
        MemorySegment.copy(data, 0, mappedRegion, ValueLayout.JAVA_BYTE, regionOffset, data.length);
    }
    
    /**
     * Write a MemorySegment to the mapped region (zero-copy).
     */
    private void writeToMappedRegion(long fileOffset, MemorySegment source) throws IOException {
        ensureMapped(fileOffset, source.byteSize());
        
        long regionOffset = fileOffset - mappedOffset;
        MemorySegment.copy(source, 0, mappedRegion, regionOffset, source.byteSize());
    }
    
    @Override
    public Writer write(final ResourceConfiguration resourceConfiguration, 
                        final PageReference pageReference,
                        final Page page, 
                        final BytesOut<?> bufferedBytes) {
        try {
            final long offset = getOffset(bufferedBytes);
            return writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, offset);
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }
    
    private long getOffset(BytesOut<?> bufferedBytes) throws IOException {
        final long fileSize = dataFileChannel.size();
        long offset;
        
        if (fileSize == 0) {
            offset = IOStorage.FIRST_BEACON;
            offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));
            offset += bufferedBytes.writePosition();
        } else {
            offset = fileSize + bufferedBytes.writePosition();
        }
        
        return offset;
    }
    
    private byte[] buildSerializedPage(final ResourceConfiguration resourceConfiguration, final Page page) throws IOException {
        final BytesIn<?> uncompressedBytes = byteBufferBytes.bytesForRead();
        
        if (page instanceof KeyValueLeafPage keyValueLeafPage && keyValueLeafPage.getBytes() != null) {
            final var cached = keyValueLeafPage.getBytes();
            if (cached instanceof MemorySegmentBytesOut msOut) {
                MemorySegment segment = msOut.getDestination();
                return segment.toArray(ValueLayout.JAVA_BYTE);
            }
            return cached.toByteArray();
        }
        
        final var pipeline = resourceConfiguration.byteHandlePipeline;
        
        if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
            MemorySegment compressedSegment = pipeline.compress(segmentIn.getSource());
            return compressedSegment.toArray(ValueLayout.JAVA_BYTE);
        }
        
        final byte[] byteArray = uncompressedBytes.toByteArray();
        
        final ByteArrayOutputStream output = new ByteArrayOutputStream(byteArray.length);
        try (final OutputStream compressStream = reader.getByteHandler().serialize(output)) {
            compressStream.write(byteArray);
        }
        return output.toByteArray();
    }
    
    @NonNull
    private MMFileWriter writePageReference(final ResourceConfiguration resourceConfiguration,
                                            final PageReference pageReference,
                                            final Page page,
                                            final BytesOut<?> bufferedBytes,
                                            long offset) {
        try {
            // Serialize page
            pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, serializationType);
            final byte[] serializedPage = buildSerializedPage(resourceConfiguration, page);
            
            byteBufferBytes.clear();
            
            int offsetToAdd = 0;
            
            // Calculate alignment
            if (serializationType == SerializationType.DATA) {
                if (page instanceof UberPage) {
                    offsetToAdd = UBER_PAGE_BYTE_ALIGN - ((serializedPage.length + IOStorage.OTHER_BEACON) % UBER_PAGE_BYTE_ALIGN);
                } else if (page instanceof RevisionRootPage && offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
                    offsetToAdd = (int) (REVISION_ROOT_PAGE_BYTE_ALIGN - (offset & (REVISION_ROOT_PAGE_BYTE_ALIGN - 1)));
                    offset += offsetToAdd;
                } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
                    offsetToAdd = (int) (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));
                    offset += offsetToAdd;
                }
            }
            
            if (!(page instanceof UberPage) && offsetToAdd > 0) {
                bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
            }
            
            bufferedBytes.writeInt(serializedPage.length);
            bufferedBytes.write(serializedPage);
            
            if (page instanceof UberPage && offsetToAdd > 0) {
                final byte[] bytesToAdd = new byte[(int) offsetToAdd];
                bufferedBytes.write(bytesToAdd);
            }
            
            // Flush if buffer is large enough
            if (bufferedBytes.writePosition() > FLUSH_SIZE) {
                flushBuffer(bufferedBytes);
            }
            
            // Remember page coordinates
            pageReference.setKey(offset);
            
            if (page instanceof KeyValueLeafPage keyValueLeafPage) {
                pageReference.setHash(keyValueLeafPage.getHashCode());
            } else {
                pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());
            }
            
            // Handle revision tracking
            if (serializationType == SerializationType.DATA) {
                if (page instanceof RevisionRootPage revisionRootPage) {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
                    buffer.putLong(offset);
                    buffer.position(8);
                    buffer.putLong(revisionRootPage.getRevisionTimestamp());
                    buffer.position(0);
                    final long revisionsFileOffset;
                    if (revisionRootPage.getRevision() == 0) {
                        revisionsFileOffset = revisionsFileChannel.size() + IOStorage.FIRST_BEACON;
                    } else {
                        revisionsFileOffset = revisionsFileChannel.size();
                    }
                    revisionsFileChannel.write(buffer, revisionsFileOffset);
                    final long currOffset = offset;
                    cache.put(revisionRootPage.getRevision(),
                              CompletableFuture.supplyAsync(() -> new RevisionFileData(currOffset,
                                                                                       Instant.ofEpochMilli(revisionRootPage.getRevisionTimestamp()))));
                } else if (page instanceof UberPage && isFirstUberPage) {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
                    buffer.put(serializedPage);
                    buffer.position(0);
                    revisionsFileChannel.write(buffer, 0);
                    buffer.position(0);
                    revisionsFileChannel.write(buffer, Writer.UBER_PAGE_BYTE_ALIGN);
                }
            }
            
            return this;
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }
    
    private void flushBuffer(BytesOut<?> bufferedBytes) throws IOException {
        if (bufferedBytes.writePosition() == 0) {
            return;
        }
        
        // Get data to write
        byte[] data = bufferedBytes.toByteArray();
        bufferedBytes.clear();
        
        // Calculate write position
        long writeOffset = dataFileChannel.size();
        if (writeOffset == 0) {
            writeOffset = IOStorage.FIRST_BEACON;
            writeOffset += (PAGE_FRAGMENT_BYTE_ALIGN - (writeOffset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));
        }
        
        // Write using memory-mapped I/O
        writeToMappedRegion(writeOffset, data);
    }
    
    @Override
    public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
                                         final PageReference pageReference,
                                         final Page page,
                                         final BytesOut<?> bufferedBytes) {
        try {
            // Flush any pending writes first
            flushBuffer(bufferedBytes);
            
            isFirstUberPage = dataFileChannel.size() == 0;
            
            pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, serializationType);
            final byte[] serializedPage = buildSerializedPage(resourceConfiguration, page);
            byteBufferBytes.clear();
            
            int offsetToAdd = UBER_PAGE_BYTE_ALIGN - ((serializedPage.length + IOStorage.OTHER_BEACON) % UBER_PAGE_BYTE_ALIGN);
            
            final long offset;
            if (!isFirstUberPage) {
                final var fileSize = dataFileChannel.size();
                offset = (fileSize % UBER_PAGE_BYTE_ALIGN == 0) 
                    ? fileSize + UBER_PAGE_BYTE_ALIGN 
                    : fileSize + (UBER_PAGE_BYTE_ALIGN - fileSize % UBER_PAGE_BYTE_ALIGN);
            } else {
                offset = IOStorage.FIRST_BEACON;
            }
            
            // Write length prefix
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
            lengthBuffer.putInt(serializedPage.length);
            lengthBuffer.flip();
            
            // Extend file and map for uber page write
            long totalSize = 4 + serializedPage.length + offsetToAdd;
            ensureMapped(offset, totalSize);
            
            long regionOffset = offset - mappedOffset;
            mappedRegion.set(ValueLayout.JAVA_INT_UNALIGNED, regionOffset, serializedPage.length);
            MemorySegment.copy(serializedPage, 0, mappedRegion, ValueLayout.JAVA_BYTE, regionOffset + 4, serializedPage.length);
            
            // Write padding
            if (offsetToAdd > 0) {
                // Just skip, memory is already zeroed
            }
            
            // Force sync
            mappedRegion.force();
            
            pageReference.setKey(offset);
            pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());
            
            // Update revision file
            if (serializationType == SerializationType.DATA && isFirstUberPage) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
                buffer.put(serializedPage);
                buffer.position(0);
                revisionsFileChannel.write(buffer, 0);
                buffer.position(0);
                revisionsFileChannel.write(buffer, Writer.UBER_PAGE_BYTE_ALIGN);
            }
            
            return this;
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }
    
    @Override
    public Writer truncateTo(final StorageEngineReader pageReadOnlyTrx, final int revision) {
        try {
            final var dataFileRevisionRootPageOffset =
                cache.get(revision, _ -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();
            
            final var buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());
            dataFileChannel.read(buffer, dataFileRevisionRootPageOffset);
            buffer.position(0);
            final int dataLength = buffer.getInt();
            
            // Close mapped region before truncating
            if (mappedArena != null) {
                mappedArena.close();
                mappedRegion = null;
                mappedArena = null;
            }
            
            dataFileChannel.truncate(dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON + dataLength);
        } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
            throw new IllegalStateException(e);
        }
        
        return this;
    }
    
    @Override
    public RevisionFileData getRevisionFileData(int revision) {
        return reader.getRevisionFileData(revision);
    }
    
    @Override
    public Writer truncate() {
        try {
            if (mappedArena != null) {
                mappedArena.close();
                mappedRegion = null;
                mappedArena = null;
            }
            dataFileChannel.truncate(0);
            revisionsFileChannel.truncate(0);
        } catch (IOException e) {
            throw new SirixIOException(e);
        }
        return this;
    }
    
    @Override
    public void close() {
        try {
            // Force sync and close mapped region
            if (mappedRegion != null) {
                mappedRegion.force();
            }
            if (mappedArena != null) {
                mappedArena.close();
                mappedRegion = null;
                mappedArena = null;
            }
            
            // Force channels
            if (dataFileChannel != null) {
                dataFileChannel.force(true);
            }
            if (revisionsFileChannel != null) {
                revisionsFileChannel.force(true);
            }
            
            // Close reader
            if (reader != null) {
                reader.close();
            }
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }
    
    @Override
    protected MMFileReader delegate() {
        return reader;
    }

    @Override
    public void forceAll() {
        try {
            if (mappedRegion != null) {
                mappedRegion.force();
            }
            if (dataFileChannel != null) {
                dataFileChannel.force(true);
            }
            if (revisionsFileChannel != null) {
                revisionsFileChannel.force(true);
            }
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }
}







