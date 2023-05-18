package org.sirix.io.cloud.amazon;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.page.KeyValueLeafPage;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;

import net.openhft.chronicle.bytes.Bytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3StorageWriter extends AbstractForwardingReader implements Writer {

	/**
	   * Random access to work on.
	   */
	  private RandomAccessFile dataFile;

	  /**
	   * {@link AmazonS3StorageReader} reference for this writer.
	   */
	  private final AmazonS3StorageReader reader;

	  private final SerializationType type;

	  private RandomAccessFile revisionsFile;

	  private final PagePersister pagePersister;

	  private final AsyncCache<Integer, RevisionFileData> cache;

	  private boolean isFirstUberPage;

	  private final Bytes<ByteBuffer> byteBufferBytes = Bytes.elasticByteBuffer(1_000);

	  private final S3Client s3Client;

	  private final String bucketName;

		/** Logger. */
	  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AmazonS3StorageWriter.class));

	public AmazonS3StorageWriter (final String dataFileKeyName, final String revisionsOffsetFileKeyName,
			  final String bucketName,
		      final SerializationType serializationType, final PagePersister pagePersister,
		      final AsyncCache<Integer, RevisionFileData> cache, final AmazonS3StorageReader reader,
		      final S3Client s3Client) throws FileNotFoundException {
		this.bucketName = bucketName;
		this.dataFile = new RandomAccessFile(requireNonNull(reader.readObjectDataFromS3(dataFileKeyName)).toFile(),"rw");
		type = requireNonNull(serializationType);
	    this.revisionsFile = type == SerializationType.DATA ? 
	            new RandomAccessFile(requireNonNull(reader.readObjectDataFromS3(revisionsOffsetFileKeyName)).toFile(),"rw") : null;
	    this.pagePersister = requireNonNull(pagePersister);
	    this.cache = cache;
	    this.reader = requireNonNull(reader);
	    this.s3Client = s3Client;
	}

	/**
	 * @param bucketName - S3 bucket name on AWS
	 * @param keyName - Name of the file that includes the full path that is supposed to be used on the local file system
	 * @param object - File that could be read from the local filesystem that contains the actual information
	 * to be stored on S3
	 * 
	 * The expectation is that user provides a File object which will contain the data that needs to backed up to the remote
	 * storage i.e. AWS S3 in this case 
	 * */
	protected void writeObjectToS3(String keyName, File object, boolean isDataFile) {
		try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-sirix", isDataFile ? "data" : "revision");
            PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .metadata(metadata)
                .build();

            s3Client.putObject(putOb, RequestBody.fromFile(object));
        } catch (S3Exception e) {
            LOGGER.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}


	@Override
	public void close() {
		try {
		      if (dataFile != null) {
		        dataFile.close();
		      }
		      if (revisionsFile != null) {
		        revisionsFile.close();
		      }
		      if (reader != null) {
		        reader.close();
		      }
		      this.s3Client.close();
		    } catch (final IOException  e) {
		      throw new SirixIOException(e);
		    }
	}

	@Override
	public Writer write(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference, Bytes<ByteBuffer> bufferedBytes) {
		try {
		      final long fileSize = dataFile.length();
		      long offset = fileSize == 0 ? IOStorage.FIRST_BEACON : fileSize;
		      return writePageReference(pageReadOnlyTrx, pageReference, offset);
		    } catch (final IOException e) {
		      throw new SirixIOException(e);
		    }
	}


	private String getFileKeyName(String fileDescriptorPath) {
		return fileDescriptorPath.substring((System.getProperty("java.io.tmpdir")+FileSystems.getDefault().getSeparator()).length());
	}
	@NotNull
	  private AmazonS3StorageWriter writePageReference(final PageReadOnlyTrx pageReadOnlyTrx, final PageReference pageReference,
	      long offset) {
	    // Perform byte operations.
	    try {
	      // Serialize page.
	      final Page page = pageReference.getPage();

	      final byte[] serializedPage;

	      try (final ByteArrayOutputStream output = new ByteArrayOutputStream(1_000);
	           final DataOutputStream dataOutput = new DataOutputStream(reader.getByteHandler().serialize(output))) {
	        pagePersister.serializePage(pageReadOnlyTrx, byteBufferBytes, page, type);
	        final var byteArray = byteBufferBytes.toByteArray();
	        dataOutput.write(byteArray);
	        dataOutput.flush();
	        serializedPage = output.toByteArray();
	      }

	      byteBufferBytes.clear();

	      final byte[] writtenPage = new byte[serializedPage.length + IOStorage.OTHER_BEACON];
	      final ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
	      buffer.putInt(serializedPage.length);
	      buffer.put(serializedPage);
	      buffer.flip();
	      buffer.get(writtenPage);

	      // Getting actual offset and appending to the end of the current file.
	      if (type == SerializationType.DATA) {
	        if (page instanceof RevisionRootPage) {
	          if (offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
	            offset += REVISION_ROOT_PAGE_BYTE_ALIGN - (offset % REVISION_ROOT_PAGE_BYTE_ALIGN);
	          }
	        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
	          offset += PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN);
	        }
	      }
	      dataFile.seek(offset);
	      dataFile.write(writtenPage);
	      /*Write the file object to S3*/
	      this.writeObjectToS3(this.getFileKeyName(dataFile.getFD().toString()), new File(dataFile.getFD().toString()), Boolean.TRUE);

	      // Remember page coordinates.
	      pageReference.setKey(offset);

	      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
	        pageReference.setHash(keyValueLeafPage.getHashCode());
	      } else {
            /*TODO : Check for correctness of this*/
	        pageReference.setHash(reader.getHashFunction().hashBytes(serializedPage).asBytes());
	      }

	      if (type == SerializationType.DATA) {
	        if (page instanceof RevisionRootPage revisionRootPage) {
	          if (revisionRootPage.getRevision() == 0) {
	            revisionsFile.seek(revisionsFile.length() + IOStorage.FIRST_BEACON);
	          } else {
	            revisionsFile.seek(revisionsFile.length());
	          }
	          revisionsFile.writeLong(offset);
	          revisionsFile.writeLong(revisionRootPage.getRevisionTimestamp());
	          if (cache != null) {
	            final long currOffset = offset;
	            cache.put(revisionRootPage.getRevision(),
	                      CompletableFuture.supplyAsync(() -> new RevisionFileData(currOffset,
	                                                                               Instant.ofEpochMilli(revisionRootPage.getRevisionTimestamp()))));
	          }
	        } else if (page instanceof UberPage && isFirstUberPage) {
	          revisionsFile.seek(0);
	          revisionsFile.write(serializedPage);
	          revisionsFile.seek(IOStorage.FIRST_BEACON >> 1);
	          revisionsFile.write(serializedPage);
	        }
	        this.writeObjectToS3(this.getFileKeyName(revisionsFile.getFD().toString()), new File(revisionsFile.getFD().toString()), Boolean.FALSE);
	      }

	      return this;
	    } catch (final IOException e) {
	      throw new SirixIOException(e);
	    }
	  }

	@Override
	public Writer writeUberPageReference(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference,
			Bytes<ByteBuffer> bufferedBytes) {
		isFirstUberPage = true;
	    writePageReference(pageReadOnlyTrx, pageReference, 0);
	    isFirstUberPage = false;
	    writePageReference(pageReadOnlyTrx, pageReference, 100);
	    return this;
	}

	@Override
	public Writer truncateTo(PageReadOnlyTrx pageReadOnlyTrx, int revision) {
		try {
		      final var dataFileRevisionRootPageOffset =
		          cache.get(revision, (unused) -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

		      // Read page from file.
		      dataFile.seek(dataFileRevisionRootPageOffset);
		      final int dataLength = dataFile.readInt();

		      dataFile.getChannel().truncate(dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON + dataLength);
		      this.writeObjectToS3(getFileKeyName(dataFile.getFD().toString()), new File(dataFile.getFD().toString()), Boolean.TRUE);
		    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
		      throw new IllegalStateException(e);
		    }

		    return this;
	}

	@Override
	public Writer truncate() {
		try {
		      dataFile.setLength(0);
		      this.writeObjectToS3(getFileKeyName(dataFile.getFD().toString()), new File(dataFile.getFD().toString()), Boolean.TRUE);
		      if (revisionsFile != null) {
		        revisionsFile.setLength(0);
		        this.writeObjectToS3(getFileKeyName(revisionsFile.getFD().toString()), new File(revisionsFile.getFD().toString()), Boolean.FALSE);
		      }
		    } catch (final IOException e) {
		      throw new SirixIOException(e);
		    }

		    return this;
	}

	@Override
	protected Reader delegate() {
		return this.reader;
	}
}
