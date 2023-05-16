package org.sirix.io.cloud.amazon;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.io.file.FileReader;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
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

		/** Logger. */
	  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AmazonS3StorageWriter.class));

	public AmazonS3StorageWriter (final String dataFileKeyName, final String revisionsOffsetFileKeyName,
		      final SerializationType serializationType, final PagePersister pagePersister,
		      final AsyncCache<Integer, RevisionFileData> cache, final AmazonS3StorageReader reader,
		      final S3Client s3Client) throws FileNotFoundException {
		this.dataFile = new RandomAccessFile(requireNonNull(reader.readObjectDataFromS3(dataFileKeyName)).toFile(),"rw");
		type = requireNonNull(serializationType);
	    this.revisionsFile = type == SerializationType.DATA ? new RandomAccessFile(requireNonNull(reader.readObjectDataFromS3(revisionsOffsetFileKeyName)).toFile(),"rw") : null;
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
	protected void writeObjectToS3(String bucketName, String keyName, File object, boolean isDataFile) {
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
	public PageReference readUberPageReference() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Page read(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub		
	}

	@Override
	public RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Instant readRevisionRootPageCommitTimestamp(int revision) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RevisionFileData getRevisionFileData(int revision) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Writer write(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference, Bytes<ByteBuffer> bufferedBytes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Writer writeUberPageReference(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference,
			Bytes<ByteBuffer> bufferedBytes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Writer truncateTo(PageReadOnlyTrx pageReadOnlyTrx, int revision) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Writer truncate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Reader delegate() {
		// TODO Auto-generated method stub
		return null;
	}
}
