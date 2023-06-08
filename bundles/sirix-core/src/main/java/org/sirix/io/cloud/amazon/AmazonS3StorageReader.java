package org.sirix.io.cloud.amazon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.time.Instant;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.file.FileReader;
import org.sirix.io.filechannel.FileChannelReader;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3StorageReader implements Reader {

	/**
	 * S3 storage bucket name
	 * 
	 */
	private final String bucketName;

	private final S3Client s3Client;

	private final ResourceConfiguration resourceConfig;
	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AmazonS3StorageReader.class));

	private FileChannelReader reader;

	public AmazonS3StorageReader(String bucketName, S3Client s3Client, String dataFileKeyName,
			String revisionsOffsetFileKeyName, final ByteHandler byteHandler, final SerializationType serializationType,
			final PagePersister pagePersister, final Cache<Integer, RevisionFileData> cache,
			ResourceConfiguration resourceConfig) {
		this.bucketName = bucketName;
		this.s3Client = s3Client;
		this.resourceConfig = resourceConfig;
		Path dataFilePath = readObjectDataFromS3(dataFileKeyName);
		Path revisionOffsetFilePath = readObjectDataFromS3(revisionsOffsetFileKeyName);
		try {
			this.reader = new FileChannelReader(new RandomAccessFile(dataFilePath.toFile(), "r").getChannel(),
					new RandomAccessFile(revisionOffsetFilePath.toFile(), "r").getChannel(), byteHandler, serializationType,
					pagePersister, cache);
		} catch (IOException io) {
			LOGGER.error(io.getMessage());
			System.exit(1);
		}

	}

	/**
	 * @param keyName - Key name of the object to be read from S3 storage
	 * @return path - The location of the local file that contains the data that is
	 *         written to the file system storage in the system temp directory.
	 */
	protected Path readObjectDataFromS3(String keyName) {

		try {
			GetObjectRequest objectRequest = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();

			ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
			byte[] data = objectBytes.asByteArray();
			/*
			 * As the bucketName has to be same as the database name, it makes sense to
			 * use/create file on the local filesystem instead of in the tmp partition
			 */
			Path path = resourceConfig.resourcePath;
			// Write the data to a local file.
			File myFile = path.toFile();
			try (OutputStream os = new FileOutputStream(myFile)) {
				os.write(data);
			}
			return path;
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (S3Exception e) {
			LOGGER.error(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		return null;
	}

	ByteHandler getByteHandler() {
		return this.reader.getByteHandler();
	}

	HashFunction getHashFunction() {
		return this.reader.getHashFunction();
	}

	@Override
	public PageReference readUberPageReference() {
		return reader.readUberPageReference();
	}

	@Override
	public Page read(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx) {
		return reader.read(key, pageReadTrx);
	}

	@Override
	public void close() {
		s3Client.close();
		reader.close();
	}

	@Override
	public RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx) {
		return reader.readRevisionRootPage(revision, pageReadTrx);
	}

	@Override
	public Instant readRevisionRootPageCommitTimestamp(int revision) {
		return reader.readRevisionRootPageCommitTimestamp(revision);
	}

	@Override
	public RevisionFileData getRevisionFileData(int revision) {
		return reader.getRevisionFileData(revision);
	}

}
