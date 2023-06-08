package org.sirix.io.cloud.amazon;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.sirix.access.ResourceConfiguration;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.ByteHandlerPipeline;
import org.sirix.io.cloud.ICloudStorage;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

/**
 * Factory to provide Amazon S3 as storage backend
 * 
 * @Auther Sanket Band (@sband)
 **/

public final class AmazonS3Storage implements ICloudStorage {

	/**
	 * Data file name.
	 */
	private static final String FILENAME = "sirix.data";

	/**
	 * Revisions file name.
	 */
	private static final String REVISIONS_FILENAME = "sirix.revisions";

	/**
	 * Instance to local storage.
	 */
	private final Path file;

	private S3Client s3Client;

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AmazonS3Storage.class));

	/**
	 * Byte handler pipeline.
	 */
	private final ByteHandlerPipeline byteHandlerPipeline;

	/**
	 * Revision file data cache.
	 */
	private final AsyncCache<Integer, RevisionFileData> cache;

	private ResourceConfiguration.AWSStorageInformation awsStorageInfo;

	private final AmazonS3StorageReader reader;

	/**
	 * Support AWS authentication only with .aws credentials file with the required
	 * profile name from the creds file
	 */
	public AmazonS3Storage(final ResourceConfiguration resourceConfig, AsyncCache<Integer, RevisionFileData> cache) {
		this.awsStorageInfo = resourceConfig.awsStoreInfo;
		this.cache = cache;
		this.byteHandlerPipeline = resourceConfig.byteHandlePipeline;
		this.file = resourceConfig.resourcePath;
		this.s3Client = getS3Client(); // this client is needed for the below checks, so initialize it here only.
		String bucketName = awsStorageInfo.getBucketName();
		boolean shouldCreateBucketIfNotExists = awsStorageInfo.shouldCreateBucketIfNotExists();
		if (!isBucketExists(bucketName) && shouldCreateBucketIfNotExists) {
			createBucket(bucketName);
		}
		this.reader = new AmazonS3StorageReader(bucketName, s3Client, getDataFilePath().toAbsolutePath().toString(),
				getRevisionFilePath().toAbsolutePath().toString(), new ByteHandlerPipeline(this.byteHandlerPipeline),
				SerializationType.DATA, new PagePersister(), cache.synchronous(), resourceConfig);
	}

	void createBucket(String bucketName) {
		try {
			S3Waiter s3Waiter = s3Client.waiter();
			CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();

			s3Client.createBucket(bucketRequest);
			HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder().bucket(bucketName).build();

			WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
			if (waiterResponse.matched().response().isPresent()) {
				LOGGER.info(String.format("S3 bucket: %s has been created.", bucketName));
			}
		} catch (S3Exception e) {
			LOGGER.error(e.awsErrorDetails().errorMessage());
			LOGGER.error(String.format("Bucket: %s could not be created. Will not consume S3 storage", bucketName));
			System.exit(1);
		}
	}

	boolean isBucketExists(String bucketName) {
		HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucketName).build();

		try {
			s3Client.headBucket(headBucketRequest);
			return true;
		} catch (NoSuchBucketException e) {
			return false;
		}
	}

	S3Client getS3Client() {
		return this.s3Client == null
				? S3Client.builder().region(Region.of(awsStorageInfo.getAwsRegion()))
						.credentialsProvider(ProfileCredentialsProvider.create(awsStorageInfo.getAwsProfile())).build()
				: this.s3Client;
	}

	S3AsyncClient getAsyncS3Client() {
		return S3AsyncClient.builder().region(Region.of(awsStorageInfo.getAwsRegion()))
				.credentialsProvider(ProfileCredentialsProvider.create(awsStorageInfo.getAwsProfile())).build();
	}

	@Override
	public Writer createWriter() {
		return new AmazonS3StorageWriter(getDataFilePath().toAbsolutePath().toString(),
				getRevisionFilePath().toAbsolutePath().toString(), awsStorageInfo.getBucketName(),
				SerializationType.DATA, new PagePersister(), cache, reader, this.getAsyncS3Client());
	}

	@Override
	public Reader createReader() {
		return this.reader;
	}

	@Override
	public void close() {

	}

	@Override
	public boolean exists() {
		Path storage = this.reader.readObjectDataFromS3(getDataFilePath().toAbsolutePath().toString());
		try {
			return Files.exists(storage) && Files.size(storage) > 0;
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public ByteHandler getByteHandler() {
		return this.byteHandlerPipeline;
	}

	/**
	 * Getting path for data file. This path would be used on the local storage
	 * 
	 * @return the path for this data file
	 */
	private Path getDataFilePath() {
		return file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(FILENAME);
	}

	/**
	 * Getting concrete storage for this file.
	 *
	 * @return the concrete storage for this database
	 */
	private Path getRevisionFilePath() {
		return file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(REVISIONS_FILENAME);
	}
}
