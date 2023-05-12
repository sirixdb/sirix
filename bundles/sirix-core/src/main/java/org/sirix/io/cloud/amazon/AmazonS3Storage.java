package org.sirix.io.cloud.amazon;

import java.nio.file.Path;

import org.sirix.access.ResourceConfiguration;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.ByteHandlerPipeline;
import org.sirix.io.cloud.ICloudStorage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
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

public class AmazonS3Storage implements ICloudStorage {

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

	/**
	 * S3 storage bucket name
	 * 
	*/
	private String bucketName;

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

	/**
	 * Support AWS authentication only with .aws credentials file with the required
	 * profile name from the creds file
	 */
	public AmazonS3Storage(String bucketName, String awsProfile,
			String region,
			boolean shouldCreateBucketIfNotExists, final ResourceConfiguration resourceConfig,
			AsyncCache<Integer, RevisionFileData> cache,
			ByteHandlerPipeline byteHandlerPipeline) {
		this.bucketName = bucketName;
		this.s3Client = this.getS3Client(awsProfile,region);
		/*
		 * If the bucket does not exist, should create a new bucket based on the boolean
		 */
		/*
		 * Exit the system if the cloud storage bucket cannot be created Alternatively,
		 * we could just set a flag that could be checked before creating a reader or
		 * writer. Return null if the bucket is not created OR does not exist But that
		 * would keep the user under false impression that the bucket is created OR
		 * exists already even if it does not exists
		 */
		if (!isBucketExists(bucketName, s3Client)) {
			if (shouldCreateBucketIfNotExists) {
				createBucket(bucketName, s3Client);
			} else {
				LOGGER.error(String.format("Bucket: %s, does not exists on Amazon S3 storage, exiting the system",
						bucketName));
				System.exit(1);
			}
		}
		this.cache = cache;
		this.byteHandlerPipeline = byteHandlerPipeline; 
		this.file = resourceConfig.resourcePath;
	}

	private void createBucket(String bucketName, S3Client s3Client) {
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

	private boolean isBucketExists(String bucketName, S3Client s3Client) {
		HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucketName).build();

		try {
			s3Client.headBucket(headBucketRequest);
			return true;
		} catch (NoSuchBucketException e) {
			return false;
		} 
	}

	private S3Client getS3Client(String awsProfile, String region) {
		S3Client s3Client = null;
		s3Client = S3Client.builder()
	            .region(Region.of(region))
	            .credentialsProvider(ProfileCredentialsProvider.create(awsProfile))
	            .build();
		return s3Client;
	}

	@Override
	public Writer createWriter() {
		/*
		 * This would create a writer that connects to the
		 * remote storagee*/
		return null;
	}

	@Override
	public Reader createReader() {
		/*
		 * This would create a reader that connects to the 
		 * remote storage on cloud
		 * */
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean exists() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ByteHandler getByteHandler() {
		return this.byteHandlerPipeline;
	}

	/**
	 * Getting path for data file.
	 * This path would be used on the local storage
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
