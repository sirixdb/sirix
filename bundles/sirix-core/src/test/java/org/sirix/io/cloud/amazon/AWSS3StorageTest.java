package org.sirix.io.cloud.amazon;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.XmlTestHelper;
import org.sirix.XmlTestHelper.PATHS;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.io.StorageType;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class AWSS3StorageTest {

	private AmazonS3Storage awsStorage;
	private S3Client s3Client;
	private AmazonS3StorageWriter cloudWriter;
	private AmazonS3StorageReader cloudReader;
	
	@Before
	public void setup() {
		final ResourceConfiguration.Builder resourceConfig = new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE);
	    resourceConfig.storageType(StorageType.CLOUD);
	    Database<XmlResourceSession> xmlDatabase = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
	    resourceConfig.awsStoreInfo(new ResourceConfiguration.AWSStorageInformation("default", 
	    		Region.US_EAST_1.id(), xmlDatabase.getName(), true));
	    ResourceConfiguration testResources  = resourceConfig.build();
	    S3Mock api = S3Mock.create(8001, ".");
		api.start();
		s3Client = S3Client.builder().region(Region.of(testResources.awsStoreInfo.getAwsRegion()))
				.credentialsProvider(AnonymousCredentialsProvider.create())
				.dualstackEnabled(true)
				.endpointOverride(URI.create("http://127.0.0.1:8001"))
				.build();
		testResources.resourcePath = PATHS.PATH1.getFile();
		
		awsStorage = (AmazonS3Storage)StorageType.CLOUD.getInstance(testResources);
		awsStorage.setS3Client(s3Client);
		cloudWriter = (AmazonS3StorageWriter)awsStorage.createWriter();
		cloudReader = (AmazonS3StorageReader)awsStorage.createReader();
	}
	
	@Test
	public void testS3StorageWriterNotNull() {
		assertNotNull(cloudWriter);
	}
	
	@Test
	public void testS3StorageReaderNotNull() {
		assertNotNull(cloudReader);
	}
	
	@Test
	public void testCreateBucket() {
		awsStorage.createBucket();
		assertTrue(awsStorage.isBucketExists());
	}
	
	
	@After
	public void tearDown() {
		XmlTestHelper.deleteEverything();
		s3Client.close();
	}
	
	
}