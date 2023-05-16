package org.sirix.io.cloud.amazon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.Instant;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.file.FileReader;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

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
	private String bucketName;

	private S3Client s3Client;

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AmazonS3StorageReader.class));
	

	private FileReader reader;
	
	public AmazonS3StorageReader(String bucketName, String keyName, 
			S3Client s3Client, 
			final RandomAccessFile dataFile, 
			final RandomAccessFile revisionsOffsetFile,
			final ByteHandler byteHandler, 
			final SerializationType serializationType, 
			final PagePersister pagePersister,
		    final Cache<Integer, RevisionFileData> cache) {
		this.bucketName = bucketName;
		this.s3Client = s3Client;
		this.reader = new FileReader(dataFile,
				revisionsOffsetFile,
				byteHandler,
				serializationType,
				pagePersister,
                cache);
	}
	
	/**
	 * @param keyName - Key name of the object to be read from S3 storage
	 * @return path - The location of the local file that contains the data that is written to the file system storage 
	 * in the system temp directory.
	 */
	protected Path readObjectDataFromS3(String keyName) {
		
		try {
            GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(keyName)
                .bucket(bucketName)
                .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            String path = System.getProperty("java.io.tmpdir")	+ FileSystems.getDefault().getSeparator() + keyName;
            // Write the data to a local file.
            File myFile = new File(path);
            try(OutputStream os = new FileOutputStream(myFile)){
                os.write(data);
            }
            return Path.of(path);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
            LOGGER.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
		return null;
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
