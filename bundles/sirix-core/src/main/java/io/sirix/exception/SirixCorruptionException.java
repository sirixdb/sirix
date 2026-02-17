package io.sirix.exception;

import io.sirix.io.PageHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception thrown when page data corruption is detected during checksum verification.
 * 
 * <p>
 * This exception indicates that the data read from storage does not match the expected checksum,
 * which typically means:
 * <ul>
 * <li>Storage media corruption (bit rot, disk failure)</li>
 * <li>Incomplete write (crash during commit)</li>
 * <li>External modification of data files</li>
 * </ul>
 * </p>
 * 
 * <p>
 * In a single-node deployment, corruption cannot be repaired automatically. The application should
 * consider:
 * </p>
 * <ul>
 * <li>Failing fast and alerting operators</li>
 * <li>Attempting to read from an earlier revision (older data may be intact)</li>
 * <li>Restoring from backup</li>
 * </ul>
 */
public final class SirixCorruptionException extends SirixRuntimeException {

  private static final Logger LOGGER = LoggerFactory.getLogger(SirixCorruptionException.class);

  private static final long serialVersionUID = 1L;

  /**
   * The storage key (offset) of the corrupted page.
   */
  private final long pageKey;

  /**
   * Description of where the corruption occurred (e.g., "compressed", "uncompressed").
   */
  private final String context;

  /**
   * The expected hash bytes (from PageReference).
   */
  private final byte[] expectedHash;

  /**
   * The actual hash bytes computed from the read data.
   */
  private final byte[] actualHash;

  /**
   * Create a new corruption exception with detailed information.
   * 
   * <p>
   * The constructor logs the error at ERROR level with hex-encoded hashes for debugging purposes.
   * </p>
   *
   * @param pageKey storage key (byte offset) of the corrupted page
   * @param context description of where corruption was detected
   * @param expectedHash the expected hash from the page reference
   * @param actualHash the actual hash computed from read data
   */
  public SirixCorruptionException(long pageKey, String context, byte[] expectedHash, byte[] actualHash) {
    super(buildMessage(pageKey, context, expectedHash, actualHash));
    this.pageKey = pageKey;
    this.context = context;
    this.expectedHash = expectedHash != null
        ? expectedHash.clone()
        : null;
    this.actualHash = actualHash != null
        ? actualHash.clone()
        : null;

    // Log detailed error information
    LOGGER.error("PAGE CORRUPTION DETECTED: key={}, context={}, expected={}, actual={}", pageKey, context,
        PageHasher.toHexString(expectedHash), PageHasher.toHexString(actualHash));
  }

  /**
   * Create a new corruption exception with a custom message.
   *
   * @param pageKey storage key of the corrupted page
   * @param message custom error message
   */
  public SirixCorruptionException(long pageKey, String message) {
    super(message);
    this.pageKey = pageKey;
    this.context = "unknown";
    this.expectedHash = null;
    this.actualHash = null;

    LOGGER.error("PAGE CORRUPTION DETECTED: key={}, message={}", pageKey, message);
  }

  private static String buildMessage(long pageKey, String context, byte[] expectedHash, byte[] actualHash) {
    return String.format("Page corruption detected at key %d (%s): expected hash %s, actual hash %s", pageKey, context,
        PageHasher.toHexString(expectedHash), PageHasher.toHexString(actualHash));
  }

  /**
   * Get the storage key (byte offset) of the corrupted page.
   * 
   * @return the page key
   */
  public long getPageKey() {
    return pageKey;
  }

  /**
   * Get the context description of where corruption was detected.
   * 
   * @return context string (e.g., "compressed", "uncompressed")
   */
  public String getContext() {
    return context;
  }

  /**
   * Get the expected hash bytes.
   * 
   * @return copy of expected hash, or null if not available
   */
  public byte[] getExpectedHash() {
    return expectedHash != null
        ? expectedHash.clone()
        : null;
  }

  /**
   * Get the actual hash bytes computed from the read data.
   * 
   * @return copy of actual hash, or null if not available
   */
  public byte[] getActualHash() {
    return actualHash != null
        ? actualHash.clone()
        : null;
  }
}

