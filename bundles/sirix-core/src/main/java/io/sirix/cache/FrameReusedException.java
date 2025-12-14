package io.sirix.cache;

/**
 * Exception thrown when a page frame is reused while a PageGuard is active.
 * <p>
 * This indicates that the buffer manager evicted and recycled the page frame
 * (incrementing its version counter) while a transaction was still holding a guard.
 * The caller should retry the operation to reload the correct page.
 * <p>
 * This follows the LeanStore/Umbra pattern of optimistic page access with version checking.
 *
 * @author Johannes Lichtenberger
 */
public class FrameReusedException extends RuntimeException {

  /**
   * Create a new FrameReusedException.
   *
   * @param message the detail message
   */
  public FrameReusedException(String message) {
    super(message);
  }

  /**
   * Create a new FrameReusedException.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public FrameReusedException(String message, Throwable cause) {
    super(message, cause);
  }
}

