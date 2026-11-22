package io.sirix.exception;

/** Unsupported persisted metadata is never corruption to auto-repair. */
public class UnsupportedStoreFeatureException extends UnsupportedOperationException {
  public UnsupportedStoreFeatureException(final String message) {
    super(message);
  }
}
