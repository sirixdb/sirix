package io.sirix.exception;

/** A valid store requires a newer implementation; this is never corruption to auto-repair. */
public final class UnsupportedStoreFeatureException extends UnsupportedOperationException {
  public UnsupportedStoreFeatureException(final String message) {
    super(message);
  }
}
