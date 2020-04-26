package org.sirix.io.bytepipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Objects;
import org.sirix.access.ResourceConfiguration;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.config.TinkConfig;
import com.google.crypto.tink.streamingaead.StreamingAeadFactory;

/**
 * Decorator for encrypting any content.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class Encryptor implements ByteHandler {

  static {
    try {
      TinkConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final byte[] mAssociatedData = {};

  private StreamingAead mStreamingAead;

  private KeysetHandle mKeySetHandle;

  private final Path mResourcePath;

  public Encryptor(final Path resourcePath) {
    mResourcePath = Objects.requireNonNull(resourcePath);
  }

  /**
   * @return the resource path
   */
  public Path getResourcePath() {
    return mResourcePath;
  }

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    try {
      final StreamingAead aead = getStreamingAead();

      return aead.newEncryptingStream(toSerialize, mAssociatedData);
    } catch (final GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private StreamingAead getStreamingAead() throws GeneralSecurityException {
    if (mStreamingAead == null)
      mStreamingAead = StreamingAeadFactory.getPrimitive(getKeysetHandle());
    return mStreamingAead;
  }

  private KeysetHandle getKeysetHandle() {
    if (mKeySetHandle == null)
      mKeySetHandle = getKeysetHandle(
          mResourcePath.resolve(ResourceConfiguration.ResourcePaths.ENCRYPTION_KEY.getPath())
                       .resolve("encryptionKey.json"));
    return mKeySetHandle;
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    try {
      final StreamingAead aead = getStreamingAead();

      return aead.newDecryptingStream(toDeserialize, mAssociatedData);
    } catch (final GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static ByteHandler create(Path resourcePath) {
    return createInstance(resourcePath);
  }

  private static ByteHandler createInstance(Path resourcePath) {
    return new Encryptor(resourcePath);
  }

  @Override
  public int hashCode() {
    return mResourcePath.hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof Encryptor))
      return false;

    final Encryptor otherEncryptor = (Encryptor) other;
    return mResourcePath.equals(otherEncryptor.mResourcePath);
  }

  @Override
  public ByteHandler getInstance() {
    return createInstance(mResourcePath);
  }

  /* Loads a KeysetHandle from {@code keyPath} or generate a new one if it doesn't exist. */
  private static KeysetHandle getKeysetHandle(Path keyPath) {
    try {
      if (Files.exists(keyPath)) {
        // Read the cleartext keyset from disk.
        // WARNING: reading cleartext keysets is a bad practice. Tink supports reading/writing
        // encrypted keysets, see
        // https://github.com/google/tink/blob/master/docs/JAVA-HOWTO.md#loading-existing-keysets.
        return CleartextKeysetHandle.read(JsonKeysetReader.withPath(keyPath));
      }

      throw new AssertionError("No file for encryption key found.");
    } catch (final GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
