package org.sirix.io.bytepipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Objects;
import org.sirix.access.conf.ResourceConfiguration;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.config.TinkConfig;
import com.google.crypto.tink.streamingaead.StreamingAeadFactory;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;

/**
 * Decorator for encrypting any content.
 *
 * @author Johannes Lichtenberger <johannes.lichtenberger@sirix.io>
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
      initKeysetHandle();

      final StreamingAead aead = StreamingAeadFactory.getPrimitive(mKeySetHandle);

      return aead.newEncryptingStream(toSerialize, mAssociatedData);
    } catch (final GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void initKeysetHandle() {
    if (mKeySetHandle == null) {
      mKeySetHandle = getKeysetHandle(
          mResourcePath.resolve(ResourceConfiguration.ResourcePaths.ENCRYPTION_KEY.getPath())
                       .resolve("encryptionKey.json"));
    }
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    try {
      initKeysetHandle();

      final StreamingAead aead = StreamingAeadFactory.getPrimitive(mKeySetHandle);

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

      Files.createFile(keyPath);
      final KeysetHandle handle =
          KeysetHandle.generateNew(StreamingAeadKeyTemplates.AES256_CTR_HMAC_SHA256_4KB);
      CleartextKeysetHandle.write(handle, JsonKeysetWriter.withPath(keyPath));
      return handle;
    } catch (final GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
