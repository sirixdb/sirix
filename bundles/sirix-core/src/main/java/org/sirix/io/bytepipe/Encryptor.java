/**
 * 
 */
package org.sirix.io.bytepipe;

import static com.google.common.base.Preconditions.checkNotNull;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.sirix.exception.TTIOException;

/**
 * Decorator for encrypting any content.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class Encryptor implements IByteHandler {

  /** Algorithm to use. */
  private static final String ALGORITHM = "AES";

  /** Iterations. */
  private static final int ITERATIONS = 2;

  /** Cipher to perform encryption and decryption operations. */
  private final Cipher mCipher;

  /** Key for access data. */
  private final Key mKey;

  /** 128bit key. */
  private static final byte[] KEYVALUE = new byte[] {
    'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k',
    'k'
  };

  /**
   * Constructor.
   * 
   * @param pComponent
   * @throws TTByteHandleException
   */
  public Encryptor() throws TTIOException {
    this(new SecretKeySpec(KEYVALUE, "AES"));
  }

  /**
   * Constructor.
   * 
   * @param
   * 
   * @throws TTIOException
   *           if an I/O error occurs
   */
  public Encryptor(final @Nonnull Key pKey) throws TTIOException {
    try {
      mCipher = Cipher.getInstance(ALGORITHM);
      mKey = checkNotNull(pKey);
    } catch (final NoSuchAlgorithmException e) {
      throw new TTIOException(e);
    } catch (final NoSuchPaddingException e) {
      throw new TTIOException(e);
    }
  }

  @Override
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws TTIOException {
    try {
      mCipher.init(Cipher.ENCRYPT_MODE, mKey);

      byte[] toEncrypt = pToSerialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] encValue = mCipher.doFinal(toEncrypt);
        toEncrypt = encValue;
      }
      return toEncrypt;
    } catch (final GeneralSecurityException e) {
      throw new TTIOException(e);
    }
  }

  @Override
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws TTIOException {
    try {
      mCipher.init(Cipher.DECRYPT_MODE, mKey);

      byte[] toDecrypt = pToDeserialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] decValue = mCipher.doFinal(toDecrypt);
        toDecrypt = decValue;
      }
      return toDecrypt;

    } catch (final InvalidKeyException e) {
      throw new TTIOException(e);
    } catch (final GeneralSecurityException e) {
      throw new TTIOException(e);
    }

  }
}
