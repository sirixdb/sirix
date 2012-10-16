/**
 * 
 */
package org.sirix.io.bytepipe;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.sirix.exception.SirixIOException;

/**
 * Decorator for encrypting any content.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class Encryptor implements ByteHandler {

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
   * @throws SirixIOException
   *           if an I/O error occurs
   */
  public Encryptor() {
    try {
      mCipher = Cipher.getInstance(ALGORITHM);
      mKey = new SecretKeySpec(KEYVALUE, "AES");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    } catch (final NoSuchPaddingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws SirixIOException {
    try {
      mCipher.init(Cipher.ENCRYPT_MODE, mKey);

      byte[] toEncrypt = pToSerialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] encValue = mCipher.doFinal(toEncrypt);
        toEncrypt = encValue;
      }
      return toEncrypt;
    } catch (final GeneralSecurityException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws SirixIOException {
    try {
      mCipher.init(Cipher.DECRYPT_MODE, mKey);

      byte[] toDecrypt = pToDeserialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] decValue = mCipher.doFinal(toDecrypt);
        toDecrypt = decValue;
      }
      return toDecrypt;

    } catch (final InvalidKeyException e) {
      throw new SirixIOException(e);
    } catch (final GeneralSecurityException e) {
      throw new SirixIOException(e);
    }
  }
  
  @Override
  public ByteHandler getInstance() {
    return new Encryptor();
  }
}
