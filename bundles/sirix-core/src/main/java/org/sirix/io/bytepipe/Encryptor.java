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

import org.sirix.exception.TTByteHandleException;

/**
 * Decorator for encrypting any content.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class Encryptor implements IByteHandler {

  private static final String ALGORITHM = "AES";
  private static final int ITERATIONS = 2;
  // 128bit key
  private static final byte[] keyValue = new byte[] {
    'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k',
    'k'
  };
  private final Cipher mCipher;

  private final Key key;

  /**
   * Constructor.
   * 
   * @throws TTByteHandleException
   */
  public Encryptor() throws TTByteHandleException {
    try {
      mCipher = Cipher.getInstance(ALGORITHM);
      key = new SecretKeySpec(keyValue, ALGORITHM);
    } catch (final NoSuchAlgorithmException e) {
      throw new TTByteHandleException(e);
    } catch (final NoSuchPaddingException e) {
      throw new TTByteHandleException(e);
    }
  }

  @Override
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws TTByteHandleException {
    try {
      mCipher.init(Cipher.ENCRYPT_MODE, key);

      byte[] toEncrypt = pToSerialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] encValue = mCipher.doFinal(toEncrypt);
        toEncrypt = encValue;
      }
      return toEncrypt;
    } catch (final GeneralSecurityException e) {
      throw new TTByteHandleException(e);
    }
  }

  @Override
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws TTByteHandleException {
    try {
      mCipher.init(Cipher.DECRYPT_MODE, key);

      byte[] toDecrypt = pToDeserialize;
      for (int i = 0; i < ITERATIONS; i++) {
        byte[] decValue = mCipher.doFinal(toDecrypt);
        toDecrypt = decValue;
      }
      return toDecrypt;

    } catch (final InvalidKeyException e) {
      throw new TTByteHandleException(e);
    } catch (final GeneralSecurityException e) {
      throw new TTByteHandleException(e);
    }

  }
}
