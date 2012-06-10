/**
 * Copyright (c) 2011, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */
package org.sirix.encryption;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Helper class to provide operations for node encryption and decrpytion.
 * 
 * @author Patrick Lang, University of Konstanz
 * 
 */
public class NodeEncryption {

  /**
   * Encryption algorithm.
   */
  protected static final String ENCRYPTION_TYPE = "AES";

  /**
   * Encryption algorithm padding type.
   */
  protected static final String ENCRYPTION_PADDING_TYPE = "AES/CBC/PKCS5Padding";

  /**
   * Number of bits for encryption.
   */
  protected static final int ENCRYPTION_BITS = 128;

  /**
   * Encrypt a node.
   * 
   * @param bytesToEncrypt
   *          bytes to encrypt.
   * 
   * @param rawSKey
   *          Secret key for encryption.
   * @return
   *         encrypted node as byte array.
   */
  public static final synchronized byte[] encrypt(final byte[] bytesToEncrypt, final byte[] rawSKey) {

    // restore secret key from byte array
    final SecretKeySpec restoredSKey = new SecretKeySpec(rawSKey, ENCRYPTION_TYPE);

    // initialize secret key specifications and cipher
    final IvParameterSpec ivParams = new IvParameterSpec(rawSKey);

    final Cipher cipher;
    byte[] encrypted = null;
    try {
      cipher = Cipher.getInstance(ENCRYPTION_PADDING_TYPE);
      cipher.init(Cipher.ENCRYPT_MODE, restoredSKey, ivParams);
      encrypted = cipher.doFinal(bytesToEncrypt);

    } catch (final GeneralSecurityException exc) {
      exc.printStackTrace();
    }
    return encrypted;
  }

  /**
   * Decrypt a node.
   * 
   * @param bytesToDecrypt
   *          Byte array to decrypt.
   * 
   * @param rawSKey
   *          Secret key for decryption.
   * @return
   *         Original byte array of node.
   */
  public static final synchronized byte[] decrypt(final byte[] bytesToDecrypt, final byte[] rawSKey) {

    // restore secret key from byte array
    final SecretKeySpec restoredSKey = new SecretKeySpec(rawSKey, ENCRYPTION_TYPE);

    final Cipher cipher;
    byte[] decrypted = null;
    final IvParameterSpec ivParams = new IvParameterSpec(rawSKey);

    try {
      cipher = Cipher.getInstance(ENCRYPTION_PADDING_TYPE);
      cipher.init(Cipher.DECRYPT_MODE, restoredSKey, ivParams);
      decrypted = cipher.doFinal(bytesToDecrypt);
    } catch (final GeneralSecurityException exc) {
      exc.printStackTrace();
    }
    return decrypted;
  }

  /**
   * Generate a secret key for en- and decryption operations.
   * 
   * @return
   *         Generated secret key.
   */
  public final synchronized byte[] generateSecretKey() {
    final KeyGenerator kGen;
    SecretKey sKey = null;
    try {
      kGen = KeyGenerator.getInstance(ENCRYPTION_TYPE);
      kGen.init(ENCRYPTION_BITS);
      sKey = kGen.generateKey();

    } catch (final NoSuchAlgorithmException exc) {
      exc.printStackTrace();
    }

    return sKey.getEncoded();
  }

  /**
   * Converting an integer value to byte array.
   * 
   * @param mIntVal
   *          Integer value to convert.
   * @return Byte array of integer value.
   */
  public final byte[] intToByteArray(final int mIntVal) {
    final byte[] mBuffer = new byte[4];

    mBuffer[0] = (byte)(0xff & (mIntVal >>> 24));
    mBuffer[1] = (byte)(0xff & (mIntVal >>> 16));
    mBuffer[2] = (byte)(0xff & (mIntVal >>> 8));
    mBuffer[3] = (byte)(0xff & mIntVal);

    return mBuffer;
  }

  /**
   * Converting a Long value to byte array.
   * 
   * @param mLongVal
   *          Long value to convert.
   * @return Byte array of long value.
   */
  public final byte[] longToByteArray(final long mLongVal) {
    final byte[] mBuffer = new byte[8];

    mBuffer[0] = (byte)(0xff & (mLongVal >> 56));
    mBuffer[1] = (byte)(0xff & (mLongVal >> 48));
    mBuffer[2] = (byte)(0xff & (mLongVal >> 40));
    mBuffer[3] = (byte)(0xff & (mLongVal >> 32));
    mBuffer[4] = (byte)(0xff & (mLongVal >> 24));
    mBuffer[5] = (byte)(0xff & (mLongVal >> 16));
    mBuffer[6] = (byte)(0xff & (mLongVal >> 8));
    mBuffer[7] = (byte)(0xff & mLongVal);

    return mBuffer;
  }

  /**
   * Converting a byte array to integer.
   * 
   * @param mByteArray
   *          Byte array to convert.
   * @return converted integer value.
   */
  public final int byteArrayToInt(final byte[] mByteArray) {
    final int mConvInt =
      ((mByteArray[0] & 0xff) << 24) | ((mByteArray[1] & 0xff) << 16) | ((mByteArray[2] & 0xff) << 8)
        | (mByteArray[3] & 0xff);

    return mConvInt;
  }

  /**
   * Converting a byte array to long.
   * 
   * @param mByteArray
   *          Byte array to convert.
   * @return converted long value.
   */
  public final long byteArrayToLong(final byte[] mByteArray) {
    final long mConvLong =
      ((long)(mByteArray[0] & 0xff) << 56) | ((long)(mByteArray[1] & 0xff) << 48)
        | ((long)(mByteArray[2] & 0xff) << 40) | ((long)(mByteArray[3] & 0xff) << 32)
        | ((long)(mByteArray[4] & 0xff) << 24) | ((long)(mByteArray[5] & 0xff) << 16)
        | ((long)(mByteArray[6] & 0xff) << 8) | (mByteArray[7] & 0xff);

    return mConvLong;
  }

}
