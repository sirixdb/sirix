/**
 * 
 */
package org.sirix.io.bytepipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;

import org.sirix.exception.SirixIOException;

/**
 * Decorator for encrypting any content.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class Encryptor implements ByteHandler {

	/** Algorithm to use. */
	private static final String ALGORITHM = "AES";

	/** Key for access data. */
	private final Key mKey;

	/** 128bit key. */
	private static final byte[] KEYVALUE = new byte[] { 'k', 'k', 'k', 'k', 'k',
			'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k' };

	/**
	 * Constructor.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public Encryptor() {
		mKey = new SecretKeySpec(KEYVALUE, ALGORITHM);
	}

	@Override
	public OutputStream serialize(final OutputStream toSerialize)
			throws IOException {
		try {
			final Cipher cipher = Cipher.getInstance(ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, mKey);
			return new CipherOutputStream(toSerialize, cipher);
		} catch (final GeneralSecurityException e) {
			throw new IOException(e);
		}
	}

	@Override
	public InputStream deserialize(final InputStream toDeserialize)
			throws IOException {
		try {
			final Cipher cipher = Cipher.getInstance(ALGORITHM);
			cipher.init(Cipher.DECRYPT_MODE, mKey);
			return new CipherInputStream(toDeserialize, cipher);
		} catch (final GeneralSecurityException e) {
			throw new IOException(e);
		}
	}

	@Override
	public ByteHandler getInstance() {
		return new Encryptor();
	}
}
