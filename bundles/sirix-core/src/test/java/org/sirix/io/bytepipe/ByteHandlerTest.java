package org.sirix.io.bytepipe;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;

import org.sirix.TestHelper;
import org.sirix.exception.SirixIOException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Byte handler test.
 *
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class ByteHandlerTest {

	/**
	 * Test method for
	 * {@link org.ByteHandler.io.bytepipe.IByteHandler#deserialize(byte[])} and
	 * for {@link org.ByteHandler.io.bytepipe.IByteHandler#serialize(byte[])}.
	 * 
	 * @throws TTByteHandleException
	 */
	@Test(dataProvider = "instantiateByteHandler")
	public void testSerializeAndDeserialize(Class<ByteHandler> clazz,
			ByteHandler[] pHandlers) throws SirixIOException {
		for (final ByteHandler handler : pHandlers) {
			final byte[] bytes = TestHelper.generateRandomBytes(10000);
			final byte[] serialized = handler.serialize(bytes);
			assertFalse(new StringBuilder("Check for ").append(handler.getClass())
					.append(" failed.").toString(), Arrays.equals(bytes, serialized));
			final byte[] deserialized = handler.deserialize(serialized);
			assertTrue(new StringBuilder("Check for ").append(handler.getClass())
					.append(" failed.").toString(), Arrays.equals(bytes, deserialized));
		}
	}

	/**
	 * Providing different implementations of the {@link ByteHandler} as
	 * Dataprovider to the test class.
	 * 
	 * @return different classes of the {@link ByteHandler}
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@DataProvider(name = "instantiateByteHandler")
	public Object[][] instantiateByteHandler() throws SirixIOException {
		Object[][] returnVal = { {
				ByteHandler.class,
				new ByteHandler[] { new Encryptor(), new DeflateCompressor(),
						new SnappyCompressor(),
						new ByteHandlePipeline(new Encryptor(), new DeflateCompressor()),
						new ByteHandlePipeline(new DeflateCompressor(), new Encryptor()),
						new ByteHandlePipeline(new Encryptor(), new SnappyCompressor()),
						new ByteHandlePipeline(new SnappyCompressor(), new Encryptor()), } } };
		return returnVal;
	}

}
