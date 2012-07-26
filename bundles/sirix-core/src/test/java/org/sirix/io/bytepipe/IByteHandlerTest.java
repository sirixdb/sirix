package org.sirix.io.bytepipe;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;

import javax.crypto.spec.SecretKeySpec;

import org.sirix.TestHelper;
import org.sirix.exception.TTIOException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class IByteHandlerTest {

  /**
   * Test method for {@link org.treetank.io.bytepipe.IByteHandler#deserialize(byte[])} and for
   * {@link org.treetank.io.bytepipe.IByteHandler#serialize(byte[])}.
   * 
   * @throws TTByteHandleException
   */
  @Test(dataProvider = "instantiateByteHandler")
  public void testSerializeAndDeserialize(Class<IByteHandler> clazz,
    IByteHandler[] pHandlers) throws TTIOException {
    for (final IByteHandler handler : pHandlers) {
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
   * Providing different implementations of the {@link IByteHandler} as Dataprovider to the test class.
   * 
   * @return different classes of the {@link IByteHandler}
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @DataProvider(name = "instantiateByteHandler")
  public Object[][] instantiateByteHandler() throws TTIOException {
    // 128bit key
    final byte[] keyValue =
      new byte[] {
        'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k', 'k',
        'k', 'k'
      };
    final SecretKeySpec key = new SecretKeySpec(keyValue, "AES");

    Object[][] returnVal =
      {
        {
          IByteHandler.class,
          new IByteHandler[] {
            new Encryptor(key),
            new DeflateCompressor(),
            new ByteHandlePipeline(new Encryptor(key), new DeflateCompressor()),
            new ByteHandlePipeline(new DeflateCompressor(), new Encryptor(key))
          }
        }
      };
    return returnVal;
  }

}
