package org.sirix.io.bytepipe;

import com.google.common.io.ByteStreams;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixIOException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Byte handler test.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class ByteHandlerTest {

  /**
   * Test method for {@link org.ByteHandler.io.bytepipe.IByteHandler#deserialize(byte[])} and for
   * {@link org.ByteHandler.io.bytepipe.IByteHandler#serialize(byte[])}.
   *
   * @throws IOException
   *
   * @throws TTByteHandleException
   */
  @Test(dataProvider = "instantiateByteHandler")
  public void testSerializeAndDeserialize(Class<ByteHandler> clazz, ByteHandler[] handlers)
      throws SirixIOException, IOException {
    for (final ByteHandler handler : handlers) {
      final int datasize = 10000;
      final byte[] bytes = XmlTestHelper.generateRandomBytes(datasize);

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      OutputStream handledOutout = handler.serialize(output);
      handledOutout.flush();

      ByteArrayInputStream input = new ByteArrayInputStream(bytes);
      ByteStreams.copy(input, handledOutout);
      output.flush();
      output.close();
      handledOutout.flush();
      handledOutout.close();
      input.close();

      final byte[] encoded = output.toByteArray();
      assertFalse(
          new StringBuilder("Check for ").append(handler.getClass()).append(" failed.").toString(),
          Arrays.equals(bytes, encoded));

      input = new ByteArrayInputStream(encoded);
      InputStream handledInput = handler.deserialize(input);
      output = new ByteArrayOutputStream();
      ByteStreams.copy(handledInput, output);
      output.flush();
      output.close();
      handledInput.close();
      input.close();

      final byte[] decoded = output.toByteArray();
      assertTrue(
          new StringBuilder("Check for ").append(handler.getClass()).append(" failed.").toString(),
          Arrays.equals(bytes, decoded));
    }
  }

  /**
   * Providing different implementations of the {@link ByteHandler} as Dataprovider to the test
   * class.
   *
   * @return different classes of the {@link ByteHandler}
   * @throws SirixIOException if an I/O error occurs
   */
  @DataProvider(name = "instantiateByteHandler")
  public Object[][] instantiateByteHandler() throws SirixIOException {
    final Path encryptionKeyPath = Paths.get("src", "test", "resources", "resourceName");

    Object[][] returnVal = {{ByteHandler.class,
        new ByteHandler[] {new Encryptor(encryptionKeyPath), new DeflateCompressor(),
            new SnappyCompressor(),
            new ByteHandlePipeline(new Encryptor(encryptionKeyPath), new DeflateCompressor()),
            new ByteHandlePipeline(new DeflateCompressor(), new Encryptor(encryptionKeyPath)),
            new ByteHandlePipeline(new Encryptor(encryptionKeyPath), new SnappyCompressor()),
            new ByteHandlePipeline(new SnappyCompressor(), new Encryptor(encryptionKeyPath))}}};
    return returnVal;
  }

}
