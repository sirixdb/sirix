package org.sirix.io.bytepipe;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import org.sirix.TestHelper;
import org.sirix.exception.SirixIOException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.io.ByteStreams;

/**
 * Byte handler test.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public class ByteHandlerTest {

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
      final byte[] bytes = TestHelper.generateRandomBytes(datasize);

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
    Object[][] returnVal = {{ByteHandler.class, new ByteHandler[] {
        new Encryptor(Paths.get("src", "test", "resources", "resourceName")),
        new DeflateCompressor(), new SnappyCompressor(),
        new ByteHandlePipeline(new Encryptor(Paths.get("src", "test", "resources", "resourceName")),
            new DeflateCompressor()),
        new ByteHandlePipeline(new DeflateCompressor(),
            new Encryptor(Paths.get("src", "test", "resources", "resourceName"))),
        new ByteHandlePipeline(new Encryptor(Paths.get("src", "test", "resources", "resourceName")),
            new SnappyCompressor()),
        new ByteHandlePipeline(new SnappyCompressor(),
            new Encryptor(Paths.get("src", "test", "resources", "resourceName")))}}};
    return returnVal;
  }

}
