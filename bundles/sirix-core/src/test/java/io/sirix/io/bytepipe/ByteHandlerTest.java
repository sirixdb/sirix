package io.sirix.io.bytepipe;

import io.sirix.exception.SirixIOException;
import io.sirix.XmlTestHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Byte handler test.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class ByteHandlerTest {

  /**
   * //todo not found Test method for {@link ByteHandler#deserialize(java.io.InputStream)} and for
   * {@link ByteHandler#serialize(java.io.OutputStream)}.
   *
   * @throws IOException
   *
   */
  @ParameterizedTest
  @MethodSource("instantiateByteHandler")
  public void testSerializeAndDeserialize(Class<ByteHandler> clazz, ByteHandler[] handlers)
      throws SirixIOException, IOException {
    for (final ByteHandler handler : handlers) {
      final int datasize = 10000;
      final byte[] bytes = XmlTestHelper.generateRandomBytes(datasize);

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      OutputStream handledOutout = handler.serialize(output);
      handledOutout.flush();

      ByteArrayInputStream input = new ByteArrayInputStream(bytes);
      input.transferTo(handledOutout);
      output.flush();
      output.close();
      handledOutout.flush();
      handledOutout.close();
      input.close();

      final byte[] encoded = output.toByteArray();
      assertFalse(Arrays.equals(bytes, encoded),
          new StringBuilder("Check for ").append(handler.getClass()).append(" failed.").toString());

      input = new ByteArrayInputStream(encoded);
      InputStream handledInput = handler.deserialize(input);
      output = new ByteArrayOutputStream();
      handledInput.transferTo(output);
      output.flush();
      output.close();
      handledInput.close();
      input.close();

      final byte[] decoded = output.toByteArray();
      assertTrue(Arrays.equals(bytes, decoded),
          new StringBuilder("Check for ").append(handler.getClass()).append(" failed.").toString());
    }
  }

  /**
   * Providing different implementations of the {@link ByteHandler} as method source to the test class.
   *
   * @return different classes of the {@link ByteHandler}
   * @throws SirixIOException if an I/O error occurs
   */
  private static Stream<Arguments> instantiateByteHandler() throws SirixIOException {
    final Path encryptionKeyPath = Paths.get("src", "test", "resources", "resourceName");

    return Stream.of(Arguments.of(ByteHandler.class,
        new ByteHandler[] {new Encryptor(encryptionKeyPath), new DeflateCompressor(),
            new ByteHandlerPipeline(new Encryptor(encryptionKeyPath), new DeflateCompressor()),
            new ByteHandlerPipeline(new DeflateCompressor(), new Encryptor(encryptionKeyPath))}));
  }

}
