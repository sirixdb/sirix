package io.sirix.io.bytepipe;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.lang.foreign.SymbolLookup;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

@Isolated
final class NativeCodecLibraryTest {
  @TempDir
  Path directory;

  @Test
  void extractionUsesContentAddressedNamesAndSkipsEmptyRoots() throws Exception {
    assumeTrue(getClass().getResource("/native/linux-x86_64/libsirix_lz77.so") != null);
    final String[] keys = {"os.name", "os.arch", "java.io.tmpdir", "user.home"};
    final String[] original = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      original[i] = System.getProperty(keys[i]);
    }
    try {
      System.setProperty("os.name", "Linux");
      System.setProperty("os.arch", "x86_64");
      System.setProperty("user.home", directory.toString());
      final byte[] bundled;
      try (InputStream source = getClass().getResourceAsStream("/native/linux-x86_64/libsirix_lz77.so")) {
        assertNotNull(source);
        bundled = source.readAllBytes();
      }
      // The filename contract uses the first eight SHA-256 bytes, independently computed by the JDK.
      final String expectedName = "libsirix_lz77-"
          + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bundled), 0, 8) + ".so";
      for (final String root : new String[] {directory.toString(), "", null}) {
        if (root == null) {
          System.clearProperty("java.io.tmpdir");
        } else {
          System.setProperty("java.io.tmpdir", root);
        }
        final Path extracted = NativeCodecLibrary.extractNativeLib();
        assertNotNull(extracted);
        assertEquals(expectedName, extracted.getFileName().toString());
        assertTrue(extracted.startsWith(directory));
        assertArrayEquals(bundled, Files.readAllBytes(extracted));
      }
      System.setProperty("os.name", "Windows 11");
      assertNull(NativeCodecLibrary.extractNativeLib());
      System.setProperty("os.name", "Linux");
      System.setProperty("os.arch", "unsupported");
      assertNull(NativeCodecLibrary.extractNativeLib());
      System.setProperty("os.arch", "aarch64");
      if (getClass().getResource("/native/linux-aarch64/libsirix_lz77.so") == null) {
        assertNull(NativeCodecLibrary.extractNativeLib());
      }
    } finally {
      for (int i = 0; i < keys.length; i++) {
        if (original[i] == null) {
          System.clearProperty(keys[i]);
        } else {
          System.setProperty(keys[i], original[i]);
        }
      }
    }
  }

  @Test
  void concurrentPublisherReusesTheWinningFileAndRemovesItsStagingFile() throws Exception {
    final Path target = directory.resolve("library.so");
    final byte[] bytes = {1, 2, 3};
    Files.write(target, bytes);
    // Model a filesystem race: the first probe precedes the winner's publication; rename loses.
    try (MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
      files.when(() -> Files.isReadable(target)).thenReturn(false, true);
      files.when(() -> Files.move(any(Path.class), eq(target), eq(StandardCopyOption.ATOMIC_MOVE)))
           .thenThrow(new FileAlreadyExistsException(target.toString()));
      assertEquals(target, NativeCodecLibrary.publishNativeLib(target, bytes));
    }
    assertArrayEquals(bytes, Files.readAllBytes(target));
    try (var files = Files.list(directory)) {
      assertEquals(1, files.count());
    }
  }

  @Test
  void anAlreadyStagedLibraryIsReusedAndAFileOfAnotherLengthIsReplaced() throws Exception {
    final Path target = directory.resolve("staged.so");
    final byte[] bytes = {1, 2, 3, 4};
    assertEquals(target, NativeCodecLibrary.publishNativeLib(target, bytes));
    final Object staged = Files.readAttributes(target, BasicFileAttributes.class).fileKey();
    // Without file keys (some file systems) a rewrite cannot be told from a reuse.
    assumeTrue(staged != null);
    assertEquals(target, NativeCodecLibrary.publishNativeLib(target, bytes));
    // Staging again would rename a new file over this one, and every process start would do it.
    assertEquals(staged, Files.readAttributes(target, BasicFileAttributes.class).fileKey());
    // A file of another length is not this library and must not be handed out as it.
    Files.write(target, new byte[] {9});
    assertEquals(target, NativeCodecLibrary.publishNativeLib(target, bytes));
    assertArrayEquals(bytes, Files.readAllBytes(target));
    try (var files = Files.list(directory)) {
      assertEquals(1, files.count());
    }
  }

  @Test
  void nativeImageResourceFollowsTheSharedLoader() throws Exception {
    final String metadata;
    try (InputStream source =
        getClass().getResourceAsStream("/META-INF/native-image/io.sirix/sirix-core/reachability-metadata.json")) {
      assertNotNull(source);
      metadata = new String(source.readAllBytes(), StandardCharsets.UTF_8);
    }
    for (JsonElement element : JsonParser.parseString(metadata).getAsJsonObject().getAsJsonArray("resources")) {
      final JsonObject resource = element.getAsJsonObject();
      if (resource.get("glob").getAsString().equals("native/**/libsirix_lz77.so")) {
        assertEquals(NativeCodecLibrary.class.getName(),
            resource.getAsJsonObject("condition").get("typeReached").getAsString());
        return;
      }
    }
    fail("Missing bundled native codec resource metadata");
  }

  @Test
  void bundledLibraryLoadsOncePerProcess() {
    assumeTrue(System.getProperty("os.name").contains("Linux"));
    assumeTrue(System.getProperty("os.arch").equals("amd64"));
    assumeTrue(getClass().getResource("/native/linux-x86_64/libsirix_lz77.so") != null);
    final SymbolLookup library = NativeCodecLibrary.lookup();
    assertNotNull(library);
    assertSame(library, NativeCodecLibrary.lookup());
    assertTrue(library.find("sirix_lz77_decode").isPresent());
  }
}
