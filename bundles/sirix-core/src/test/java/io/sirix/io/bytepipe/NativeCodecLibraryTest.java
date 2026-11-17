package io.sirix.io.bytepipe;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.lang.foreign.SymbolLookup;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class NativeCodecLibraryTest {
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
  void bundledAdaptersShareOneProcessLifetimeLibrary() {
    assumeTrue(System.getProperty("os.name").contains("Linux"));
    assumeTrue(System.getProperty("os.arch").equals("amd64"));
    assumeTrue(getClass().getResource("/native/linux-x86_64/libsirix_lz77.so") != null);
    final SymbolLookup library = NativeCodecLibrary.lookup();
    assertNotNull(library);
    assertSame(library, NativeCodecLibrary.lookup());
    assertTrue(library.find("sirix_lz77_decode").isPresent());
    assertTrue(library.find("sirix_lz4_decode_range").isPresent());
  }
}
