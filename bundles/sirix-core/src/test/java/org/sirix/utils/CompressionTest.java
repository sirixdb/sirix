package org.sirix.utils;

import org.junit.Test;
import org.sirix.settings.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.Deflater;

import static org.junit.Assert.assertTrue;

public class CompressionTest {

    @Test
    public void testCompressAndDecompress() throws java.lang.Exception {
        Path path = Paths.get("src", "test", "resources", "json").resolve("test.json");
        String string = Files.readString(path, Constants.DEFAULT_ENCODING);
        byte[] actual = TypedValue.getBytes(string);

        byte[] compressed = Compression.compress(actual, Deflater.BEST_COMPRESSION);
        String compressedString = new String(compressed);
        int actualSize = (new String(actual)).length();
        int compressedSize = compressedString.length();
        assertTrue("Compressed string size should be less than actual string", compressedSize < actualSize);

        byte[] decompressed = Compression.decompress(compressed);
        String decompressedString = new String(decompressed);
        int decompressedSize = decompressedString.length();
        assertTrue("Decompressed string size should be greater than compressed string", decompressedSize > compressedSize);
        assertTrue("Compressed String size should be equal to actual string", decompressedSize == actualSize);
        assertTrue("Compressed String should be same as actual string", decompressedString.equals(string));
    }
}