package io.sirix.query.shell;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ShellPaletteTest {
  @Test
  void respectsOutputAndUserPolicyBeforeCapabilities() {
    assertEquals(ShellPalette.NONE, ShellPalette.detect(false, false, Map.of(), 256));
    assertEquals(ShellPalette.NONE, ShellPalette.detect(true, true, Map.of(), 256));
    assertEquals(ShellPalette.NONE, ShellPalette.detect(true, false, Map.of("NO_COLOR", ""), 256));
    assertEquals(ShellPalette.NONE, ShellPalette.detect(true, false, Map.of("TERM", "dumb"), 256));
    assertEquals(ShellPalette.TRUE_COLOR, ShellPalette.detect(true, false, Map.of("COLORTERM", "truecolor"), 256));
    assertEquals(ShellPalette.TRUE_COLOR, ShellPalette.detect(true, false, Map.of("COLORTERM", "24bit"), 256));
    assertEquals(ShellPalette.ANSI_256, ShellPalette.detect(true, false, Map.of(), 256));
    assertEquals(ShellPalette.ANSI_16, ShellPalette.detect(true, false, Map.of(), 16));
    assertEquals(ShellPalette.NONE, ShellPalette.detect(true, false, Map.of(), 0));
  }

  @Test
  void colourCapabilityBoundariesAndUnknownEnvironment() {
    assertEquals(ShellPalette.NONE, ShellPalette.detect(true, false, Map.of(), 7));
    assertEquals(ShellPalette.ANSI_16, ShellPalette.detect(true, false, Map.of(), 8));
    assertEquals(ShellPalette.ANSI_16, ShellPalette.detect(true, false, Map.of("COLORTERM", "unknown"), 255));
    assertEquals(ShellPalette.TRUE_COLOR, ShellPalette.detect(true, false, Map.of(), 16777216));
  }

  @Test
  void emberNightGoldenColours() {
    assertEquals("\033[38;2;244;123;32m", ShellPalette.TRUE_COLOR.orange());
    assertEquals("\033[38;2;66;182;240m", ShellPalette.TRUE_COLOR.blue());
    assertEquals("\033[38;5;208m", ShellPalette.ANSI_256.orange());
    assertEquals("\033[38;5;75m", ShellPalette.ANSI_256.blue());
    assertEquals("\033[33m", ShellPalette.ANSI_16.orange());
    assertEquals("\033[36m", ShellPalette.ANSI_16.blue());
    assertEquals("", ShellPalette.NONE.orange());
    assertEquals("", ShellPalette.NONE.reset());
  }
}
