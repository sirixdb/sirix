package org.sirix.service.json.serializer;

import org.junit.Test;
import org.sirix.service.json.serialize.StringValue;

import static org.junit.Assert.assertEquals;

public final class StringValueTest {

  @Test
  public void escapeFormfeed() {
    assertEquals("Form feed character '\\f' should be escaped", "\\f", StringValue.escape("\f"));
  }

  @Test
  public void escapeTab() {
    assertEquals("Tab character '\\t' should be escaped", "\\t", StringValue.escape("\t"));
  }

  @Test
  public void escapeBackspace() {
    assertEquals("Backspace character '\\b' should be escaped", "\\b", StringValue.escape("\b"));
  }

  @Test
  public void escapeEmoji() {
    assertEquals("Bomb emoji should not be escaped", "💣", StringValue.escape("💣"));
  }
}
