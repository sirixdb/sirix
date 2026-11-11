package io.sirix.mcp;

import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LimitedStringWriterTest {
  @Test
  void resetsContentAndLimitWhilePreviouslyReturnedStringsStayIndependent() throws IOException {
    final var writer = new LimitedStringWriter(4);
    writer.write("abcd");
    final String first = writer.toString();
    writer.reset(2);
    assertThat(writer.length()).isZero();
    writer.write(new char[] {'漢', '字'}, 0, 2);
    assertThat(writer.toString()).isEqualTo("漢字");
    assertThatThrownBy(() -> writer.write('!')).isInstanceOf(LimitedStringWriter.LimitExceededException.class);
    assertThat(writer.toString()).isEqualTo("漢字");
    assertThat(first).isEqualTo("abcd");
    writer.reset(8);
    writer.write("next");
    assertThat(writer.toString()).isEqualTo("next");
    assertThat(writer.length()).isEqualTo(4);
  }

  @Test
  void resetsAfterOverflowAndRejectsInvalidLimitsWithoutChangingContent() throws IOException {
    final var writer = new LimitedStringWriter(3);
    writer.write('a');
    assertThatThrownBy(() -> writer.write("long", 0, 4)).isInstanceOf(LimitedStringWriter.LimitExceededException.class);
    assertThat(writer.toString()).isEqualTo("a");
    assertThatThrownBy(() -> writer.reset(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThat(writer.toString()).isEqualTo("a");
    writer.reset(0);
    assertThat(writer.length()).isZero();
    assertThatThrownBy(() -> writer.write(new char[] {'x'}, 0, 1)).isInstanceOf(
        LimitedStringWriter.LimitExceededException.class);
    writer.reset(3);
    writer.write("abc");
    assertThat(writer.toString()).isEqualTo("abc");
  }
}
