package io.sirix.query.shell;

import java.util.Map;

/** Ember Night foreground colours, respecting terminal and user capabilities. */
public enum ShellPalette {
  NONE("", "", "", ""), ANSI_16("\033[33m", "\033[36m", "\033[32m", "\033[31m"), ANSI_256("\033[38;5;208m",
      "\033[38;5;75m", "\033[38;5;114m", "\033[38;5;203m"), TRUE_COLOR("\033[38;2;244;123;32m", "\033[38;2;66;182;240m",
          "\033[38;2;152;195;121m", "\033[38;2;240;106;106m");

  private final String orange;
  private final String blue;
  private final String green;
  private final String red;

  ShellPalette(final String orange, final String blue, final String green, final String red) {
    this.orange = orange;
    this.blue = blue;
    this.green = green;
    this.red = red;
  }

  public static ShellPalette detect(final boolean stdoutTerminal, final boolean disabled,
      final Map<String, String> environment, final int colours) {
    if (!stdoutTerminal || disabled || environment.containsKey("NO_COLOR") || "dumb".equals(environment.get("TERM"))
        || colours < 8)
      return NONE;
    final String capability = environment.get("COLORTERM");
    if ("truecolor".equals(capability) || "24bit".equals(capability) || colours >= 16777216)
      return TRUE_COLOR;
    return colours >= 256
        ? ANSI_256
        : ANSI_16;
  }

  public boolean enabled() {
    return this != NONE;
  }

  public String orange() {
    return orange;
  }

  public String blue() {
    return blue;
  }

  public String green() {
    return green;
  }

  public String red() {
    return red;
  }

  public String reset() {
    return enabled()
        ? "\033[0m"
        : "";
  }

  public String subtle() {
    return enabled()
        ? "\033[2m"
        : "";
  }
}
