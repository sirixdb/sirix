package io.sirix.utils;

/**
 * Lightweight replacement for {@code com.google.common.base.MoreObjects.toStringHelper()}.
 * Produces output in the format: {@code ClassName{key=value, key2=value2}}.
 *
 * <p>Thread-safety: not thread-safe (single-threaded usage only).
 */
public final class ToStringHelper {

  private final StringBuilder sb;
  private boolean firstEntry = true;

  private ToStringHelper(final String className) {
    // Pre-size: className + '{' + estimated content + '}'
    this.sb = new StringBuilder(128);
    sb.append(className).append('{');
  }

  /**
   * Creates a new helper using the simple class name of the given instance.
   *
   * @param self the object whose class name to use
   * @return a new ToStringHelper
   */
  public static ToStringHelper of(final Object self) {
    return new ToStringHelper(self.getClass().getSimpleName());
  }

  /**
   * Creates a new helper using the given class name string.
   *
   * @param className the class name to use in the output
   * @return a new ToStringHelper
   */
  public static ToStringHelper of(final String className) {
    return new ToStringHelper(className);
  }

  /**
   * Adds a named value pair.
   *
   * @param name  the field name
   * @param value the field value (may be null)
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final Object value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds a named int value (avoids boxing).
   *
   * @param name  the field name
   * @param value the int value
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final int value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds a named long value (avoids boxing).
   *
   * @param name  the field name
   * @param value the long value
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final long value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds a named boolean value (avoids boxing).
   *
   * @param name  the field name
   * @param value the boolean value
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final boolean value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds a named float value (avoids boxing).
   *
   * @param name  the field name
   * @param value the float value
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final float value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds a named double value (avoids boxing).
   *
   * @param name  the field name
   * @param value the double value
   * @return this helper for chaining
   */
  public ToStringHelper add(final String name, final double value) {
    appendSeparator();
    sb.append(name).append('=').append(value);
    return this;
  }

  /**
   * Adds an unnamed value.
   *
   * @param value the value to add (may be null)
   * @return this helper for chaining
   */
  public ToStringHelper addValue(final Object value) {
    appendSeparator();
    sb.append(value);
    return this;
  }

  /**
   * Adds an unnamed int value (avoids boxing).
   *
   * @param value the int value
   * @return this helper for chaining
   */
  public ToStringHelper addValue(final int value) {
    appendSeparator();
    sb.append(value);
    return this;
  }

  /**
   * Adds an unnamed long value (avoids boxing).
   *
   * @param value the long value
   * @return this helper for chaining
   */
  public ToStringHelper addValue(final long value) {
    appendSeparator();
    sb.append(value);
    return this;
  }

  private void appendSeparator() {
    if (firstEntry) {
      firstEntry = false;
    } else {
      sb.append(", ");
    }
  }

  @Override
  public String toString() {
    sb.append('}');
    return sb.toString();
  }
}
