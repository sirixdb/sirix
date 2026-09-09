package io.sirix.page;

import io.sirix.settings.Constants;

public final class PageConstants {

  private PageConstants() {
    throw new AssertionError("May never be instantiated!");
  }

  /**
   * Largest encoded record allocation retained inline in a slotted document page.
   *
   * <p>
   * An alias of {@link Constants#MAX_RECORD_SIZE}, which is itself the compact directory's 10-bit
   * length ceiling; keeping one source of truth prevents generic and direct/fused record writers from
   * choosing different storage shapes. Inline Dewey-ID bytes and their trailer count toward this
   * ceiling. For an overflow record, only the encoded record body moves to the {@link OverflowPage};
   * its Dewey ID remains in page metadata.
   * </p>
   */
  public static final int MAX_RECORD_SIZE = Constants.MAX_RECORD_SIZE;

  /**
   * First physical {@link NamePage} slot available to a secondary NAME index in a JSON resource.
   * Slots 0, 1 and 2 belong to the object-key dictionary, FSST symbol tables and the global
   * projection-value dictionary respectively.
   */
  public static final int JSON_NAME_INDEX_OFFSET = 3;

  /**
   * First physical {@link NamePage} slot available to a secondary NAME index in an XML resource.
   * Slots 0 through 3 belong to the XML name dictionaries, slot 4 to FSST symbol tables and slot 5 to
   * the global projection-value dictionary.
   */
  public static final int XML_NAME_INDEX_OFFSET = 6;
}
