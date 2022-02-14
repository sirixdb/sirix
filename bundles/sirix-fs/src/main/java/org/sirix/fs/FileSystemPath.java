package org.sirix.fs;

import org.checkerframework.checker.nullness.qual.NonNull;

/** Determins if a {@link FileSystemPath} denotes a directory or file. */
@NonNull
public enum FileSystemPath {
  /** It is a directory. */
  ISDIRECTORY {
    @Override
    public StringBuilder append(final StringBuilder pQueryBuilder) {
      pQueryBuilder.append("/dir");
      return pQueryBuilder;
    }
  },

  /** It is a file. */
  ISFILE {
    @Override
    public StringBuilder append(final StringBuilder pQueryBuilder) {
      pQueryBuilder.append("/file");
      return pQueryBuilder;
    }
  };

  /**
   * Append to xquery string.
   * 
   * @param pQueryBuilder the query builder
   * @return the query builder with an appended xpath valu
   */
  public abstract StringBuilder append(final StringBuilder pQueryBuilder);
}
