package org.sirix.api;

/**
 * Interface for builders to support the Abstract Factory pattern.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <T> generic type parameter
 */
public interface Builder<T> {
  /**
   * Build a new instance of type T.
   * 
   * @return instance of type T
   */
  T build();
}
