package org.treetank.api;

/**
 * Interface for builders to support the Abstract Factory pattern.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 *         <T> generic type parameter
 */
public interface IBuilder<T> {
  /**
   * Build a new instance of type T.
   * 
   * @return instance of type T
   */
  T build();
}
