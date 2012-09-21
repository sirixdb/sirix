package org.sirix.service.xml.shredder;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixException;

/**
 * Interface all shredders have to implement.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <S>
 *          generic type parameter for start tag/end tags (usually a {@link QName}
 * @param <T>
 *          generic type parameter for text nodes (usually a String)
 */
public interface IShredder<S, T> {
  /**
   * Process a start tag.
   * 
   * @param pName
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processStartTag(@Nonnull final T pName) throws SirixException;

  /**
   * Process a text node
   * 
   * @param pText
   *          text, usually of type String
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processText(@Nonnull final S pText) throws SirixException;

  /**
   * Process an end tag.
   * 
   * @param pName
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processEndTag(@Nonnull final T pName) throws SirixException;

  /**
   * Process an empty element.
   * 
   * @param pName
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processEmptyElement(@Nonnull final T pName) throws SirixException;
}
