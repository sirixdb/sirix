package org.sirix.service.xml.shredder;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixException;

/**
 * Interface all shredders have to implement.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <T>
 *          generic type parameter for start tag/end tags (usually a {@link QName}
 * @param <S>
 *          generic type parameter for text nodes (usually a String)
 */
public interface Shredder<S, T> {
	
	/**
	 * Process a processing instruction.
	 * 
	 * @param content
	 * 						the content
	 * @param target
	 * 						the target
	 */
	void processPI(final @Nonnull S content, final @Nonnull S target) throws SirixException;
	
	/**
	 * Process a comment.
	 * 
	 * @param value
	 * 						the value
	 */
	void processComment(@Nonnull final S value) throws SirixException;
	
  /**
   * Process a start tag.
   * 
   * @param name
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processStartTag(@Nonnull final T name) throws SirixException;

  /**
   * Process a text node
   * 
   * @param text
   *          text, usually of type String
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processText(@Nonnull final S text) throws SirixException;

  /**
   * Process an end tag.
   * 
   * @param name
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processEndTag(@Nonnull final T name) throws SirixException;

  /**
   * Process an empty element.
   * 
   * @param name
   *          name, usually a {@link QName}
   * @throws SirixException
   *           if Sirix fails to insert a new node
   */
  void processEmptyElement(@Nonnull final T name) throws SirixException;
}
