/**
 * 
 */
package org.sirix.gui.view.model.interfaces;

import javax.xml.stream.XMLStreamException;

import org.sirix.exception.SirixException;

/**
 * Allows changes to the underlying storage.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface ChangeModel {

	/**
	 * Commit changes.
	 * 
	 * @throws SirixException
	 *           if something fails
	 */
	void commit() throws SirixException;

	/**
	 * Add XML fragment, that is add it to a PUL but don't commit them.
	 * 
	 * @param paramFragment
	 *          the XML fragment to insert
	 * @throws SirixException
	 *           if something fails
	 * @throws XMLStreamException
	 *           if parsing XML fragment fails
	 */
	void addXMLFragment(final String paramFragment) throws SirixException,
			XMLStreamException;
}
