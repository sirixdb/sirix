package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.brackit.xquery.atomic.QNm;

/**
 * Encapsulates generic stuff.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class Utils {

	/**
	 * Building name consisting of a prefix and a name. The namespace-URI is not
	 * used over here.
	 * 
	 * @param qName
	 *          the {@link QName} of an element
	 * @return a string with [prefix:]localname
	 */
	public static String buildName(final @Nonnull QNm qName) {
		return qName.getPrefix().isEmpty() ? qName.getLocalName()
				: new StringBuilder(qName.getPrefix()).append(":")
						.append(qName.getLocalName()).toString();
	}
}
