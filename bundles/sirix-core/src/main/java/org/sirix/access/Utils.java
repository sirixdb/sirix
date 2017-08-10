package org.sirix.access;

import javax.xml.namespace.QName;

import org.brackit.xquery.atomic.QNm;

/**
 * Encapsulates generic stuff.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class Utils {

	/**
	 * Building name consisting of a prefix and a name. The namespace-URI is not used over here.
	 * 
	 * @param qName the {@link QName} of an element
	 * @return a string: [prefix:]localname
	 */
	public static String buildName(final QNm qName) {
		return qName.getPrefix().isEmpty() ? qName.getLocalName()
				: new StringBuilder(qName.getPrefix()).append(":").append(qName.getLocalName()).toString();
	}
}
