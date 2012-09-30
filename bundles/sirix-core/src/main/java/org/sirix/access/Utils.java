package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

/**
 * Encapsulates generic stuff.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class Utils {

	/**
	 * Building name consisting of a prefix and a name. The namespace-URI is not used
	 * over here.
	 * 
	 * @param pQName
	 *          the {@link QName} of an element
	 * @return a string with [prefix:]localname
	 */
	public static String buildName(final @Nonnull QName pQName) {
		return pQName.getPrefix().isEmpty() ? pQName.getLocalPart()
				: new StringBuilder(pQName.getPrefix()).append(":")
						.append(pQName.getLocalPart()).toString();
	}
}
