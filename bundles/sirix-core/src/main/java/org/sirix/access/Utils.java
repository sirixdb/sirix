package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

public class Utils {

	/**
	 * Building name consisting out of prefix and name. NamespaceUri is not used
	 * over here.
	 * 
	 * @param pQName
	 *          the {@link QName} of an element
	 * @return a string with [prefix:]localname
	 */
	public static String buildName(final @Nonnull QName pQName) {
		String name;
		if (pQName.getPrefix().isEmpty()) {
			name = pQName.getLocalPart();
		} else {
			name = new StringBuilder(pQName.getPrefix()).append(":")
					.append(pQName.getLocalPart()).toString();
		}
		return name;
	}
}
