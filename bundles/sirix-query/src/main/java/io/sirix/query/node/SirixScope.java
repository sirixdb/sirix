package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Scope;
import io.brackit.query.jdm.Stream;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.settings.Fixed;

/**
 * Sirix scope.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixScope implements Scope {

	/** Sirix {@link XmlNodeReadOnlyTrx}. */
	private final XmlNodeReadOnlyTrx rtx;

	/**
	 * Constructor.
	 *
	 * @param node
	 *            database node
	 */
	public SirixScope(final XmlDBNode node) {
		// Assertion instead of requireNonNull(...) (part of internal API).
		assert node != null;
		rtx = node.getTrx();
	}

	@Override
	public Stream<String> localPrefixes() {
		return new Stream<>() {
			private int index;

			private final int mNamespaces = rtx.getNamespaceCount();

			@Override
			public String next() throws DocumentException {
				if (index < mNamespaces) {
					rtx.moveToNamespace(index++);
					return rtx.nameForKey(rtx.getPrefixKey());
				}
				return null;
			}

			@Override
			public void close() {
			}
		};
	}

	@Override
	public String defaultNS() {
		return resolvePrefix("");
	}

	@Override
	public void addPrefix(final String prefix, final String uri) {
		if (rtx instanceof final XmlNodeTrx wtx) {
			try {
				wtx.insertNamespace(new QNm(uri, prefix, ""));
			} catch (final SirixException e) {
				throw new DocumentException(e);
			}
		}
	}

	@Override
	public String resolvePrefix(final @Nullable String prefix) {
		final int prefixVocID = (prefix == null || prefix.isEmpty()) ? -1 : rtx.keyForName(prefix);
		while (true) {
			// First iterate over all namespaces.
			for (int i = 0, namespaces = rtx.getNamespaceCount(); i < namespaces; i++) {
				rtx.moveToNamespace(i);
				final String name = rtx.nameForKey(prefixVocID);
				if (name != null) {
					return name;
				}
				rtx.moveToParent();
			}
			// Then move to parent.
			if (rtx.hasParent() && rtx.getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty()) {
				rtx.moveToParent();
			} else {
				break;
			}
		}
		if (prefix.equals("xml")) {
			return "http://www.w3.org/XML/1998/namespace";
		}
		return prefixVocID == -1 ? "" : null;
	}

	@Override
	public void setDefaultNS(final String uri) {
		addPrefix("", uri);
	}
}
