package org.sirix.xquery.node;

import javax.annotation.Nonnull;

import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Scope;
import org.brackit.xquery.xdm.Stream;

public final class SirixScope implements Scope {

	/** Database node. */
	private final DBNode mNode;

	/**
	 * Constructor.
	 * 
	 * @param node
	 * 					database node
	 */
	public SirixScope(final @Nonnull DBNode node) {
		// Assertion instead of checkNotNull(...) (part of internal API).
		assert node != null;
		mNode = node;
	}
	
	@Override
	public Stream<String> localPrefixes() throws DocumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String defaultNS() throws DocumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addPrefix(final @Nonnull String prefix, final @Nonnull String uri) throws DocumentException {
		
		
	}

	@Override
	public String resolvePrefix(String prefix) throws DocumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDefaultNS(String uri) throws DocumentException {
		// TODO Auto-generated method stub
		
	}

}
