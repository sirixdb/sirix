package org.sirix.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.NodeWriteTrx;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public final class DeweyIDGenerator extends DefaultHandler {

	/** Sirix {@link NodeWriteTrx}. */
	private final NodeWriteTrx mWtx;

	/**
	 * Constructor.
	 * 
	 * @param wtx
	 *          Sirix {@link NodeWriteTrx}
	 */
	public DeweyIDGenerator(final NodeWriteTrx wtx) {
		mWtx = checkNotNull(wtx);
	}

	@Override
	public void processingInstruction(String target, String data)
			throws SAXException {
		// TODO Auto-generated method stub
		super.processingInstruction(target, data);
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {

	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		// TODO Auto-generated method stub
		super.startElement(uri, localName, qName, attributes);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		// TODO Auto-generated method stub
		super.endElement(uri, localName, qName);
	}

}
