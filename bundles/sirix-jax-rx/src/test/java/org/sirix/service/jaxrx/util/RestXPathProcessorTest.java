/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * 
 */
package org.sirix.service.jaxrx.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.implementation.DatabaseRepresentation;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class tests the class {@link RestXPathProcessor}.
 * 
 * @author Lukas Lewandowski, University of Konstanz
 * 
 */
public class RestXPathProcessorTest {

	/**
	 * The rxProcessor reference;
	 */
	private final transient RestXPathProcessor rxProcessor = new RestXPathProcessor(
			TestHelper.PATHS.PATH1.getFile());

	/**
	 * The resource name.
	 */
	private static final transient String RESOURCENAME = "books";

	/**
	 * Instances param books static variable.
	 */
	public static final transient String PARAMBOOKS = "book";
	/**
	 * Instances param author static variable.
	 */
	public static final transient String PARAMAUTHOR = "author";
	/**
	 * Instances param jaxrx result static variable.
	 */
	public static final transient String PARAMJAXRES = "jaxrx:result";
	/**
	 * Instances param rest sequence static variable.
	 */
	public static final transient String PARAMJRESTSEQ = "rest:sequence";

	@BeforeClass
	public static void setUpGlobal() throws SirixException {
		deleteDirectory(TestHelper.PATHS.PATH1.getFile());
		final InputStream xmlInput = RestXPathProcessorTest.class
				.getResourceAsStream("/books.xml");
		new DatabaseRepresentation(TestHelper.PATHS.PATH1.getFile()).shred(
				xmlInput, RESOURCENAME);
	}

	/**
	 * Test method for
	 * {@link org.sirix.service.jaxrx.util.RestXPathProcessor#RestXPathProcessor()}
	 * .
	 */
	@Test
	public final void testRestXPathProcessor() {
		final RestXPathProcessor reference = new RestXPathProcessor(
				TestHelper.PATHS.PATH1.getFile());
		assertNotNull("checks if the reference is not null and constructor works",
				reference);
	}

	/**
	 * Test method for
	 * {@link org.sirix.service.jaxrx.util.RestXPathProcessor#getXpathResource(java.lang.String, java.lang.String, boolean, java.lang.Long, java.io.OutputStream, boolean)}
	 * .
	 * 
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws sirixException
	 */
	@Test
	public final void testGetXpathResourceStringStringBooleanLongOutputStreamBoolean()
			throws IOException, SAXException, ParserConfigurationException,
			SirixException {
		String xPath = "//book";
		boolean withNodeIds = true;
		OutputStream output = new ByteArrayOutputStream();
		rxProcessor.getXpathResource(RESOURCENAME, xPath, withNodeIds, 0, output,
				true);
		InputStream xmlInput = new ByteArrayInputStream(
				((ByteArrayOutputStream) output).toByteArray());
		Document resultDoc = xmlDocument(xmlInput);
		final NodeList bNodes = resultDoc.getElementsByTagName(PARAMBOOKS);
		assertEquals("test for items size of books is 6", 6, bNodes.getLength());
		final NodeList aNodes = resultDoc.getElementsByTagName(PARAMAUTHOR);
		assertEquals("test for items size of authors is 12", 12, aNodes.getLength());
		final NodeList rNodes = resultDoc.getElementsByTagName(PARAMJAXRES);
		assertEquals("test for existence of result element is 1", 1,
				rNodes.getLength());
		final NodeList iNodes = resultDoc.getElementsByTagName(PARAMJRESTSEQ);
		assertEquals("test for existence of node ids", 6, iNodes.getLength());
		xPath = "//author";
		withNodeIds = false;
		output = new ByteArrayOutputStream();
		rxProcessor.getXpathResource(RESOURCENAME, xPath, withNodeIds, null,
				output, true);
		xmlInput = new ByteArrayInputStream(
				((ByteArrayOutputStream) output).toByteArray());
		resultDoc = xmlDocument(xmlInput);
		final NodeList b2Nodes = resultDoc.getElementsByTagName(PARAMBOOKS);
		assertEquals("test for items size of books is 0", 0, b2Nodes.getLength());
		final NodeList a2Nodes = resultDoc.getElementsByTagName(PARAMAUTHOR);
		assertEquals("test for items size of authors", 12, a2Nodes.getLength());
		final NodeList r2Nodes = resultDoc.getElementsByTagName(PARAMJAXRES);
		assertEquals("test for existence of result element", 1, r2Nodes.getLength());
		final NodeList i2Nodes = resultDoc.getElementsByTagName(PARAMJRESTSEQ);
		assertEquals("test for existence of node ids", 0, i2Nodes.getLength());

	}

	/**
	 * Test method for
	 * {@link org.sirix.service.jaxrx.util.RestXPathProcessor#getXpathResource(java.io.File, long, java.lang.String, boolean, java.lang.Long, java.io.OutputStream, boolean)}
	 * .
	 * 
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 * @throws sirixException
	 */
	@Test
	public final void testGetXpathResourceFileLongStringBooleanLongOutputStreamBoolean()
			throws SAXException, IOException, ParserConfigurationException,
			SirixException {
		String xPath = "//author";
		boolean withNodeIds = true;
		OutputStream output = new ByteArrayOutputStream();
		final File tnkFile = new File(TestHelper.PATHS.PATH1.getFile(),
				RESOURCENAME);
		rxProcessor.getXpathResource(tnkFile, 10L, xPath, withNodeIds, 0, output,
				true);
		InputStream xmlInput = new ByteArrayInputStream(
				((ByteArrayOutputStream) output).toByteArray());
		Document resultDoc = xmlDocument(xmlInput);
		final NodeList bNodes = resultDoc.getElementsByTagName(PARAMBOOKS);
		assertEquals("test for items size of books", 0, bNodes.getLength());
		final NodeList aNodes = resultDoc.getElementsByTagName(PARAMAUTHOR);
		assertEquals("test for items size of authors is 2", 2, aNodes.getLength());
		final NodeList rNodes = resultDoc.getElementsByTagName(PARAMJAXRES);
		assertEquals("test for item of result element is 1", 1, rNodes.getLength());
		final NodeList iNodes = resultDoc.getElementsByTagName(PARAMJRESTSEQ);
		assertEquals("test for existence of node ids", 2, iNodes.getLength());
		xPath = "//author";
		withNodeIds = false;
		output = new ByteArrayOutputStream();
		rxProcessor.getXpathResource(tnkFile, 10L, xPath, withNodeIds, null,
				output, true);
		xmlInput = new ByteArrayInputStream(
				((ByteArrayOutputStream) output).toByteArray());
		resultDoc = xmlDocument(xmlInput);
		final NodeList b2Nodes = resultDoc.getElementsByTagName(PARAMBOOKS);
		assertEquals("test for item size of books", 0, b2Nodes.getLength());
		final NodeList a2Nodes = resultDoc.getElementsByTagName(PARAMAUTHOR);
		assertEquals("test for item size of authors", 2, a2Nodes.getLength());
		final NodeList r2Nodes = resultDoc.getElementsByTagName(PARAMJAXRES);
		assertEquals("test of result element", 1, r2Nodes.getLength());
		final NodeList i2Nodes = resultDoc.getElementsByTagName(PARAMJRESTSEQ);
		assertEquals("test existence of node ids", 0, i2Nodes.getLength());
	}

	/**
	 * This method deletes a not empty directory.
	 * 
	 * @param path
	 *          The director that has to be deleted.
	 * @return <code>true</code> if the deletion process has been successful.
	 *         <code>false</code> otherwise.
	 */
	private static boolean deleteDirectory(final File path) {
		if (path.exists()) {
			final File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				} else {
					files[i].delete();
				}
			}
		}
		return path.delete();
	}

	/**
	 * This method creates of an input stream an XML document.
	 * 
	 * @param input
	 *          The input stream.
	 * @return The packed XML document.
	 * @throws SAXException
	 *           Exception occurred.
	 * @throws IOException
	 *           Exception occurred.
	 * @throws ParserConfigurationException
	 *           Exception occurred.
	 */
	private Document xmlDocument(final InputStream input) throws SAXException,
			IOException, ParserConfigurationException {
		return DocumentBuilderFactory.newInstance().newDocumentBuilder()
				.parse(input);
	}

}