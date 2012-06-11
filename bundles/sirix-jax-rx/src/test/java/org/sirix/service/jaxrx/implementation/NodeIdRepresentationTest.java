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
package org.sirix.service.jaxrx.implementation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.parsers.ParserConfigurationException;

import org.jaxrx.core.JaxRxException;
import org.jaxrx.core.QueryParameter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.exception.AbsTTException;
import org.sirix.service.jaxrx.enums.EIdAccessType;
import org.sirix.service.jaxrx.util.DOMHelper;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class is responsible to test the {@link NodeIdRepresentation} class.
 * 
 * @author Patrick Lang, Lukas Lewandowski, University of Konstanz
 * 
 */
public class NodeIdRepresentationTest {

    /**
     * The NodeIdRepresentation reference.
     */
    private transient static NodeIdRepresentation ridWorker;
    /**
     * The sirix reference.
     */
    private transient static DatabaseRepresentation sirix;
    /**
     * This variable defines the node id from where the resource should be
     * retrieved
     */
    private static transient final long NODEIDGETRESOURCE = 17;
    /**
     * This variable defines the node id that should be modified
     */
    private static transient final long NODEIDTOMODIFY = 11;

    /**
     * name constant.
     */
    private static transient final String NAME = "name";

    /**
     * The name for the input stream file.
     */
    private static final transient String RESOURCENAME = "factyTest";

    /**
     * Instances the Literal true static variable
     */
    private static final transient String LITERALSTRUE = "yes";
    /**
     * Instances the Literal false static variable
     */
    private static final transient String LITERALSFALSE = "no";
    /**
     * The name of the result node
     */
    private static final transient String RESULTNAME = "jaxrx:result";
    /**
     * The name of the id attribute
     */
    private static final transient String IDNAME = "rest:ttid";
    /**
     * The name of the country node
     */
    private static final transient String COUNTRYNAME = "country";

    /**
     * query constant.
     */
    private static final String QUERY = "/descendant-or-self::city/child::name";

    /**
     * node name constant.
     */
    private static final String NODENAME = "myNode";

    /**
     * The set up method.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        TestHelper.closeEverything();
        TestHelper.deleteEverything();
        TestHelper.getDatabase(TestHelper.PATHS.PATH1.getFile());
        ridWorker = new NodeIdRepresentation(TestHelper.PATHS.PATH1.getFile());
        sirix = new DatabaseRepresentation(TestHelper.PATHS.PATH1.getFile());
        final InputStream input =
            NodeIdRepresentationTest.class.getClass().getResourceAsStream("/factbook.xml");
        sirix.shred(input, RESOURCENAME);
    }

    /**
     * The tear down method.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        TestHelper.closeEverything();
        TestHelper.deleteEverything();
    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#getResource(java.lang.String, long, java.util.Map)}
     * .
     * 
     * @throws IOException
     * @throws WebApplicationException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    @Test
    public final void testGetResource() throws WebApplicationException, IOException,
        ParserConfigurationException, SAXException {
        final Map<QueryParameter, String> queryParams = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, "0");

        ByteArrayOutputStream outputStream;
        StreamingOutput result;
        Document doc;
        NodeList list;
        Node node;
        Node resultNode;
        Attr attribute;

        // Test for fist child
        result = ridWorker.getResource(RESOURCENAME, NODEIDGETRESOURCE, queryParams);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName("city");
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        final NodeList resultList = doc.getElementsByTagName(RESULTNAME);
        resultNode = resultList.item(0);
        outputStream.close();
        assertNotNull("Test if node exist 1a", node);
        assertNotNull("Test if node exist 1b", resultNode);
        assertNotNull("Test if node exist 1c", attribute);

        queryParams.clear();
        queryParams.put(QueryParameter.OUTPUT, LITERALSFALSE);
        queryParams.put(QueryParameter.QUERY, null);
        queryParams.put(QueryParameter.WRAP, LITERALSFALSE);
        queryParams.put(QueryParameter.REVISION, null);

        result = ridWorker.getResource(RESOURCENAME, NODEIDGETRESOURCE, queryParams);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(COUNTRYNAME);
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        resultNode = doc.getElementsByTagName(RESULTNAME).item(0);
        outputStream.close();
        assertNotNull("Test if node exist 2a", node);
        assertNull("Test if node exist 2b", resultNode);
        assertNull("Test if node exist 2c", attribute);

        queryParams.clear();
        queryParams.put(QueryParameter.OUTPUT, LITERALSFALSE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, null);
        result = ridWorker.getResource(RESOURCENAME, NODEIDGETRESOURCE, queryParams);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName("city");
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        resultNode = doc.getElementsByTagName(RESULTNAME).item(0);
        outputStream.close();
        assertNotNull("Test if node exist 3a", node);
        assertNotNull("Test if node exist 3b", resultNode);
        assertNull("Test if node exist 3c", attribute);

        queryParams.clear();
        queryParams.put(QueryParameter.OUTPUT, LITERALSFALSE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, "0");
        result = ridWorker.getResource(RESOURCENAME, NODEIDGETRESOURCE, queryParams);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(COUNTRYNAME);
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        resultNode = doc.getElementsByTagName(RESULTNAME).item(0);
        outputStream.close();
        assertNotNull("Test if node exist 4a", node);
        assertNotNull("Test if node exist 4b", resultNode);
        assertNull("Test if node exist 4c", attribute);
    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#getResourceByAT(java.lang.String, long, java.util.Map, org.sirix.service.jaxrx.enums.EIdAccessType)}
     * .
     * 
     * @throws IOException
     * @throws WebApplicationException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    @Test
    public final void testGetResourceByAT() throws WebApplicationException, IOException,
        ParserConfigurationException, SAXException {
        final Map<QueryParameter, String> queryParams = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, "0");

        ByteArrayOutputStream outputStream;
        Document doc;
        NodeList list;
        String textContent = null;
        Node node;
        Attr attribute;

        // Test for fist child
        final StreamingOutput fChildOutput =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParams, EIdAccessType.FIRSTCHILD);
        outputStream = new ByteArrayOutputStream();
        fChildOutput.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NAME);
        node = list.item(0);
        textContent = node.getTextContent();
        outputStream.close();
        assertEquals("Test expected name ", "Albania", textContent);

        // Test for last child final StreamingOutput lChildOutput =
        final StreamingOutput lChildOutput =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParams, EIdAccessType.LASTCHILD);
        outputStream = new ByteArrayOutputStream();
        lChildOutput.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName("border");
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem("length");
        outputStream.close();
        assertEquals("Test expected border value ", "287", attribute.getValue());

        // Test for rightSibling final StreamingOutput rSiblingOutput =
        final StreamingOutput rSiblingOutput =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParams,
                EIdAccessType.RIGHTSIBLING);
        outputStream = new ByteArrayOutputStream();
        rSiblingOutput.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName("country");
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(NAME);
        outputStream.close();
        assertEquals("Test expected right sibling name ", "Andorra", attribute.getValue());

        // Test for leftSibling final StreamingOutput lSiblingOutput =
        final StreamingOutput lSiblingOutput =
            ridWorker
                .getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParams, EIdAccessType.LEFTSIBLING);
        outputStream = new ByteArrayOutputStream();
        lSiblingOutput.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName("continent");
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(NAME);
        outputStream.close();
        assertEquals("Test expected right sibling name ", "Africa", attribute.getValue());
    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#performQueryOnResource(java.lang.String, long, java.lang.String, java.util.Map)}
     * .
     * 
     * @throws IOException
     * @throws WebApplicationException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    @Test
    public final void testPerformQueryOnResource() throws WebApplicationException, IOException,
        ParserConfigurationException, SAXException {

        final Map<QueryParameter, String> queryParams = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, "0");

        ByteArrayOutputStream outputStream;
        Document doc;
        NodeList list;
        String textContent = null;
        Node node;
        Node resultNode;
        Attr attribute;
        StreamingOutput output;

        output = ridWorker.performQueryOnResource(RESOURCENAME, NODEIDGETRESOURCE, QUERY, queryParams);
        outputStream = new ByteArrayOutputStream();
        output.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NAME);
        node = list.item(0);
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        resultNode = doc.getElementsByTagName(RESULTNAME).item(0);
        textContent = node.getTextContent();

        assertEquals("Test expected city Tirane", "Tirane", textContent);
        assertNotNull("Test if wrapping is available", resultNode);
        assertNotNull("Test if node id is supported", attribute);
        node = list.item(list.getLength() - 1);
        textContent = node.getTextContent();
        outputStream.close();
        assertEquals("Test expected city Korce", "Korce", textContent);

        queryParams.clear();
        queryParams.put(QueryParameter.OUTPUT, LITERALSFALSE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);
        queryParams.put(QueryParameter.REVISION, "0");
        output = ridWorker.performQueryOnResource(RESOURCENAME, NODEIDGETRESOURCE, QUERY, queryParams);
        outputStream = new ByteArrayOutputStream();
        output.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NAME);
        node = list.item(0);
        textContent = node.getTextContent();
        attribute = (Attr)node.getAttributes().getNamedItem(IDNAME);
        resultNode = doc.getElementsByTagName(RESULTNAME).item(0);
        outputStream.close();
        assertEquals("Test expected city name Tirane", "Tirane", textContent);
        assertNotNull("Test if wrapping is supported", resultNode);
        assertNull("Test if node id is not supported", attribute);

    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#deleteResource(java.lang.String, long)}
     * .
     * 
     * @throws IOException
     * @throws WebApplicationException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws InterruptedException
     */
    @Test
    public final void testDeleteResource() throws WebApplicationException, IOException,
        ParserConfigurationException, SAXException, InterruptedException {

        ridWorker.deleteResource(RESOURCENAME, NODEIDGETRESOURCE);
        final Map<QueryParameter, String> queryParams = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);
        queryParams.put(QueryParameter.WRAP, LITERALSFALSE);

        final StreamingOutput result =
            ridWorker.performQueryOnResource(RESOURCENAME, 1, "//country[@name=\"Albania\"]", queryParams);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write("<resulti>".getBytes());
        result.write(outputStream);
        outputStream.write("</resulti>".getBytes());
        final Document doc = DOMHelper.buildDocument(outputStream);
        final NodeList list = doc.getElementsByTagName("country");
        Node searchedNode = null;
        for (int i = 0; i < list.getLength(); i++) {
            final Element elem = (Element)list.item(i);
            final Attr name = elem.getAttributeNode(NAME);
            if (name.getTextContent().equals("Albania")) {
                searchedNode = list.item(i);
                break;
            }
        }
        outputStream.close();
        assertNull("Country Albania does not exist anymore", searchedNode);
    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#modifyResource(java.lang.String, long, java.io.InputStream)}
     * .
     * 
     * @throws AbsTTException
     * @throws JaxRxException
     */
    @Test
    public final void testModifyResource() throws JaxRxException, AbsTTException {
        final InputStream inputStream = new ByteArrayInputStream("<testNode/>".getBytes());
        long lastRevision = sirix.getLastRevision(RESOURCENAME);
        ridWorker.modifyResource(RESOURCENAME, NODEIDTOMODIFY, inputStream);
        assertEquals("Test modify resource", sirix.getLastRevision(RESOURCENAME), ++lastRevision);
    }

    /**
     * Test method for
     * {@link org.sirix.service.jaxrx.implementation.NodeIdRepresentation#addSubResource(java.lang.String, long, java.io.InputStream, org.sirix.service.jaxrx.enums.EIdAccessType)}
     * .
     * 
     * @throws IOException
     * @throws WebApplicationException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    @Test
    public final void testAddSubResource() throws WebApplicationException, IOException,
        ParserConfigurationException, SAXException {
        final Map<QueryParameter, String> queryParams = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);
        queryParams.put(QueryParameter.WRAP, LITERALSTRUE);

        final Map<QueryParameter, String> queryParamsComp = new HashMap<QueryParameter, String>();
        queryParams.put(QueryParameter.OUTPUT, LITERALSTRUE);

        ByteArrayOutputStream outputStream;
        StreamingOutput result;
        Document doc;
        NodeList list;
        Node node;

        final InputStream newNode =
            new ByteArrayInputStream("<myNode><test>hello</test></myNode>".getBytes());

        // Test append first child
        ridWorker.addSubResource(RESOURCENAME, NODEIDGETRESOURCE, newNode, EIdAccessType.FIRSTCHILD);
        result =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParamsComp,
                EIdAccessType.FIRSTCHILD);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NODENAME);
        node = list.item(0);
        outputStream.close();
        assertNotNull("Test if first child exist after inserting", node);

        // Test append last child
        newNode.reset();
        ridWorker.addSubResource(RESOURCENAME, NODEIDGETRESOURCE, newNode, EIdAccessType.LASTCHILD);
        result =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParamsComp,
                EIdAccessType.LASTCHILD);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NODENAME);
        node = list.item(0);
        outputStream.close();
        assertNotNull("Test if last child exist after inserting", node);

        // Test append left sibling
        newNode.reset();
        ridWorker.addSubResource(RESOURCENAME, NODEIDGETRESOURCE, newNode, EIdAccessType.LEFTSIBLING);
        result =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParamsComp,
                EIdAccessType.LEFTSIBLING);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NODENAME);
        node = list.item(0);
        outputStream.close();
        assertNotNull("Test if left sibling exist after inserting", node);

        // Test append right sibling
        newNode.reset();
        ridWorker.addSubResource(RESOURCENAME, NODEIDGETRESOURCE, newNode, EIdAccessType.RIGHTSIBLING);
        result =
            ridWorker.getResourceByAT(RESOURCENAME, NODEIDGETRESOURCE, queryParamsComp,
                EIdAccessType.RIGHTSIBLING);
        outputStream = new ByteArrayOutputStream();
        result.write(outputStream);
        doc = DOMHelper.buildDocument(outputStream);
        list = doc.getElementsByTagName(NODENAME);
        node = list.item(0);
        outputStream.close();
        assertNotNull("Test if right sibling exist after inserting", node);

    }
}