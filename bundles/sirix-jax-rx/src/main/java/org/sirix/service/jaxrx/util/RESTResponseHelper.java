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

package org.sirix.service.jaxrx.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.sirix.exception.AbsTTException;
import org.sirix.service.jaxrx.implementation.DatabaseRepresentation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * This class is a helper class to build the response XML fragment for REST
 * resources.
 * 
 * @author Patrick Lang, Lukas Lewandowski, University of Konstanz
 * 
 */
public final class RESTResponseHelper {

    /**
     * The private empty constructor.
     */
    private RESTResponseHelper() {
        // i do nothing
        // constructor only exists to meet pmd requirements
    }

    /**
     * This method creates a new {@link Document} instance for the surrounding
     * XML element for the client response.
     * 
     * @return The created {@link Document} instance.
     * @throws ParserConfigurationException
     *             The exception occurred.
     */
    private static Document createSurroundingXMLResp() throws ParserConfigurationException {
        final Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        return document;
    }

    /**
     * This method creates the sirix response XML element.
     * 
     * @param document
     *            The {@link Document} instance for the response.
     * @return The created XML {@link Element}.
     */
    private static Element createResultElement(final Document document) {
        final Element ttResponse = document.createElementNS("http://jaxrx.org/", "result");
        ttResponse.setPrefix("jaxrx");
        return ttResponse;
    }

    /**
     * This method creates the XML element containing a collection. This
     * collection contains the available resources which are children of the
     * database.
     * 
     * @param resources
     *            The list with the path of the available resources.
     * @param pStoragePath
     *            path where the data must be stored
     * @param document
     *            The XML {@link Document} instance.
     * @return A list of XML {@link Element} as the collection.
     * @throws AbsTTException
     * @throws WebApplicationException
     */
    private static List<Element> createCollectionElementDBs(final File pStoragePath,
        final List<String> resources, final Document document) throws WebApplicationException, AbsTTException {
        final List<Element> collectionsEls = new ArrayList<Element>();
        for (final String res : resources) {
            final Element elRes = document.createElement("resource");
            // Getting the name
            elRes.setAttribute("name", res);

            // get last revision from given db name
            final DatabaseRepresentation dbWorker = new DatabaseRepresentation(pStoragePath);
            final String lastRevision = Long.toString(dbWorker.getLastRevision(res.toString()));

            elRes.setAttribute("lastRevision", lastRevision);
            collectionsEls.add(elRes);
        }

        return collectionsEls;
    }

    /**
     * This method builds the overview for the resources and collection we offer
     * in our implementation.
     * 
     * @param pStoragePath
     *            path to the storage
     * @param availableRes
     *            A list of available resources or collections.
     * @return The streaming output for the HTTP response body.
     */
    public static StreamingOutput buildResponseOfDomLR(final File pStoragePath,
        final List<String> availableRes) {

        final StreamingOutput sOutput = new StreamingOutput() {

            @Override
            public void write(final OutputStream output) throws IOException, WebApplicationException {
                Document document;
                try {
                    document = createSurroundingXMLResp();
                    final Element resElement = RESTResponseHelper.createResultElement(document);

                    List<Element> collections;
                    try {
                        collections =
                            RESTResponseHelper.createCollectionElementDBs(pStoragePath, availableRes,
                                document);
                    } catch (final AbsTTException exce) {
                        throw new WebApplicationException(exce);
                    }
                    for (final Element resource : collections) {
                        resElement.appendChild(resource);
                    }
                    document.appendChild(resElement);
                    final DOMSource domSource = new DOMSource(document);
                    final StreamResult streamResult = new StreamResult(output);
                    final Transformer transformer = TransformerFactory.newInstance().newTransformer();
                    transformer.transform(domSource, streamResult);

                } catch (final ParserConfigurationException exce) {
                    throw new WebApplicationException(exce);
                } catch (final TransformerConfigurationException exce) {
                    throw new WebApplicationException(exce);
                } catch (final TransformerFactoryConfigurationError exce) {
                    throw new WebApplicationException(exce);
                } catch (final TransformerException exce) {
                    throw new WebApplicationException(exce);
                }
            }
        };

        return sOutput;
    }

}