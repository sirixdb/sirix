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

package org.sirix.service.jaxrx.implementation; // NOPMD we need all these imports, declaring with * is
// pointless

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.jaxrx.core.JaxRxException;
import org.jaxrx.core.QueryParameter;
import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.axis.AbsAxis;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.util.RESTResponseHelper;
import org.sirix.service.jaxrx.util.RESTXMLShredder;
import org.sirix.service.jaxrx.util.RestXPathProcessor;
import org.sirix.service.jaxrx.util.WorkerHelper;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.settings.EFixed;

/**
 * This class is the Sirix DB connection for RESTful Web Services processing.
 * When a RESTful WS database request occurs it will be forwarded to sirix to
 * manage the request. Here XML files can be shredded and serialized to build
 * the client response. Further more it supports XPath queries.
 * 
 * @author Patrick Lang, Lukas Lewandowski, Sebastian Graf University of
 *         Konstanz
 * 
 */
public class DatabaseRepresentation {

	/** Path to storage. */
	private final File mStoragePath;

	/**
	 * This field the begin result element of a XQuery or XPath expression.
	 */
	private final static transient String beginResult = "<jaxrx:result xmlns:jaxrx=\"http://jaxrx.org/\">";

	/**
	 * This field the end result element of a XQuery or XPath expression.
	 */
	private final static transient String endResult = "</jaxrx:result>";

	/**
	 * Often used 'yes' {@link String}.
	 */
	private final static transient String YESSTRING = "yes";

	/**
	 * Constructor.
	 * 
	 * @param pStoragePath
	 *          path to storage
	 */
	public DatabaseRepresentation(final File pStoragePath) {
		mStoragePath = pStoragePath;
	}

	/**
	 * This method is responsible to create a new database.
	 * 
	 * @param inputStream
	 *          The stream containing the XML document that has to be stored.
	 * @param resourceName
	 *          The name of the new database.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public void createResource(final InputStream inputStream,
			final String resourceName) throws JaxRxException {
		synchronized (resourceName) {
			if (inputStream == null) {
				throw new JaxRxException(400, "Bad user request");
			} else {
				try {
					shred(inputStream, resourceName);
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		}
	}

	/**
	 * This method is responsible to deliver the whole database. Additional
	 * parameters can be set (wrap, revision, output) which change the response
	 * view.
	 * 
	 * @param resourceName
	 *          The name of the requested database.
	 * @param queryParams
	 *          The optional query parameters.
	 * @return The XML database resource, depending on the query parameters.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public StreamingOutput getResource(final String resourceName,
			final Map<QueryParameter, String> queryParams) throws JaxRxException {
		final StreamingOutput streamingOutput = new StreamingOutput() {
			@Override
			public void write(final OutputStream output) throws IOException,
					JaxRxException {
				final String revision = queryParams.get(QueryParameter.REVISION);
				final String wrap = queryParams.get(QueryParameter.WRAP);
				final String nodeId = queryParams.get(QueryParameter.OUTPUT);
				final boolean wrapResult = (wrap == null) ? false : wrap
						.equalsIgnoreCase(YESSTRING);
				final boolean nodeid = (nodeId == null) ? false : nodeId
						.equalsIgnoreCase(YESSTRING);
				try {
					if (revision == null) {
						serialize(resourceName, null, nodeid, output, wrapResult);
					} else {
						// pattern which have to match against the input
						final Pattern pattern = Pattern.compile("[0-9]+[-]{1}[1-9]+");

						final Matcher matcher = pattern.matcher(revision);

						if (matcher.matches()) {
							getModificHistory(resourceName, revision, nodeid, output,
									wrapResult);
						} else {
							serialize(resourceName, Integer.valueOf(revision), nodeid,
									output, wrapResult);
						}
					}
				} catch (final NumberFormatException exce) {
					throw new JaxRxException(400, exce.getMessage());
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		};
		return streamingOutput;
	}

	/**
	 * This method is responsible to perform queries on a special database. (XPath
	 * queries).
	 * 
	 * @param resource
	 *          The name of the database instance.
	 * @param query
	 *          The XPath expression.
	 * @param otherParams
	 *          Further query parameters (output, wrap, revision) which change the
	 *          response.
	 * @return The result of the XPath query expression.
	 */
	public StreamingOutput performQueryOnResource(final String resource,
			final String query, final Map<QueryParameter, String> otherParams) {
		final StreamingOutput streamingOutput = new StreamingOutput() {
			@Override
			public void write(final OutputStream output) throws IOException,
					JaxRxException {
				final String revision = otherParams.get(QueryParameter.REVISION);
				final String wrap = otherParams.get(QueryParameter.WRAP);
				final String nodeId = otherParams.get(QueryParameter.OUTPUT);
				final boolean wrapResult = (wrap == null) ? true : wrap
						.equalsIgnoreCase(YESSTRING);
				final boolean nodeid = (nodeId == null) ? false : nodeId
						.equalsIgnoreCase(YESSTRING);
				final Integer rev = revision == null ? null : Integer.valueOf(revision);
				final RestXPathProcessor xpathProcessor = new RestXPathProcessor(
						mStoragePath);
				try {
					xpathProcessor.getXpathResource(resource, query, nodeid, rev, output,
							wrapResult);
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		};
		return streamingOutput;
	}

	/**
	 * This method is responsible to deliver a list of available resources and
	 * collections supported by sirix's REST implementation.
	 * 
	 * @return The list of available databases and collections wrapped in an XML
	 *         document.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public StreamingOutput getResourcesNames() throws JaxRxException {
		final List<String> availResources = new ArrayList<String>();
		final File[] files = new File(mStoragePath,
				DatabaseConfiguration.Paths.Data.getFile().getName()).listFiles();
		if (files != null) {
			for (final File file : files) {
				if (file.isDirectory()) {
					final String dirName = file.getAbsoluteFile().getName();
					availResources.add(dirName);
				}
			}
		}

		return RESTResponseHelper
				.buildResponseOfDomLR(mStoragePath, availResources);
	}

	/**
	 * This method is responsible to add a new XML document to a collection.
	 * 
	 * @param input
	 *          the new XML document packed in an {@link InputStream}
	 * @param resource
	 *          the name of the resource
	 * @throws JaxRxException
	 *           if any exception occurred
	 */
	public boolean add(final InputStream input, final String resource)
			throws JaxRxException {
		synchronized (resource) {
			try {
				return shred(input, resource);
			} catch (final SirixException exce) {
				throw new JaxRxException(exce);
			}
		}
	}

	/**
	 * This method is responsible to delete an existing database.
	 * 
	 * @param resourceName
	 *          The name of the database.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public void deleteResource(final String resourceName)
			throws WebApplicationException {
		synchronized (resourceName) {
			try {
				final IDatabase database = Database.openDatabase(mStoragePath);
				database.truncateResource(resourceName);
			} catch (final SirixException exc) {
				throw new JaxRxException(500, "Deletion could not be performed");
			}
		}
	}

	/**
	 * This method is responsible to save the XML file, which is in an
	 * {@link InputStream}, as a sirix object.
	 * 
	 * @param xmlInput
	 *          XML file wrapped in an {@link InputStream}
	 * @param resource
	 *          name of the resource
	 * @return {@code true} if shredding process has been successful,
	 *         {@code false} otherwise.
	 * @throws SirixException
	 *           if any sirix related exception occurs
	 */
	public final boolean shred(final InputStream xmlInput, final String resource)
			throws SirixException {
		boolean allOk = false;
		INodeWriteTrx wtx = null;
		IDatabase database = null;
		ISession session = null;
		boolean abort = false;
		try {

			final DatabaseConfiguration dbConf = new DatabaseConfiguration(
					mStoragePath);
			Database.createDatabase(dbConf);
			database = Database.openDatabase(dbConf.getFile());
			// Shredding the database to the file as XML
			final ResourceConfiguration resConf = new ResourceConfiguration.Builder(
					resource, dbConf).setRevisionsToRestore(1).build();
			if (database.createResource(resConf)) {
				session = database
						.getSession(new SessionConfiguration.Builder(resource).build());
				wtx = session.beginNodeWriteTrx();
				wtx.moveTo(EFixed.NULL_NODE_KEY.getStandardProperty());
				final XMLShredder shredder = new XMLShredder(wtx,
						RESTXMLShredder.createReader(xmlInput), EInsert.ASFIRSTCHILD);
				shredder.call();
				allOk = true;
			}
		} catch (final Exception exce) {
			abort = true;
			throw new JaxRxException(exce);
		} finally {
			WorkerHelper.closeWTX(abort, wtx, session, database);
		}
		return allOk;
	}

	/**
	 * This method is responsible to build an {@link OutputStream} containing an
	 * XML file out of the sirix file.
	 * 
	 * @param resource
	 *          The name of the resource that will be offered as XML.
	 * @param nodeid
	 *          To response the resource with a restid for each node (
	 *          <code>true</code>) or without ( <code>false</code>).
	 * @param revision
	 *          The revision of the requested resource. If <code>null</code>, than
	 *          response the latest revision.
	 * @return The {@link OutputStream} containing the serialized XML file.
	 * @throws IOException
	 * @throws SirixException
	 * @throws WebApplicationException
	 */
	private final OutputStream serialize(final String resource,
			final Integer revision, final boolean nodeid, final OutputStream output,
			final boolean wrapResult) throws IOException, JaxRxException,
			SirixException {

		if (WorkerHelper.checkExistingResource(mStoragePath, resource)) {
			try {
				if (wrapResult) {
					output.write(beginResult.getBytes());
					serializIt(resource, revision, output, nodeid);
					output.write(endResult.getBytes());
				} else {
					serializIt(resource, revision, output, nodeid);
				}
			} catch (final Exception exce) {
				throw new JaxRxException(exce);
			}
		} else {
			throw new JaxRxException(404, "Not found");
		}

		return output;
	}

	/**
	 * This method reads the existing database, and offers the last revision id of
	 * the database
	 * 
	 * @param resourceName
	 *          The name of the existing database.
	 * @return The {@link OutputStream} containing the result
	 * @throws WebApplicationException
	 *           The Exception occurred.
	 * @throws SirixException
	 */
	public long getLastRevision(final String resourceName) throws JaxRxException,
			SirixException {
		long lastRevision;
		if (WorkerHelper.checkExistingResource(mStoragePath, resourceName)) {
			IDatabase database = Database.openDatabase(mStoragePath);
			INodeReadTrx rtx = null;
			ISession session = null;
			try {
				session = database.getSession(new SessionConfiguration.Builder(
						resourceName).build());
				lastRevision = session.getLastRevisionNumber();
			} catch (final Exception globExcep) {
				throw new JaxRxException(globExcep);
			} finally {
				WorkerHelper.closeRTX(rtx, session, database);
			}
		} else {
			throw new JaxRxException(404, "Resource not found");
		}
		return lastRevision;
	}

	/**
	 * This method reads the existing database, and offers all modifications of
	 * the two given revisions
	 * 
	 * @param resourceName
	 *          The name of the existing database.
	 * @param revisionRange
	 *          Contains the range of revisions
	 * @param nodeid
	 *          To response the resource with a restid for each node (
	 *          <code>true</code>) or without ( <code>false</code>).
	 * @param output
	 *          The OutputStream reference which have to be modified and returned
	 * @param wrap
	 *          <code>true</code> if the results have to be wrapped.
	 *          <code>false</code> otherwise.
	 * @return The {@link OutputStream} containing the result
	 * @throws SirixException
	 * @throws WebApplicationException
	 *           The Exception occurred.
	 */
	public OutputStream getModificHistory(
			final String resourceName, // NOPMD this method needs alls these
			// functions
			final String revisionRange, final boolean nodeid,
			final OutputStream output, final boolean wrap) throws JaxRxException,
			SirixException {

		// extract both revision from given String value
		final StringTokenizer tokenizer = new StringTokenizer(revisionRange, "-");
		final int revision1 = Integer.valueOf(tokenizer.nextToken());
		final int revision2 = Integer.valueOf(tokenizer.nextToken());

		if (revision1 < revision2 && revision2 <= getLastRevision(resourceName)) {

			// variables for highest rest-id in respectively revision
			long maxRestidRev1 = 0;
			long maxRestidRev2 = 0;

			// Connection to sirix, creating a session
			IDatabase database = null;
			AbsAxis axis = null;
			INodeReadTrx rtx = null;
			ISession session = null;
			// List for all restIds of modifications
			final List<Long> modificRestids = new LinkedList<Long>();

			// List of all restIds of revision 1
			final List<Long> restIdsRev1 = new LinkedList<Long>();

			try {
				database = Database.openDatabase(mStoragePath);
				session = database.getSession(new SessionConfiguration.Builder(
						resourceName).build());

				// get highest rest-id from given revision 1
				rtx = session.beginNodeReadTrx(revision1);
				axis = new XPathAxis(rtx, ".//*");

				while (axis.hasNext()) {
					if (rtx.getNodeKey() > maxRestidRev1) {
						maxRestidRev1 = rtx.getNodeKey();
					}
					// stores all restids from revision 1 into a list
					restIdsRev1.add(rtx.getNodeKey());
				}
				rtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
				rtx.close();

				// get highest rest-id from given revision 2
				rtx = session.beginNodeReadTrx(revision2);
				axis = new XPathAxis(rtx, ".//*");

				while (axis.hasNext()) {
					final Long nodeKey = rtx.getNodeKey();
					if (nodeKey > maxRestidRev2) {
						maxRestidRev2 = rtx.getNodeKey();
					}
					if (nodeKey > maxRestidRev1) {
						/*
						 * writes all restids of revision 2 higher than the highest restid
						 * of revision 1 into the list
						 */
						modificRestids.add(nodeKey);
					}
					/*
					 * removes all restids from restIdsRev1 that appears in revision 2 all
					 * remaining restids in the list can be seen as deleted nodes
					 */
					restIdsRev1.remove(nodeKey);
				}
				rtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
				rtx.close();

				rtx = session.beginNodeReadTrx(revision1);

				// linked list for holding unique restids from revision 1
				final List<Long> restIdsRev1New = new LinkedList<Long>();

				/*
				 * Checks if a deleted node has a parent node that was deleted too. If
				 * so, only the parent node is stored in new list to avoid double print
				 * of node modification
				 */
				for (Long nodeKey : restIdsRev1) {
					rtx.moveTo(nodeKey);
					final long parentKey = rtx.getParentKey();
					if (!restIdsRev1.contains(parentKey)) {
						restIdsRev1New.add(nodeKey);
					}
				}
				rtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
				rtx.close();

				if (wrap) {
					output.write(beginResult.getBytes());
				}
				/*
				 * Shred modified restids from revision 2 to xml fragment Just
				 * modifications done by post commands
				 */
				rtx = session.beginNodeReadTrx(revision2);

				for (Long nodeKey : modificRestids) {
					rtx.moveTo(nodeKey);
					WorkerHelper.serializeXML(session, output, false, nodeid, nodeKey,
							revision2).call();
				}
				rtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
				rtx.close();

				/*
				 * Shred modified restids from revision 1 to xml fragment Just
				 * modifications done by put and deletes
				 */
				rtx = session.beginNodeReadTrx(revision1);
				for (Long nodeKey : restIdsRev1New) {
					rtx.moveTo(nodeKey);
					WorkerHelper.serializeXML(session, output, false, nodeid, nodeKey,
							revision1).call();
				}
				if (wrap) {
					output.write(endResult.getBytes());
				}

				rtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());

			} catch (final Exception globExcep) {
				throw new JaxRxException(globExcep);
			} finally {
				WorkerHelper.closeRTX(rtx, session, database);
			}
		} else {
			throw new JaxRxException(400, "Bad user request");
		}

		return output;
	}

	/**
	 * The XML serializer to a given tnk file.
	 * 
	 * @param resource
	 *          The resource that has to be serialized.
	 * @param revision
	 *          The revision of the document.
	 * @param output
	 *          The output stream where we write the XML file.
	 * @param nodeid
	 *          <code>true</code> when you want the result nodes with node id's.
	 *          <code>false</code> otherwise.
	 * @throws WebApplicationException
	 *           The exception occurred.
	 * @throws SirixException
	 */
	private void serializIt(final String resource, final Integer revision,
			final OutputStream output, final boolean nodeid) throws JaxRxException,
			SirixException {
		// Connection to sirix, creating a session
		IDatabase database = null;
		ISession session = null;
		// INodeReadTrx rtx = null;
		try {
			database = Database.openDatabase(mStoragePath);
			session = database.getSession(new SessionConfiguration.Builder(resource)
					.build());
			// and creating a transaction
			// if (revision == null) {
			// rtx = session.beginReadTransaction();
			// } else {
			// rtx = session.beginReadTransaction(revision);
			// }
			final XMLSerializerBuilder builder;
			if (revision == null)
				builder = new XMLSerializerBuilder(session, output);
			else
				builder = new XMLSerializerBuilder(session, output, revision);
			builder.setREST(nodeid);
			builder.setID(nodeid);
			builder.setDeclaration(false);
			final XMLSerializer serializer = builder.build();
			serializer.call();
		} catch (final Exception exce) {
			throw new JaxRxException(exce);
		} finally {
			// closing the sirix storage
			WorkerHelper.closeRTX(null, session, database);
		}
	}

	/**
	 * This method reverts the latest revision data to the requested.
	 * 
	 * @param resourceName
	 *          The name of the XML resource.
	 * @param backToRevision
	 *          The revision value, which has to be set as the latest.
	 * @throws WebApplicationException
	 * @throws SirixException
	 */
	public void revertToRevision(final String resourceName,
			final int backToRevision) throws JaxRxException, SirixException {
		IDatabase database = null;
		ISession session = null;
		INodeWriteTrx wtx = null;
		boolean abort = false;
		try {
			database = Database.openDatabase(mStoragePath);
			session = database.getSession(new SessionConfiguration.Builder(
					resourceName).build());
			wtx = session.beginNodeWriteTrx();
			wtx.revertTo(backToRevision);
			wtx.commit();
		} catch (final SirixException exce) {
			abort = true;
			throw new JaxRxException(exce);
		} finally {
			WorkerHelper.closeWTX(abort, wtx, session, database);
		}
	}

}
