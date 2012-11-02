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

package org.sirix.service.jaxrx.implementation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.ws.rs.core.StreamingOutput;

import org.jaxrx.core.JaxRxException;
import org.jaxrx.core.QueryParameter;
import org.sirix.access.Databases;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.enums.IDAccessType;
import org.sirix.service.jaxrx.util.RestXPathProcessor;
import org.sirix.service.jaxrx.util.WorkerHelper;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.service.xml.serialize.XMLSerializerProperties;
import org.sirix.service.xml.shredder.Insert;

/**
 * This class is responsible to work with database specific XML node id's. It
 * allows to access a resource by a node id, modify an existing resource by node
 * id, delete an existing resource by node id and to append a new resource to an
 * existing XML element identified by a node id.
 * 
 * @author Patrick Lang, Lukas Lewandowski, University of Konstanz
 * 
 */
public class NodeIdRepresentation {

	/**
	 * This field specifies the begin result element of the request.
	 */
	private static final byte[] BEGINRESULT = "<jaxrx:result xmlns:jaxrx=\"http://jaxrx.org/\">"
			.getBytes();

	/**
	 * This field specifies the end result element of the request.
	 */
	private static final byte[] ENDRESULT = "</jaxrx:result>".getBytes();

	private static final String NOTFOUND = "Node id not found";

	/**
	 * The 'yes' string.
	 */
	private static final String YESSTRING = "yes";

	/**
	 * Storage path to the data.
	 */
	private final File mStoragePath;

	/**
	 * 
	 * Constructor.
	 * 
	 * @param pStoragePath
	 *          storage to be set
	 */
	public NodeIdRepresentation(final File pStoragePath) {
		mStoragePath = pStoragePath;
	}

	/**
	 * This method is responsible to deliver the whole XML resource addressed by a
	 * unique node id.
	 * 
	 * @param resourceName
	 *          The name of the database, where the node id belongs.
	 * @param nodeId
	 *          The unique node id of the requested resource.
	 * @param queryParams
	 *          The optional query parameters.
	 * @return The whole XML resource addressed by a unique node id.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public StreamingOutput getResource(final String resourceName,
			final long nodeId, final Map<QueryParameter, String> queryParams)
			throws JaxRxException {
		final StreamingOutput sOutput = new StreamingOutput() {
			@Override
			public void write(final OutputStream output) throws IOException,
					JaxRxException {

				// final String xPath = queryParams.get(QueryParameter.QUERY);
				final String revision = queryParams.get(QueryParameter.REVISION);
				final String wrap = queryParams.get(QueryParameter.WRAP);
				final String doNodeId = queryParams.get(QueryParameter.OUTPUT);
				final boolean wrapResult = (wrap == null) ? false : wrap
						.equalsIgnoreCase(YESSTRING);
				final boolean nodeid = (doNodeId == null) ? false : doNodeId
						.equalsIgnoreCase(YESSTRING);
				final Long rev = revision == null ? null : Long.valueOf(revision);
				serialize(resourceName, nodeId, rev, nodeid, output, wrapResult);
			}
		};

		return sOutput;
	}

	/**
	 * This method is responsible to deliver the whole XML resource addressed by a
	 * unique node id.
	 * 
	 * @param resourceName
	 *          The name of the database, where the node id belongs.
	 * @param nodeId
	 *          The unique node id of the requested resource.
	 * @param queryParams
	 *          The optional query parameters.
	 * @param accessType
	 *          The id access type to access a resource by a relative method type
	 *          defined in {@link IDAccessType}.
	 * @return The whole XML resource addressed by a unique node id.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public StreamingOutput getResourceByAT(final String resourceName,
			final long nodeId, final Map<QueryParameter, String> queryParams,
			final IDAccessType accessType) throws JaxRxException {
		final StreamingOutput sOutput = new StreamingOutput() {
			@Override
			public void write(final OutputStream output) throws IOException,
					JaxRxException {

				// final String xPath = queryParams.get(QueryParameter.QUERY);
				final String revision = queryParams.get(QueryParameter.REVISION);
				final String wrap = queryParams.get(QueryParameter.WRAP);
				final String doNodeId = queryParams.get(QueryParameter.OUTPUT);
				final boolean wrapResult = (wrap == null) ? false : wrap
						.equalsIgnoreCase(YESSTRING);
				final boolean nodeid = (doNodeId == null) ? false : doNodeId
						.equalsIgnoreCase(YESSTRING);
				final Integer rev = revision == null ? null : Integer.valueOf(revision);
				serializeAT(resourceName, nodeId, rev, nodeid, output, wrapResult,
						accessType);
			}
		};

		return sOutput;
	}

	/**
	 * This method is responsible to perform a XPath query expression on the XML
	 * resource which is addressed through a unique node id.
	 * 
	 * @param resourceName
	 *          The name of the database, the node id belongs to.
	 * @param nodeId
	 *          The node id of the requested resource.
	 * @param query
	 *          The XPath expression.
	 * @param queryParams
	 *          The optional query parameters (output, wrap, revision).
	 * @return The result of the XPath query expression.
	 */
	public StreamingOutput performQueryOnResource(final String resourceName,
			final long nodeId, final String query,
			final Map<QueryParameter, String> queryParams) {

		final StreamingOutput sOutput = new StreamingOutput() {
			@Override
			public void write(final OutputStream output) throws IOException,
					JaxRxException {

				final File dbFile = new File(mStoragePath, resourceName);
				final String revision = queryParams.get(QueryParameter.REVISION);
				final String wrap = queryParams.get(QueryParameter.WRAP);
				final String doNodeId = queryParams.get(QueryParameter.OUTPUT);
				final boolean wrapResult = (wrap == null) ? true : wrap
						.equalsIgnoreCase(YESSTRING);
				final boolean nodeid = (doNodeId == null) ? false : doNodeId
						.equalsIgnoreCase(YESSTRING);
				final Integer rev = revision == null ? null : Integer.valueOf(revision);
				final RestXPathProcessor xpathProcessor = new RestXPathProcessor(
						mStoragePath);
				try {
					xpathProcessor.getXpathResource(dbFile, nodeId, query, nodeid, rev,
							output, wrapResult);
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		};

		return sOutput;
	}

	/**
	 * This method is responsible to delete an XML resource addressed through a
	 * unique node id (except root node id).
	 * 
	 * @param resourceName
	 *          The name of the database, which the node id belongs to.
	 * @param nodeId
	 *          The unique node id.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public void deleteResource(final String resourceName, final long nodeId)
			throws JaxRxException {
		synchronized (resourceName) {
			Session session = null;
			Database database = null;
			NodeWriteTrx wtx = null;
			boolean abort = false;
			if (WorkerHelper.checkExistingResource(mStoragePath, resourceName)) {
				try {
					database = Databases.openDatabase(mStoragePath);
					// Creating a new session
					session = database.getSession(new SessionConfiguration.Builder(
							resourceName).build());
					// Creating a write transaction
					wtx = session.beginNodeWriteTrx();
					// move to node with given rest id and deletes it
					if (wtx.moveTo(nodeId).hasMoved()) {
						wtx.remove();
						wtx.commit();
					} else {
						// workerHelper.closeWTX(abort, wtx, session, database);
						throw new JaxRxException(404, NOTFOUND);
					}
				} catch (final SirixException exce) {
					abort = true;
					throw new JaxRxException(exce);
				} finally {
					try {
						WorkerHelper.closeWTX(abort, wtx, session, database);
					} catch (final SirixException exce) {
						throw new JaxRxException(exce);
					}
				}
			} else {
				throw new JaxRxException(404, "DB not found");
			}
		}
	}

	/**
	 * This method is responsible to modify the XML resource, which is addressed
	 * through a unique node id.
	 * 
	 * @param resourceName
	 *          The name of the database, where the node id belongs to.
	 * @param nodeId
	 *          The node id.
	 * @param newValue
	 *          The new value of the node that has to be replaced.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public void modifyResource(final String resourceName, final long nodeId,
			final InputStream newValue) throws JaxRxException {
		synchronized (resourceName) {
			Session session = null;
			Database database = null;
			NodeWriteTrx wtx = null;
			boolean abort = false;
			if (WorkerHelper.checkExistingResource(mStoragePath, resourceName)) {
				try {
					database = Databases.openDatabase(mStoragePath);
					// Creating a new session
					session = database.getSession(new SessionConfiguration.Builder(
							resourceName).build());
					// Creating a write transaction
					wtx = session.beginNodeWriteTrx();

					if (wtx.moveTo(nodeId).hasMoved()) {
						final long parentKey = wtx.getParentKey();
						wtx.remove();
						wtx.moveTo(parentKey);
						WorkerHelper.shredInputStream(wtx, newValue, Insert.ASFIRSTCHILD);

					} else {
						// workerHelper.closeWTX(abort, wtx, session, database);
						throw new JaxRxException(404, NOTFOUND);
					}

				} catch (final SirixException exc) {
					abort = true;
					throw new JaxRxException(exc);
				} finally {
					try {
						WorkerHelper.closeWTX(abort, wtx, session, database);
					} catch (final SirixException exce) {
						throw new JaxRxException(exce);
					}
				}
			} else {
				throw new JaxRxException(404, "Requested resource not found");
			}
		}
	}

	/**
	 * This method is responsible to perform a POST request to a node id. This
	 * method adds a new XML subtree as first child or as right sibling to the
	 * node which is addressed through a node id.
	 * 
	 * @param resourceName
	 *          The name of the database, the node id belongs to.
	 * @param nodeId
	 *          The node id.
	 * @param input
	 *          The new XML subtree.
	 * @param type
	 *          The type which indicates if the new subtree has to be inserted as
	 *          right sibling or as first child.
	 * @throws JaxRxException
	 *           The exception occurred.
	 */
	public void addSubResource(final String resourceName, final long nodeId,
			final InputStream input, final IDAccessType type) throws JaxRxException {
		Session session = null;
		Database database = null;
		NodeWriteTrx wtx = null;
		synchronized (resourceName) {
			boolean abort;
			if (WorkerHelper.checkExistingResource(mStoragePath, resourceName)) {
				abort = false;
				try {

					database = Databases.openDatabase(mStoragePath);
					// Creating a new session
					session = database.getSession(new SessionConfiguration.Builder(
							resourceName).build());
					// Creating a write transaction
					wtx = session.beginNodeWriteTrx();
					final boolean exist = wtx.moveTo(nodeId).hasMoved();
					if (exist) {
						if (type == IDAccessType.FIRSTCHILD) {
							WorkerHelper.shredInputStream(wtx, input, Insert.ASFIRSTCHILD);
						} else if (type == IDAccessType.RIGHTSIBLING) {
							WorkerHelper.shredInputStream(wtx, input, Insert.ASRIGHTSIBLING);
						} else if (type == IDAccessType.LASTCHILD) {
							if (wtx.moveToFirstChild().hasMoved()) {
								long last = wtx.getNodeKey();
								while (wtx.moveToRightSibling().hasMoved()) {
									last = wtx.getNodeKey();
								}
								wtx.moveTo(last);
								WorkerHelper
										.shredInputStream(wtx, input, Insert.ASRIGHTSIBLING);

							} else {
								throw new JaxRxException(404, NOTFOUND);
							}

						} else if (type == IDAccessType.LEFTSIBLING
								&& wtx.moveToLeftSibling().hasMoved()) {

							WorkerHelper.shredInputStream(wtx, input, Insert.ASRIGHTSIBLING);

						}
					} else {
						throw new JaxRxException(404, NOTFOUND);
					}
				} catch (final JaxRxException exce) { // NOPMD due
					// to
					// different
					// exception
					// types
					abort = true;
					throw exce;
				} catch (final Exception exce) {
					abort = true;
					throw new JaxRxException(exce);
				} finally {
					try {
						WorkerHelper.closeWTX(abort, wtx, session, database);
					} catch (final SirixException exce) {
						throw new JaxRxException(exce);
					}
				}
			}
		}
	}

	/**
	 * This method serializes requested resource
	 * 
	 * @param resource
	 *          The requested resource
	 * @param nodeId
	 *          The node id of the requested resource.
	 * @param revision
	 *          The revision of the requested resource.
	 * @param doNodeId
	 *          Specifies whether the node id's have to be shown in the result.
	 * @param output
	 *          The output stream to be written.
	 * @param wrapResult
	 *          Specifies whether the result has to be wrapped with a result
	 *          element.
	 */
	private void serialize(final String resource, final long nodeId,
			final Long revision, final boolean doNodeId, final OutputStream output,
			final boolean wrapResult) {
		if (WorkerHelper.checkExistingResource(mStoragePath, resource)) {
			Session session = null;
			Database database = null;
			try {
				database = Databases.openDatabase(mStoragePath);
				session = database
						.getSession(new SessionConfiguration.Builder(resource).build());
				if (wrapResult) {
					output.write(BEGINRESULT);
					final XMLSerializerProperties props = new XMLSerializerProperties();
					final XMLSerializerBuilder builder = new XMLSerializerBuilder(
							session, nodeId, output, props);
					builder.isRESTful(doNodeId);
					builder.setID(doNodeId);
					builder.setDeclaration(false);
					final XMLSerializer serializer = builder.build();
					serializer.call();
					output.write(ENDRESULT);
				} else {
					final XMLSerializerProperties props = new XMLSerializerProperties();
					final XMLSerializerBuilder builder = new XMLSerializerBuilder(
							session, nodeId, output, props);
					builder.isRESTful(doNodeId);
					builder.setID(doNodeId);
					builder.setDeclaration(false);
					final XMLSerializer serializer = builder.build();
					serializer.call();

				}
			} catch (final SirixException ttExcep) {
				throw new JaxRxException(ttExcep);
			} catch (final IOException ioExcep) {
				throw new JaxRxException(ioExcep);
			} catch (final Exception globExcep) {
				if (globExcep instanceof JaxRxException) { // NOPMD due
					// to
					// different
					// exception
					// types
					throw (JaxRxException) globExcep;
				} else {
					throw new JaxRxException(globExcep);
				}
			} finally {
				try {
					WorkerHelper.closeRTX(null, session, database);
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}

		} else {
			throw new JaxRxException(404, "Resource does not exist");
		}

	}

	/**
	 * This method serializes requested resource with an access type.
	 * 
	 * @param resource
	 *          The requested resource
	 * @param nodeId
	 *          The node id of the requested resource.
	 * @param revision
	 *          The revision of the requested resource.
	 * @param doNodeId
	 *          Specifies whether the node id's have to be shown in the result.
	 * @param output
	 *          The output stream to be written.
	 * @param wrapResult
	 *          Specifies whether the result has to be wrapped with a result
	 *          element.
	 * @param accessType
	 *          The {@link IDAccessType} which indicates the access to a special
	 *          node.
	 */
	private void serializeAT(final String resource, final long nodeId,
			final Integer revision, final boolean doNodeId,
			final OutputStream output, final boolean wrapResult,
			final IDAccessType accessType) {
		if (WorkerHelper.checkExistingResource(mStoragePath, resource)) {
			Session session = null;
			Database database = null;
			NodeReadTrx rtx = null;
			try {
				database = Databases.openDatabase(mStoragePath);
				session = database
						.getSession(new SessionConfiguration.Builder(resource).build());
				if (revision == null) {
					rtx = session.beginNodeReadTrx();
				} else {
					rtx = session.beginNodeReadTrx(revision);
				}

				if (rtx.moveTo(nodeId).hasMoved()) {
					switch (accessType) {
					case FIRSTCHILD:
						if (!rtx.moveToFirstChild().hasMoved())
							throw new JaxRxException(404, NOTFOUND);
						break;
					case LASTCHILD:
						if (rtx.moveToFirstChild().hasMoved()) {
							long last = rtx.getNodeKey();
							while (rtx.moveToRightSibling().hasMoved()) {
								last = rtx.getNodeKey();
							}
							rtx.moveTo(last);
						} else {
							throw new JaxRxException(404, NOTFOUND);
						}
						break;
					case RIGHTSIBLING:
						if (!rtx.moveToRightSibling().hasMoved())
							throw new JaxRxException(404, NOTFOUND);
						break;
					case LEFTSIBLING:
						if (!rtx.moveToLeftSibling().hasMoved())
							throw new JaxRxException(404, NOTFOUND);
						break;
					default: // nothing to do;
					}
					if (wrapResult) {
						output.write(BEGINRESULT);
						final XMLSerializerProperties props = new XMLSerializerProperties();
						final XMLSerializerBuilder builder = new XMLSerializerBuilder(
								session, rtx.getNodeKey(), output, props);
						builder.isRESTful(doNodeId);
						builder.setID(doNodeId);
						builder.setDeclaration(false);
						builder.doIndend(false);
						final XMLSerializer serializer = builder.build();
						serializer.call();

						output.write(ENDRESULT);
					} else {
						final XMLSerializerProperties props = new XMLSerializerProperties();
						final XMLSerializerBuilder builder = new XMLSerializerBuilder(
								session, rtx.getNodeKey(), output, props);
						builder.isRESTful(doNodeId);
						builder.setID(doNodeId);
						builder.setDeclaration(false);
						builder.doIndend(false);
						final XMLSerializer serializer = builder.build();
						serializer.call();
					}
				} else {
					throw new JaxRxException(404, NOTFOUND);
				}
			} catch (final SirixException ttExcep) {
				throw new JaxRxException(ttExcep);
			} catch (final IOException ioExcep) {
				throw new JaxRxException(ioExcep);
			} catch (final Exception globExcep) {
				if (globExcep instanceof JaxRxException) { // NOPMD due
					// to
					// different
					// exception
					// types
					throw (JaxRxException) globExcep;
				} else {
					throw new JaxRxException(globExcep);
				}
			} finally {
				try {
					WorkerHelper.closeRTX(rtx, session, database);
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		} else {
			throw new JaxRxException(404, "Resource does not exist");
		}

	}

}