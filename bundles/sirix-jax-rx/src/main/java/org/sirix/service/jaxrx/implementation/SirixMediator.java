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
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.core.StreamingOutput;

import org.jaxrx.JaxRx;
import org.jaxrx.core.JaxRxException;
import org.jaxrx.core.QueryParameter;
import org.jaxrx.core.ResourcePath;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.enums.IDAccessType;
import org.sirix.service.jaxrx.util.WorkerHelper;

/**
 * This class works as mediator between the JAX-RX REST interface layer and the
 * responsible sirix implementation class. It checks the requested resource path
 * and forwards it then to the responsible one.
 * 
 * @author Lukas Lewandowski, University of Konstanz
 * 
 */
public final class SirixMediator implements JaxRx {

	/**
	 * The instance of the database.
	 */
	private final DatabaseRepresentation database;

	/**
	 * The instance of access to a node id in a database.
	 */
	private final NodeIdRepresentation nodeIdResource;

	/**
	 * 
	 * Constructor.
	 * 
	 * @param storagePath
	 *          where the data should be stored
	 */
	public SirixMediator(final @Nonnull File storagePath) {
		database = new DatabaseRepresentation(storagePath);
		nodeIdResource = new NodeIdRepresentation(storagePath);
	}

	/**
	 * Not allowed message string.
	 */
	private static final String NOTALLOWEDSTRING = "Method not allowed on this resource request";

	@Override
	public String add(final InputStream input, final ResourcePath path)
			throws JaxRxException {
		final int depth = path.getDepth();
		if (depth == 1) {
			database.add(input, path.getResourcePath());
		} else if (depth == 2) {
			nodeIdResource.addSubResource(path.getResource(0),
					Long.valueOf(path.getResource(1)), input, IDAccessType.FIRSTCHILD);
		} else if (depth == 3) {
			final IDAccessType accessType = WorkerHelper.getInstance()
					.validateAccessType(path.getResource(2));
			nodeIdResource.addSubResource(path.getResource(0),
					Long.valueOf(path.getResource(1)), input, accessType);
		}
		return null;
	}

	@Override
	public StreamingOutput command(final String command, final ResourcePath path)
			throws JaxRxException {

		// Here we have to discuss.... because on command in get AND post
		// request ... enforcement of REST concept

		if ("revert".equalsIgnoreCase(command) && path.getDepth() == 1) {
			final String revision = path.getValue(QueryParameter.REVISION);
			if (revision != null) {
				try {
					database.revertToRevision(path.getResourcePath(),
							Integer.valueOf(revision));
					return null;
				} catch (final NumberFormatException exce) {
					throw new JaxRxException(400, "False value for REVISION paramter: "
							+ exce.getMessage());
				} catch (final SirixException exce) {
					throw new JaxRxException(exce);
				}
			}
		}

		throw new JaxRxException(
				403,
				"Currently only 'revert' is accepted as COMMAND query parameter in a POST request. In GET requests we do not support COMMAND query parameters");
	}

	@Override
	public String update(final InputStream input, final ResourcePath path)
			throws JaxRxException {
		final int depth = path.getDepth();
		if (depth == 1) {
			database.createResource(input, path.getResourcePath());
		} else if (depth == 2) {
			nodeIdResource.modifyResource(path.getResource(0),
					Long.valueOf(path.getResource(1)), input);
		} else {
			throw new JaxRxException(405, NOTALLOWEDSTRING);
		}
		return null;

	}

	@Override
	public String delete(final ResourcePath path) throws JaxRxException {
		final int depth = path.getDepth();
		switch (depth) {
		case 1:
			database.deleteResource(path.getResourcePath());
			break;
		case 2:
			nodeIdResource.deleteResource(path.getResource(0),
					Long.valueOf(path.getResource(1)));
			break;
		default:
			throw new JaxRxException(405, NOTALLOWEDSTRING);
		}
		return null;
	}

	@Override
	public StreamingOutput get(final ResourcePath path) throws JaxRxException {
		final int depth = path.getDepth();
		StreamingOutput response;
		switch (depth) {
		case 0:
			response = database.getResourcesNames();
			break;
		case 1:
			response = database.getResource(path.getResourcePath(),
					path.getQueryParameter());
			break;
		case 2:
			response = nodeIdResource.getResource(path.getResource(0),
					Long.valueOf(path.getResource(1)), path.getQueryParameter());
			break;
		case 3:
			final IDAccessType accessType = WorkerHelper.getInstance()
					.validateAccessType(path.getResource(2));
			if (accessType == null) {
				throw new JaxRxException(400, "The access type: " + path.getResource(2)
						+ " is not supported.");

			} else {
				response = nodeIdResource.getResourceByAT(path.getResource(0),
						Long.valueOf(path.getResource(1)), path.getQueryParameter(),
						accessType);
			}
			break;
		default:
			throw new JaxRxException(405, NOTALLOWEDSTRING);
		}
		return response;
	}

	@Override
	public Set<QueryParameter> getParameters() {
		final Set<QueryParameter> availParams = new HashSet<QueryParameter>();
		availParams.add(QueryParameter.OUTPUT);
		availParams.add(QueryParameter.QUERY);
		availParams.add(QueryParameter.REVISION);
		availParams.add(QueryParameter.WRAP);
		availParams.add(QueryParameter.COMMAND);
		return availParams;
	}

	@Override
	public StreamingOutput query(final String query, final ResourcePath path)
			throws JaxRxException {
		StreamingOutput response;
		final int depth = path.getDepth();
		switch (depth) {
		case 1:
			response = database.performQueryOnResource(path.getResourcePath(), query,
					path.getQueryParameter());
			break;
		case 2:
			response = nodeIdResource.performQueryOnResource(path.getResource(0),
					Long.valueOf(path.getResource(1)), query, path.getQueryParameter());
			break;
		default:
			throw new JaxRxException(405, NOTALLOWEDSTRING);
		}
		return response;
	}

	@Override
	public StreamingOutput run(final String file, final ResourcePath path)
			throws JaxRxException {
		throw new JaxRxException(403,
				"Currently no applicable RUN query parameter in a GET request within sirix.");
	}

}