/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * <h1>Access to Sirix</h1>
 * <p>
 * The access semantics is as follows:
 * <ul>
 * <li>There can only be a single {@link org.sirix.api.Database} instance per Database-Folder</li>
 * <li>There can be multiple {@link org.sirix.api.ResourceManager} instances per
 * {@link org.sirix.api.Database} linked uniquely to resources representing concrete
 * data-storages.</li>
 * <li>There can only be a single {@link org.sirix.api.xdm.XdmNodeTrx} instance per
 * {@link org.sirix.api.ResourceManager}</li>
 * <li>There can be multiple {@link org.sirix.api.xdm.XdmNodeReadOnlyTrx} instances per
 * {@link org.sirix.api.ResourceManager}.</li>
 * </ul>
 * </p>
 * <p>
 * Code examples:
 *
 * <pre>
 * // DatabaseConfiguration denoted the configuration for a connected set of data resources.
 * final DatabaseConfiguration dbConfig = new DatabaseConfiguration(new File("/path/to/db/location"));
 * // Creation of a database. Returns true if successful, false if not (including existence of the database)
 * Database.createDatabase(dbConfig);
 * // Getting of database instance, will be a singleton for the denoted path
 * final IDatabase database = Database.openDatabase(new File("/path/to/db/location");
 * // Creation of a resource within the db. The creation includes the setting of versioning, etc. It must take place only one.
 * final ResourceConfiguration resourceConfig = new ResourceConfiguration.Builder(&quot;coolResource&quot;).setRevision(ERevisioning.Differential).build();
 * database.createResource(resourceConfig);
 * // Getting access via a ISession
 * final SessionConfiguration sessionConfig = new SessionConfiguration(&quot;coolResource&quot;);
 * final ISession someSession = Session.beginSession(sessionConfig);
 *
 * final IWriteTransaction someWTX = someSession.beginWriteTransaction();
 * final IReadTransaction someRTX = someSession.beginReadTransaction();
 * final IReadTransaction someConcurrentRTX = someSession.beginReadTransaction();
 *
 * someWTX.abort();
 * someWTX.close();
 * someRTX.close();
 * someConcurrentRTX.close();
 * someSession.close();
 * database.close();
 * </pre>
 *
 * </p>
 * <p>
 * Best practice to safely manipulate a sirix resource within a database if everything exists:
 *
 * <pre>
 *         final IDatabase database = Database.openDatabase(new File(&quot;/path/to/db/location&quot;);
 *         final ISession session = Session.beginSession(new SessionConfiguration(&quot;existingResource&quot;);
 *         final IWriteTransaction wtx = session.beginWriteTransaction();
 *         try {
 *           wtx.insertElementAsFirstChild("foo", "", "");
 *           ...
 *           wtx.commit();
 *         } catch (final AbsTTException exc) {
 *           wtx.abort();
 *           throw new RuntimeException(exc);
 *         } finally {
 *           wtx.close();
 *         }
 *         session.close(); // Might also stand in the finally...
 *         database.close();
 * </pre>
 *
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 */
package org.sirix.access;

