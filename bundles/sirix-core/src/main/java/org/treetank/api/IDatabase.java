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

package org.treetank.api;

import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.exception.AbsTTException;
import org.treetank.exception.TTIOException;

/**
 * This interface describes database instances handled by treetank. A database
 * is a persistent place where all data is stored. The access to the data is
 * done with the help of {@link ISession}s.
 * 
 * Furthermore, databases can be created with the help of {@link org.access.conf.DatabaseConfiguration}s.
 * After creation, the settings
 * of a database cannot be changed.
 * 
 * 
 * <h2>Usage Example</h2>
 * 
 * <p>
 * <pre>
 * // Simple session with standards as defined in <code>EDatabaseSetting</code> and 
 * <code>ESessionSetting</code>. Creation takes place in open-process
 * final IDatabase database = Database.openDatabase(&quot;examplek&quot;);
 * final ISession session = database.getSession()
 * 
 * // Database with berkeley db and incremental revisioning.
 * final Properties dbProps = new Properties();
 * dbProps.setProperty(STORAGE_TYPE.name(),StorageType.Berkeley.name());
 * dbProps.setProperty(REVISION_TYPE.name(), ERevisioning.INCREMENTAL.name());
 * final DatabaseConfiguration dbConfig = new DatabaseConfiguration(&quot;example&quot;, dbProps);
 * Database.create(dbConfig);
 * final IDatabase database = Database.openDatabase(&quot;examplek&quot;);
 * final ISession session = database.getSession();
 * </pre>
 * 
 * </p>
 * 
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public interface IDatabase extends AutoCloseable {
  /**
   * Creation of a resource. Since databases can consist out of several
   * resources, those can be created within this method. This includes the
   * creation of a suitable folder structure as well as the serialization of
   * the configuration of this resource.
   * 
   * @param pResConf
   *          the config of the resource
   * @return boolean with true if successful, false otherwise
   * @throws TTIOException
   *           if anything happens while creating the resource
   */
  boolean createResource(ResourceConfiguration pResConf) throws TTIOException;

  /**
   * Getting the session associated within this database.
   * 
   * @param pSessionConf
   *          {@link SessionConfiguration.Builder} reference
   * @return the session
   * @throws AbsTTException
   *           if can't get session
   */
  ISession getSession(SessionConfiguration pSessionConf) throws AbsTTException;

  /**
   * Truncating a resource. This includes the removal of all data stored
   * within this resource.
   * 
   * @param pResConf
   *          storing the name of the resource
   * 
   */
  void truncateResource(ResourceConfiguration pResConf);

  /**
   * Closing the database for further access.
   * 
   * @throws AbsTTException
   *           if anything happens within treetank.
   */
  @Override
  void close() throws AbsTTException;

  /**
   * Get the {@link DatabaseConfiguration} associated with this database.
   * 
   * @return {@link DatabaseConfiguration} reference associated with this database
   */
  DatabaseConfiguration getDatabaseConfig();
}
