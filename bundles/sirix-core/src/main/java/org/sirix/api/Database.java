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

package org.sirix.api;

import java.nio.file.Path;
import java.util.List;
import javax.annotation.Nonnegative;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;

/**
 * <p>
 * This interface describes database instances handled by Sirix. A database is a persistent place
 * where all data is stored. {@link ResourceManager}s are used to access the data in individual
 * resources.
 * </p>
 *
 * <p>
 * Furthermore, databases are created by {@link org.access.conf.DatabaseConfiguration}s. After
 * creation, the settings of a database cannot be changed.
 * </p>
 *
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public interface Database<T extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    extends AutoCloseable {
  /**
   * Creation of a resource. Since databases can consist out of several resources, those can be
   * created within this method. This includes the creation of a suitable folder structure as well as
   * the serialization of the configuration of this resource.
   *
   * @param config the config of the resource
   * @return {@code true} if successful, {@code false} otherwise
   * @throws SirixIOException if anything happens while creating the resource
   */
  boolean createResource(ResourceConfiguration config) throws SirixIOException;

  /**
   * Is the resource within this database existing?
   *
   * @param resourceName resource to be checked
   * @return {@code true}, if existing, {@code false} otherwise
   */
  boolean existsResource(String resourceName);

  /**
   * List all resources within this database.
   *
   * @return all resources
   */
  List<Path> listResources();

  /**
   * Getting the resource manager to open and work with a resource stored in this database.
   *
   * @param resourceName the resource to work on
   * @return the session
   * @throws SirixException if can't get session
   */
  T getResourceManager(String resourceName) throws SirixException;

  /**
   * Truncating a resource. This includes the removal of all data stored within this resource.
   *
   * @param resourceName resource name
   */
  Database<T> removeResource(String resourceName);

  /**
   * Closing the database for further access.
   *
   * @throws SirixException if anything happens within sirix.
   */
  @Override
  void close() throws SirixException;

  /**
   * Get the {@link DatabaseConfiguration} associated with this database.
   *
   * @return {@link DatabaseConfiguration} reference associated with this database
   */
  DatabaseConfiguration getDatabaseConfig();

  /**
   * Begin a database wide transaction.
   *
   * @return the started transaction
   */
  Transaction beginTransaction();

  /**
   * Get the resource name associated with the given ID.
   *
   * @param id unique ID of resource
   * @return resource name
   * @throws IllegalArgumentException if {@code pID} is negative
   */
  String getResourceName(@Nonnegative long id);

  /**
   * Get the resource-ID associated with the given resource name.
   *
   * @param name name of resource
   * @return ID of resource with the given name
   * @throws NullPointerException if {@code pName} is {@code null}
   */
  long getResourceID(String name);
}
