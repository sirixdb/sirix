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
 * <h1>Configuration ofTreetank</h1>
 * <p>
 * All configuration of Treetank takes place of the classes in this package.
 * <ul>
 * <li>The {@link org.treetank.access.conf.DatabaseConfiguration} defines the settings within a
 * {@link org.treetank.access.Database} (e.g. path to the database and the version where the data was
 * created). This configuration is only needed within the creation of a {@link org.treetank.access.Database}
 * and serialized.</li>
 * <li>The {@link org.treetank.access.conf.ResourceConfiguration} defines the persisted settings within a
 * {@link org.treetank.access.Session} (e.g. versioning-kind of the resource, hashing-kind, ...). This
 * configuration is only needed within the creation of a resource and serialized.</li>
 * <li>The {@link org.treetank.access.conf.SessionConfiguration} defines the runtime settings within a
 * {@link org.treetank.access.Session} (e.g. numbers of transactions, ...). This configuration is needed
 * within each instantiation of a {@link org.treetank.access.Session}.</li>
 * </ul>
 * </p>
 * <p>
 * All configurations are set within the {@link org.treetank.access.Database} only.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 */
package org.treetank.access.conf;

