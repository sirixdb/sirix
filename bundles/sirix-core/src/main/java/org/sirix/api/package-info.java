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
 * sirix API
 * 
 * This package contains the public sirix API. Users will have to connect to any sirix through this API.
 * Note that for common usage, only access-interfaces provided by this package should be used.
 * 
 * <h2>Usage of sirix</h2> sirix is based on three layers of interaction:
 * <ul>
 * <li>IDatabase: This layer denotes a persistent database. Each database can be created using one specific
 * <code>DatabaseConfiguration</code>. Afterwards, this configuration is valid for the whole lifetime of the
 * database.</li>
 * <li>ISession: This layer denotes a runtime access on the database. Only one Session is allowed at one time.
 * The layer has ability to provide runtime-settings as well. Especially settings regarding the
 * transaction-handling can be provided. See <code>SessionConfiguration</code> for more information</li>
 * <li>IReadTransaction/IWriteTransaction: This layer provided direct access to the database. All access to
 * nodes used either a <code>IReadTransaction</code> or <code>IWriteTransaction</code>.
 * </ul>
 * Additional to these access-interfaces, this api-packages provides direct access-methods for the
 * node-structure:
 * <ul>
 * <li>IAxis: This interface is for providing common access to different axis. All axis are listed in the
 * axis-package. The idea is to provide a method for iterating over all nodes denoted by this axis.</li>
 * <li>INode: Each node in the treestructure is encapsulated by this interface.</li>
 * <li>INodeList: To provide common usage of the included XPath 2.0-engine, this interface provides a liste
 * for the resulting items.</li>
 * <li>IFilter: Easy filtering of axis-based node-access is possible with the help if this interface.</li>
 * </ul>
 * 
 * 
 * 
 * @author Marc Kramis, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 */
package org.sirix.api;

