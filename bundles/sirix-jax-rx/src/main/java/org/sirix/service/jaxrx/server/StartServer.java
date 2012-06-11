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
package org.sirix.service.jaxrx.server;

import org.jaxrx.JettyServer;

/**
 * This class starts the in JAX-RX embedded Jetty server.
 * 
 * @author Lukas Lewandowski, University of Konstanz
 * 
 */
public final class StartServer {

  /**
   * The Jetty instance.
   */
  private JettyServer jetty;

  /**
   * I'm a lazy constructor.
   * 
   * @param sPort
   *          port for the REST server.
   * @throws Exception
   *           Exception occurred
   * 
   */
  public StartServer(final int sPort) throws Exception {
    System.setProperty("org.jaxrx.systemPath", "org.sirix.service.jaxrx.implementation.sirixMediator");
    System.setProperty("org.jaxrx.systemName", "sirix");
    jetty = new JettyServer(sPort);
  }

  /**
   * This method starts the embedded Jetty server.
   * 
   * @param args
   *          Not used parameter.
   * @throws Exception
   *           Exception occurred.
   */
  public static void main(final String[] args) throws Exception {
    int port = 8093;
    if (args != null && args.length > 0) {
      port = Integer.parseInt(args[0]);
    }
    System.setProperty("org.jaxrx.systemPath", "org.sirix.service.jaxrx.implementation.sirixMediator");
    System.setProperty("org.jaxrx.systemName", "sirix");
    new JettyServer(port);
  }

  /**
   * This method stops the Jetty server.
   * 
   * @throws Exception
   *           The exception occurred while stopping server.
   */
  public void stopServer() throws Exception {
    jetty.stop();
  }
}
