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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.enums.EIdAccessType;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.service.xml.serialize.XMLSerializerProperties;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * This class contains methods that are respectively used by this worker classes
 * (NodeIdRepresentation.java and DatabaseRepresentation.java)
 * 
 * @author Patrick Lang, Lukas Lewandowski, University of Konstanz
 * 
 */

public final class WorkerHelper {

  /**
   * The map containing the available access types.
   */
  private final transient Map<String, EIdAccessType> typeList;

  /**
   * This constructor initializes the {@link EIdAccessType}s.
   */
  private WorkerHelper() {
    typeList = new HashMap<String, EIdAccessType>();
    typeList.put("FIRSTCHILD()", EIdAccessType.FIRSTCHILD);
    typeList.put("LASTCHILD()", EIdAccessType.LASTCHILD);
    typeList.put("RIGHTSIBLING()", EIdAccessType.RIGHTSIBLING);
    typeList.put("LEFTSIBLING()", EIdAccessType.LEFTSIBLING);
  }

  /**
   * The instance variable for singleton.
   */
  private static final transient WorkerHelper INSTANCE = new WorkerHelper();

  /**
   * Shreds a given InputStream
   * 
   * @param wtx
   *          current write transaction reference
   * @param value
   *          InputStream to be shred
   */
  public static void shredInputStream(final INodeWriteTrx wtx, final InputStream value,
    final EInsert child) {
    final XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XMLEventReader parser;
    try {
      parser = factory.createXMLEventReader(value);
    } catch (final XMLStreamException xmlse) {
      throw new WebApplicationException(xmlse);
    }

    try {
      final XMLShredder shredder = new XMLShredder(wtx, parser, child);
      shredder.call();
    } catch (final Exception exce) {
      throw new WebApplicationException(exce);
    }
  }

  /**
   * This method checks if the file is available and not empty.
   * 
   * @param storagePath
   *          the storage path
   * @param resource
   *          The file that will be checked.
   * @return <code>true</code> when the file exists and is not empty. <code>false</code> otherwise.
   */
  public static boolean checkExistingResource(final File storagePath, final String resource) {
    final File resourceFile =
      new File(new File(storagePath, DatabaseConfiguration.Paths.Data.getFile().getName()), resource);

    boolean isExisting;
    if (resourceFile.getTotalSpace() > 0) {
      isExisting = true;
    } else {
      isExisting = false;
    }
    return isExisting;
  }

  /**
   * This method creates a new XMLSerializer reference
   * 
   * @param session
   *          Associated session.
   * @param out
   *          OutputStream
   * 
   * @param serializeXMLDec
   *          specifies whether XML declaration should be shown
   * @param serializeRest
   *          specifies whether node id should be shown
   * 
   * @return new XMLSerializer reference
   */
  public static XMLSerializer serializeXML(final ISession session, final OutputStream out,
    final boolean serializeXMLDec, final boolean serializeRest, final Long revision) {
    final XMLSerializerBuilder builder;
    if (revision == null)
      builder = new XMLSerializerBuilder(session, out);
    else
      builder = new XMLSerializerBuilder(session, out, revision);
    builder.setREST(serializeRest);
    builder.setID(serializeRest);
    builder.setDeclaration(serializeXMLDec);
    final XMLSerializer serializer = builder.build();
    return serializer;
  }

  /**
   * This method creates a new XMLSerializer reference
   * 
   * @param session
   *          Associated session.
   * @param out
   *          OutputStream
   * 
   * @param serializeXMLDec
   *          specifies whether XML declaration should be shown
   * @param serializeRest
   *          specifies whether node id should be shown
   * 
   * @return new XMLSerializer reference
   */
  public static XMLSerializer serializeXML(final ISession session, final OutputStream out,
    final boolean serializeXMLDec, final boolean serializeRest, final Long nodekey, final Long revision) {
    final XMLSerializerProperties props = new XMLSerializerProperties();
    final XMLSerializerBuilder builder;
    if (revision == null && nodekey == null) {
      builder = new XMLSerializerBuilder(session, out);
    } else if (revision != null && nodekey == null) {
      builder = new XMLSerializerBuilder(session, out, revision);
    } else if (revision == null && nodekey != null) {
      builder = new XMLSerializerBuilder(session, nodekey, out, props);
    } else {
      assert revision != null;
      builder = new XMLSerializerBuilder(session, nodekey, out, props, revision);
    }
    builder.setREST(serializeRest).setID(serializeRest).setDeclaration(serializeXMLDec).setIndend(false);
    final XMLSerializer serializer = builder.build();
    return serializer;
  }

  /**
   * This method creates a new StringBuilder reference
   * 
   * @return new StringBuilder reference
   */
  public StringBuilder createStringBuilderObject() {
    return new StringBuilder();
  }

  /**
   * This method closes all open sirix connections concerning a
   * NodeWriteTrx.
   * 
   * @param abortTransaction
   *          <code>true</code> if the transaction has to be aborted, <code>false</code> otherwise.
   * @param wtx
   *          INodeWriteTrx to be closed
   * @param ses
   *          ISession to be closed
   * @param dbase
   *          IDatabase to be closed
   * @throws sirixException
   */
  public static void closeWTX(final boolean abortTransaction, final INodeWriteTrx wtx, final ISession ses,
    final IDatabase dbase) throws SirixException {
    synchronized (dbase) {
      if (abortTransaction) {
        wtx.abort();
      }
      dbase.close();
    }
  }

  /**
   * This method closes all open sirix connections concerning a
   * NodeReadTrx.
   * 
   * @param rtx
   *          INodeReadTrx to be closed
   * @param ses
   *          ISession to be closed
   * @param dbase
   *          IDatabase to be closed
   * @throws SirixException
   */
  public static void closeRTX(final INodeReadTrx rtx, final ISession ses, final IDatabase dbase)
    throws SirixException {
    synchronized (dbase) {
      dbase.close();
    }
  }

  /**
   * This method checks the variable URL path after the node id resource (e.g.
   * http://.../factbook/3/[ACCESSTYPE]) for the available access type to
   * identify a node. The access types are defined in {@link EIdAccessType}.
   * 
   * @param accessType
   *          The access type as String value encoded in the URL request.
   * @return The valid access type or null otherwise.
   */
  public EIdAccessType validateAccessType(final String accessType) {
    return typeList.get(accessType.toUpperCase(Locale.US));
  }

  /**
   * This method return the singleton instance.
   * 
   * @return The single instance.
   */
  public static WorkerHelper getInstance() {
    return INSTANCE;
  }
}
