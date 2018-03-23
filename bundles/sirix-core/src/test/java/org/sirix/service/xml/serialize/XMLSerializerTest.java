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

package org.sirix.service.xml.serialize;

import static org.junit.Assert.assertEquals;
import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.utils.DocumentCreater;

public class XMLSerializerTest {

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    TestHelper.closeEverything();
  }

  @Test
  public void testXMLSerializer() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final ResourceManager manager = database.getResourceManager(
        new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
    final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();

    // Generate from this session.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XMLSerializer serializer =
        new XMLSerializerBuilder(manager, out).emitXMLDeclaration().build();
    serializer.call();
    assertEquals(DocumentCreater.XML, out.toString());
    manager.close();
  }

  @Test
  public void testRestSerializer() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final ResourceManager manager = database.getResourceManager(
        new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
    final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();

    // Generate from this session.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XMLSerializer serializer =
        XMLSerializer.newBuilder(manager, out).emitRESTful().emitIDs().emitXMLDeclaration().build();
    serializer.call();
    assertEquals(DocumentCreater.REST, out.toString());

    manager.close();
  }

  @Test
  public void testIDSerializer() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final ResourceManager manager = database.getResourceManager(
        new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
    final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();

    // Generate from this session.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XMLSerializer serializer =
        new XMLSerializerBuilder(manager, out).emitIDs().emitXMLDeclaration().build();
    serializer.call();
    assertEquals(DocumentCreater.ID, out.toString());
    manager.close();
  }

  @Test
  public void testSampleCompleteSerializer() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final ResourceManager manager = database.getResourceManager(
        new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
    final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    // generate serialize all from this session
    DocumentCreater.createVersioned(wtx);
    wtx.close();

    XMLSerializer serializerall =
        new XMLSerializerBuilder(manager, out, -1).emitXMLDeclaration().build();
    serializerall.call();
    assertEquals(DocumentCreater.VERSIONEDXML, out.toString());
    out.reset();

    serializerall = new XMLSerializerBuilder(manager, out, 1, 2, 3).emitXMLDeclaration().build();
    serializerall.call();
    assertEquals(DocumentCreater.VERSIONEDXML, out.toString());
    manager.close();
  }

  /**
   * This test check the XPath //books expression and expects 6 books as result. But the failure is,
   * that only the children of the books will be serialized and NOT the book node itself.
   */
  @Test
  public void testKeyStart() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final ResourceManager manager = database.getResourceManager(
        new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
    final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    // generate serialize all from this session
    DocumentCreater.createVersioned(wtx);
    wtx.commit();
    wtx.close();

    XMLSerializer serializerall = new XMLSerializerBuilder(manager, 5l, out,
        new XMLSerializerProperties()).emitXMLDeclaration().build();
    serializerall.call();
    final String result =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><b>foo<c/></b>";

    assertEquals(result, out.toString());
    out.reset();

    serializerall = new XMLSerializerBuilder(manager, out, 1, 2, 3).emitXMLDeclaration().build();
    serializerall.call();
    assertEquals(DocumentCreater.VERSIONEDXML, out.toString());
    manager.close();
  }
}
