/**
 * Copyright (c) 2018, Sirix
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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
package org.sirix.xquery.function.sdb.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.compiler.CompileChain;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;
import org.sirix.utils.SirixFiles;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.node.BasicDBStore;
import org.xml.sax.SAXException;
import junit.framework.TestCase;

/**
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 *
 */
public final class ImportTestCase extends TestCase {

  private Path mTempDir;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    mTempDir = Files.createTempDirectory("sirix");
  }

  @Override
  protected void tearDown() throws Exception {
    SirixFiles.recursiveRemove(mTempDir);

    super.tearDown();
  }

  public void test() {

  }

  // @Test
  // public void test() throws QueryException, IOException, SAXException {
  // final Path doc = Paths.get("src", "test", "resources", "revXMLsAll");
  //
  // // Initialize query context and store.
  // try (final DBStore store =
  // DBStore.newBuilder().location(mTempDir).buildPathSummary(false).build()) {
  // final CompileChain cc = new SirixCompileChain(store);
  // final QueryContext ctx = new SirixQueryContext(store);
  //
  // // Use XQuery to load sample document into store.
  // final String xq1 = String.format(
  // "sdb:load('mydoc.col', 'mydoc.xml', '%s')", doc.resolve("1.xml").toString());
  // new XQuery(cc, xq1).evaluate(ctx);
  //
  // // final String xq = "sdb:doc('mydoc.col', 'mydoc.xml')";
  // // final DBNode node = (DBNode) new XQuery(cc, xq).evaluate(ctx);
  // // final OutputStream out = new ByteArrayOutputStream();
  // // final XMLSerializer serializer =
  // // new XMLSerializerBuilder(node.getTrx().getResourceManager(), out).prettyPrint().build();
  // // serializer.call();
  // // System.out.println(out.toString());
  //
  // // Use XQuery to import the differences.
  // final String xq2 = String.format(
  // "sdb:import('mydoc.col', 'mydoc.xml', '%s')", doc.resolve("2.xml").toString());
  // final DBNode node = (DBNode) new XQuery(cc, xq2).evaluate(ctx);
  //
  // final OutputStream out = new ByteArrayOutputStream();
  // final XMLSerializer serializer =
  // new XMLSerializerBuilder(node.getTrx().getResourceManager(), out).build();
  // serializer.call();
  // System.out.println(out.toString());
  //
  // final StringBuilder sBuilder = TestHelper.readFile(doc.resolve("2.xml"), false);
  //
  // final Diff diff = new Diff(sBuilder.toString(), out.toString());
  // final DetailedDiff detDiff = new DetailedDiff(diff);
  // @SuppressWarnings("unchecked")
  // final List<Difference> differences = detDiff.getAllDifferences();
  // for (final Difference difference : differences) {
  // System.err.println("***********************");
  // System.err.println(difference);
  // System.err.println("***********************");
  // }
  //
  // assertTrue("pieces of XML are similar " + diff, diff.similar());
  // assertTrue("but are they identical? " + diff, diff.identical());
  // }
  // }
}
