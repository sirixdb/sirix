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

package org.sirix.saxon.wrapper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLTestCase;
import org.custommonkey.xmlunit.XMLUnit;
import org.custommonkey.xmlunit.examples.RecursiveElementNameAndTextQualifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.exception.AbsTTException;
import org.sirix.saxon.evaluator.XSLTEvaluator;

/**
 * Test XSLT S9Api.
 * 
 * @author johannes
 * 
 */
public final class TestNodeWrapperS9ApiXSLT extends XMLTestCase {

  /** Stylesheet file. */
  private static final File STYLESHEET = new File("src" + File.separator + "test" + File.separator
    + "resources" + File.separator + "styles" + File.separator + "books.xsl");

  private Holder mHolder;

  @Override
  @Before
  public void setUp() throws Exception {
    BookShredding.createBookDB();
    mHolder = Holder.generateRtx();

    saxonTransform(BookShredding.BOOKS, STYLESHEET);

    XMLUnit.setIgnoreWhitespace(true);
  }

  @Override
  @After
  public void tearDown() throws AbsTTException {
    mHolder.close();
    TestHelper.deleteEverything();
  }

  @Test
  public void testWithoutSerializer() throws Exception {
    final OutputStream out =
      new XSLTEvaluator(mHolder.getSession(), STYLESHEET, new ByteArrayOutputStream()).call();

    final StringBuilder sBuilder = readFile();

    final Diff diff = new Diff(sBuilder.toString(), out.toString());
    diff.overrideElementQualifier(new RecursiveElementNameAndTextQualifier());

    assertTrue(diff.toString(), diff.similar());
  }

  @Test
  public void testWithSerializer() throws Exception {
    final Serializer serializer = new Serializer();
    serializer.setOutputProperty(Serializer.Property.METHOD, "xml");
    serializer.setOutputProperty(Serializer.Property.INDENT, "yes");

    final OutputStream out =
      new XSLTEvaluator(mHolder.getSession(), STYLESHEET, new ByteArrayOutputStream(), serializer).call();

    final StringBuilder sBuilder = readFile();

    final Diff diff = new Diff(sBuilder.toString(), out.toString());
    diff.overrideElementQualifier(new RecursiveElementNameAndTextQualifier());

    assertTrue(diff.toString(), diff.similar());
  }

  /**
   * Transform source document with the given stylesheet.
   * 
   * @param xml
   *          Source xml file.
   * @param stylesheet
   *          Stylesheet to transform sourc xml file.
   * @throws SaxonApiException
   *           Exception from Saxon in case anything goes wrong.
   */
  @Ignore("Not a test, utility method only")
  public void saxonTransform(final File xml, final File stylesheet) throws SaxonApiException {
    final Processor proc = new Processor(false);
    final XsltCompiler comp = proc.newXsltCompiler();
    final XsltExecutable exp = comp.compile(new StreamSource(stylesheet));
    final XdmNode source = proc.newDocumentBuilder().build(new StreamSource(xml));
    final Serializer out = new Serializer();
    out.setOutputProperty(Serializer.Property.METHOD, "xml");
    out.setOutputProperty(Serializer.Property.INDENT, "yes");
    out.setOutputFile(new File(TestHelper.PATHS.PATH1.getFile(), "books1.html"));
    final XsltTransformer trans = exp.load();
    trans.setInitialContextNode(source);
    trans.setDestination(out);
    trans.transform();
  }

  /**
   * Read file, which has been generated by "pure" Saxon.
   * 
   * @return StringBuilder instance, which has the string representation of
   *         the document.
   * @throws IOException
   *           throws an IOException if any I/O operation fails.
   */
  @Ignore("Not a test, utility method only")
  public StringBuilder readFile() throws IOException {
    final BufferedReader in =
      new BufferedReader(new FileReader(new File(TestHelper.PATHS.PATH1.getFile(), "books1.html")));
    final StringBuilder sBuilder = new StringBuilder();
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      sBuilder.append(line + "\n");
    }

    // Remove last newline.
    sBuilder.replace(sBuilder.length() - 1, sBuilder.length(), "");
    in.close();

    return sBuilder;
  }

}
