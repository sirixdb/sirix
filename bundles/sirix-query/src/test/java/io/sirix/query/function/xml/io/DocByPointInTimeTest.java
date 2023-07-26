/*
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
package io.sirix.query.function.xml.io;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import io.sirix.utils.XmlDocumentCreator;
import junit.framework.TestCase;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.XQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class DocByPointInTimeTest extends TestCase {
  /** The {@link Holder} instance. */
  private Holder holder;

  @BeforeEach
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @AfterEach
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void test() throws QueryException {
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXdmNodeWriteTrx());
    holder.getXdmNodeWriteTrx().close();

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();

    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = database.toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq1 = "xml:open('" + dbName + "','" + resName + "', xs:dateTime(\"2219-05-01T00:00:00\"))";

      // final String xq1 =
      // "(xs:dateTime(\"2019-05-01T00:00:00-00:00\") - xs:dateTime(\"1970-01-01T00:00:00-00:00\")) div
      // xs:dayTimeDuration('PT0.001S')";

      final XQuery query = new XQuery(SirixCompileChain.createWithNodeStore(store), xq1);
      final XmlDBNode node = (XmlDBNode) query.evaluate(ctx);

      assertEquals(5, node.getTrx().getRevisionNumber());
    }
  }
}
