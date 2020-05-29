/*
 * [New BSD License] Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org> All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the Brackit Project Team nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.xquery.function.xml.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.io.IOUtils;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.function.xml.XMLFun;

/**
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Serialize extends AbstractFunction {

  public static final QNm DEFAULT_NAME = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "serialize");

  public Serialize() {
    this(DEFAULT_NAME);
  }

  /**
   * Constructor.
   *
   * @param name the qname
   */
  public Serialize(QNm name) {
    super(name,
        new Signature(new SequenceType(AtomicType.STR, Cardinality.One), SequenceType.ITEM_SEQUENCE,
            new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
        true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final Sequence sequence = args[0];
    if (sequence == null) {
      return Int32.ZERO;
    }
    final boolean format = FunUtil.getBoolean(args, 1, "prettyPrint", false, false);
    final String file = FunUtil.getString(args, 2, "file", null, null, false);
    final PrintStream buf;
    if (file == null) {
      buf = IOUtils.createBuffer();
    } else {
      try {
        buf = new PrintStream(new FileOutputStream(new File(file)));
      } catch (final FileNotFoundException e) {
        throw new QueryException(SDBFun.ERR_FILE_NOT_FOUND, e);
      }
    }
    new StringSerializer(buf).setFormat(format).serialize(sequence);
    return new Str(buf.toString());
  }
}
