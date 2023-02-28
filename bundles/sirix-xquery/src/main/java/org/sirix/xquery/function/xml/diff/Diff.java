/**
 * Copyright (c) 2018, Sirix
 * <p>
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the <organization> nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
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
package org.sirix.xquery.function.xml.diff;

import com.google.api.client.util.Objects;
import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.Utils;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.diff.DiffObserver;
import org.sirix.diff.DiffTuple;
import org.sirix.node.NodeKind;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * Function for diffing two revisions of a resource in a collection/database. The Supported
 * signature is:
 * </p>
 *
 * <pre>
 * <code>sdb:diff($coll as xs:string, $res as xs:string, $rev1 as xs:int, $rev2 as xs:int) as xs:string</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Diffing of two versions of a resource.", parameters = {
    "$coll, $res, $rev1, $rev2" })
public final class Diff extends AbstractFunction implements DiffObserver {

  /**
   * Sort by document order name.
   */
  public final static QNm DIFF = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "diff");

  private final StringBuilder buf;

  private final List<DiffTuple> diffs;

  private final ExecutorService pool;

  private CountDownLatch latch;

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public Diff(final QNm name, final Signature signature) {
    super(name, signature, true);

    buf = new StringBuilder();
    diffs = new ArrayList<>();
    pool = Executors.newSingleThreadExecutor();
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final XmlDBCollection col = (XmlDBCollection) ctx.getNodeStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String expResName = ((Str) args[1]).stringValue();
    final int rev1 = FunUtil.getInt(args, 2, "revision1", -1, null, false);
    final int rev2 = FunUtil.getInt(args, 3, "revision2", -1, null, false);
    final XmlDBNode doc = col.getDocument(expResName);

    diffs.clear();
    buf.setLength(0);
    latch = new CountDownLatch(1);

    try (final XmlResourceSession resMrg = doc.getTrx().getResourceSession()) {
      pool.submit(() -> DiffFactory.invokeFullXmlDiff(new DiffFactory.Builder<>(resMrg,
                                                                                rev2,
                                                                                rev1,
                                                                                resMrg.getResourceConfig().hashType
                                                                                    == HashType.NONE
                                                                                    ? DiffOptimized.NO
                                                                                    : DiffOptimized.HASHED,
                                                                                ImmutableSet.of(this)).skipSubtrees(true)));

      try {
        latch.await(100000, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new QueryException(new QNm("Interrupted exception"), e);
      }

      if (diffs.size() == 1 && (diffs.get(0).getDiff() == DiffType.SAMEHASH
          || diffs.get(0).getDiff() == DiffType.SAME)) {
        return null;
      }

      final Set<Long> nodeKeysOfInserts = diffs.stream()
                                               .filter(tuple -> tuple.getDiff() == DiffType.INSERTED
                                                   || tuple.getDiff() == DiffType.REPLACEDNEW)
                                               .map(DiffTuple::getNewNodeKey)
                                               .collect(Collectors.toSet());

      final Set<Long> nodeKeysOfDeletes = diffs.stream()
                                               .filter(tuple -> tuple.getDiff() == DiffType.DELETED
                                                   || tuple.getDiff() == DiffType.REPLACEDOLD)
                                               .map(DiffTuple::getOldNodeKey)
                                               .collect(Collectors.toSet());

      buf.append("let $doc := ");
      createDocString(args, rev1);
      buf.append(System.getProperty("line.separator"));
      buf.append("return (");
      buf.append(System.getProperty("line.separator"));

      try (final XmlNodeReadOnlyTrx oldRtx = resMrg.beginNodeReadOnlyTrx(rev1);
           final XmlNodeReadOnlyTrx newRtx = resMrg.beginNodeReadOnlyTrx(rev2)) {

        final Iterator<DiffTuple> iter = diffs.iterator();
        while (iter.hasNext()) {
          final DiffTuple tuple = iter.next();

          final DiffType diffType = tuple.getDiff();

          if (diffType == DiffType.SAME || diffType == DiffType.SAMEHASH || diffType == DiffType.REPLACEDOLD) {
            iter.remove();
          } else if (diffType == DiffType.INSERTED || diffType == DiffType.REPLACEDNEW) {
            newRtx.moveTo(tuple.getNewNodeKey());

            if ((newRtx.isAttribute() || newRtx.isNamespace()) && nodeKeysOfInserts.contains(newRtx.getParentKey()))
              iter.remove();
          }

          if (diffType == DiffType.DELETED) {
            oldRtx.moveTo(tuple.getOldNodeKey());

            if ((oldRtx.isAttribute() || oldRtx.isNamespace()) && nodeKeysOfDeletes.contains(oldRtx.getParentKey()))
              iter.remove();
          }
        }

        if (diffs.isEmpty())
          return null;

        // Plain old for-loop as Java is still missing an indexed forEach(...) loop (on a
        // collection).
        for (int i = 0, length = diffs.size(); i < length; i++) {
          final DiffTuple diffTuple = diffs.get(i);
          final DiffType diffType = diffTuple.getDiff();
          newRtx.moveTo(diffTuple.getNewNodeKey());
          oldRtx.moveTo(diffTuple.getOldNodeKey());

          switch (diffType) {
            case INSERTED:
              if ((newRtx.isAttribute() || newRtx.isNameNode()) && nodeKeysOfInserts.contains(newRtx.getParentKey()))
                continue;

              if (newRtx.isAttribute()) {
                buf.append("  insert node ").append(printNode(newRtx)).append(" into sdb:select-item($doc");
                buf.append(",");
                buf.append(newRtx.getParentKey());
                buf.append(")");
              } else {
                buf.append("  insert nodes ");
                buf.append(printSubtreeNode(newRtx));

                if (oldRtx.isDocumentRoot()) {
                  buildUpdateStatement(newRtx);
                } else {
                  buildUpdateStatement(oldRtx);
                }
              }

              if (i + 1 < length)
                buf.append(",");

              buf.append(System.getProperty("line.separator"));
              break;
            case DELETED:
              if ((newRtx.isAttribute() || newRtx.isNameNode()) && nodeKeysOfInserts.contains(newRtx.getParentKey()))
                continue;

              buf.append("  delete nodes sdb:select-item($doc");
              buf.append(", ");
              buf.append(diffTuple.getOldNodeKey());
              buf.append(")");

              if (i + 1 < length)
                buf.append(",");

              buf.append(System.getProperty("line.separator"));
              break;
            case REPLACEDNEW:
              if (newRtx.getKind() == NodeKind.ATTRIBUTE && nodeKeysOfInserts.contains(newRtx.getParentKey()))
                continue;

              buf.append("  replace node sdb:select-item($doc");
              buf.append(", ");
              buf.append(diffTuple.getOldNodeKey());
              buf.append(") with ");
              buf.append(printSubtreeNode(newRtx));

              if (i + 1 < length)
                buf.append(",");

              buf.append(System.getProperty("line.separator"));
              break;
            case UPDATED:
              if (oldRtx.isText() || oldRtx.isComment()) {
                replaceValue(newRtx, diffTuple);
              } else if (oldRtx.isAttribute()) {
                final boolean isNameEqual = oldRtx.getName().equals(newRtx.getName());
                if (!isNameEqual) {
                  renameNode(newRtx, diffTuple);
                }
                if (!Objects.equal(oldRtx.getValue(), newRtx.getValue())) {
                  if (!isNameEqual) {
                    buf.append(",");
                    buf.append(System.getProperty("line.separator"));
                  }
                  replaceValue(newRtx, diffTuple);
                }
              } else {
                renameNode(newRtx, diffTuple);
              }

              if (i + 1 < length)
                buf.append(",");

              buf.append(System.getProperty("line.separator"));
              // $CASES-OMITTED$
            default:
              // Do nothing.
          }
        }
      }
    }

    buf.append(")");
    buf.append(System.getProperty("line.separator"));

    return new Str(buf.toString());
  }

  private void replaceValue(final XmlNodeReadOnlyTrx newRtx, final DiffTuple diffTuple) {
    buf.append("  replace value of node sdb:select-item($doc");
    buf.append(", ");
    buf.append(diffTuple.getOldNodeKey());
    buf.append(") with ");
    buf.append("\"");
    buf.append(newRtx.getValue());
    buf.append("\"");
  }

  private void renameNode(final XmlNodeReadOnlyTrx newRtx, final DiffTuple diffTuple) {
    buf.append("  rename node sdb:select-item($doc");
    buf.append(", ");
    buf.append(diffTuple.getOldNodeKey());
    buf.append(") as \"");
    buf.append(Utils.buildName(newRtx.getName()));
    buf.append("\"");
  }

  private void buildUpdateStatement(final XmlNodeReadOnlyTrx rtx) {
    if (rtx.hasLeftSibling()) {
      buf.append(" before sdb:select-item($doc");
    } else {
      rtx.moveToParent();
      buf.append(" as first into sdb:select-item($doc");
    }

    buf.append(", ");
    buf.append(rtx.getNodeKey());
    buf.append(")");
  }

  private void createDocString(final Sequence[] args, final int rev1) {
    buf.append("xml:doc('");
    buf.append(((Str) args[0]).stringValue());
    buf.append("','");
    buf.append(((Str) args[1]).stringValue());
    buf.append("', ");
    buf.append(rev1);
    buf.append(")");
  }

  @Override
  public void diffListener(final @NonNull DiffType diffType, final long newNodeKey, final long oldNodeKey,
      final @NonNull DiffDepth depth) {
    diffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {
    latch.countDown();
  }

  private static String printSubtreeNode(final XmlNodeReadOnlyTrx rtx) {
    switch (rtx.getKind()) {
      case ELEMENT -> {
        final OutputStream out = new ByteArrayOutputStream();
        final XmlSerializer serializer =
            XmlSerializer.newBuilder(rtx.getResourceSession(), out).startNodeKey(rtx.getNodeKey()).build();
        serializer.call();
        return out.toString();
      }
      case ATTRIBUTE -> {
        return "attribute " + rtx.getName() + " { \"" + rtx.getValue() + "\" }";
      }
      // $CASES-OMITTED$
      default -> {
        return "\"" + rtx.getValue() + "\"";
      }
    }
  }

  private static String printNode(final XmlNodeReadOnlyTrx rtx) {
    return switch (rtx.getKind()) {
      case ELEMENT -> "<" + rtx.getName() + "/>";
      case ATTRIBUTE -> "attribute " + rtx.getName() + " { \"" + rtx.getValue() + "\" }";
      // $CASES-OMITTED$
      default -> "\"" + rtx.getValue() + "\"";
    };
  }
}
