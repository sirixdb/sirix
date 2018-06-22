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
package org.sirix.xquery.function.sdb.diff;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.trx.node.HashKind;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.diff.DiffObserver;
import org.sirix.diff.DiffTuple;
import org.sirix.node.Kind;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;
import com.google.common.collect.ImmutableSet;

/**
 * <p>
 * Function for diffing two revisions of a resource in a collection/database. The Supported
 * signature is:
 * </p>
 *
 * <pre>
 * <code>sdb:store($coll as xs:string, $res as xs:string, $rev1 as xs:int, $rev2 as xs:int) as xs:string</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionAnnotation(description = "Diffing of two versions of a resource.",
    parameters = {"$coll, $res, $rev1, $rev2"})
public final class Diff extends AbstractFunction implements DiffObserver {

  /** Sort by document order name. */
  public final static QNm DIFF = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "diff");

  private final StringBuilder mBuf;

  private final List<DiffTuple> mDiffs;

  private final ExecutorService mPool;

  private CountDownLatch mLatch;

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Diff(final QNm name, final Signature signature) {
    super(name, signature, true);

    mBuf = new StringBuilder();
    mDiffs = new ArrayList<>();
    mPool = Executors.newSingleThreadExecutor();
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    if (args.length != 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final DBCollection col = (DBCollection) ctx.getStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String expResName = ((Str) args[1]).stringValue();
    final int rev1 = FunUtil.getInt(args, 2, "revision1", -1, null, false);
    final int rev2 = FunUtil.getInt(args, 3, "revision2", -1, null, false);
    final DBNode doc = col.getDocument(expResName);

    mDiffs.clear();
    mBuf.setLength(0);
    mLatch = new CountDownLatch(1);

    try (final ResourceManager resMrg = doc.getTrx().getResourceManager()) {
      mPool.submit(
          () -> DiffFactory.invokeFullDiff(
              new DiffFactory.Builder(resMrg, rev2, rev1,
                  resMrg.getResourceConfig().mHashKind == HashKind.NONE
                      ? DiffOptimized.NO
                      : DiffOptimized.HASHED,
                  ImmutableSet.of(this)).skipSubtrees(true)));

      try {
        mLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new QueryException(new QNm("Interrupted exception"), e);
      }

      final Set<Long> nodeKeysOfInserts = mDiffs.stream()
                                                .filter(
                                                    tuple -> tuple.getDiff() == DiffType.INSERTED
                                                        || tuple.getDiff() == DiffType.REPLACEDNEW)
                                                .map(DiffTuple::getNewNodeKey)
                                                .collect(Collectors.toSet());

      mBuf.append("let $doc := ");
      createDocString(args, rev1);
      mBuf.append(System.getProperty("line.separator"));
      mBuf.append("return (");
      mBuf.append(System.getProperty("line.separator"));

      try (final XdmNodeReadTrx oldRtx = resMrg.beginNodeReadTrx(rev1);
          final XdmNodeReadTrx newRtx = resMrg.beginNodeReadTrx(rev2)) {
        // Plain old for-loop as Java is still missing an indexed forEach(...) loop (on a
        // collection).
        for (int i = 0, length = mDiffs.size(); i < length; i++) {
          final DiffTuple diffTuple = mDiffs.get(i);
          final DiffType diffType = diffTuple.getDiff();
          newRtx.moveTo(diffTuple.getNewNodeKey());
          oldRtx.moveTo(diffTuple.getOldNodeKey());

          final Optional<DiffTuple> anotherTupleToEmit;

          switch (diffType) {
            case INSERTED:
              if (newRtx.getKind() == Kind.ATTRIBUTE
                  && nodeKeysOfInserts.contains(newRtx.getParentKey()))
                continue;

              mBuf.append("  insert nodes ");
              mBuf.append(printSubtreeNode(newRtx));

              if (oldRtx.hasLeftSibling()) {
                mBuf.append(" before into sdb:select-node($doc");
              } else {
                oldRtx.moveToParent();
                mBuf.append(" as first into sdb:select-node($doc");
              }

              mBuf.append(", ");
              mBuf.append(oldRtx.getNodeKey());
              mBuf.append(")");

              anotherTupleToEmit =
                  determineIfAnotherTupleToEmitExists(i + 1, nodeKeysOfInserts, newRtx);

              if (anotherTupleToEmit.isPresent())
                mBuf.append(",");

              mBuf.append(System.getProperty("line.separator"));
              break;
            case DELETED:
              mBuf.append("  delete nodes sdb:select-node($doc");
              mBuf.append(", ");
              mBuf.append(diffTuple.getOldNodeKey());
              mBuf.append(")");
              if (i != length - 1)
                mBuf.append(",");
              mBuf.append(System.getProperty("line.separator"));
              break;
            case REPLACEDNEW:
              if (newRtx.getKind() == Kind.ATTRIBUTE
                  && nodeKeysOfInserts.contains(newRtx.getParentKey()))
                continue;

              mBuf.append("  replace node sdb:select-node($doc");
              mBuf.append(", ");
              mBuf.append(diffTuple.getOldNodeKey());
              mBuf.append(") with ");
              mBuf.append(printSubtreeNode(newRtx));

              anotherTupleToEmit =
                  determineIfAnotherTupleToEmitExists(i + 1, nodeKeysOfInserts, newRtx);

              if (anotherTupleToEmit.isPresent())
                mBuf.append(",");

              mBuf.append(System.getProperty("line.separator"));
              break;
            case UPDATED:
              if (oldRtx.isText())
                mBuf.append("  replace node sdb:select-node($doc");
              else
                mBuf.append("  rename node sdb:select-node($doc");
              mBuf.append(", ");
              mBuf.append(diffTuple.getOldNodeKey());
              if (oldRtx.isText())
                mBuf.append(") with ");
              else
                mBuf.append(") as ");
              mBuf.append(printNode(newRtx));
              if (i != length - 1)
                mBuf.append(",");
              mBuf.append(System.getProperty("line.separator"));
              // $CASES-OMITTED$
            default:
              // Do nothing.
          }
        }
      }
    }

    mBuf.append(")");
    mBuf.append(System.getProperty("line.separator"));

    return new Str(mBuf.toString());
  }

  private Optional<DiffTuple> determineIfAnotherTupleToEmitExists(int i,
      final Set<Long> nodeKeysOfInserts, final XdmNodeReadTrx newRtx) {
    final Predicate<DiffTuple> filter = diffTuplePredicate(nodeKeysOfInserts, newRtx);

    final Optional<DiffTuple> anotherTupleToEmit =
        mDiffs.subList(i, mDiffs.size()).stream().filter(filter).findFirst();
    return anotherTupleToEmit;
  }

  private Predicate<DiffTuple> diffTuplePredicate(final Set<Long> nodeKeysOfInserts,
      final XdmNodeReadTrx newRtx) {
    final Predicate<DiffTuple> filter = tuple -> {
      if ((tuple.getDiff() == DiffType.INSERTED || tuple.getDiff() == DiffType.REPLACEDNEW)
          && newRtx.moveTo(tuple.getNewNodeKey()).hasMoved() && newRtx.getKind() == Kind.ATTRIBUTE
          && nodeKeysOfInserts.contains(newRtx.getParentKey())) {
        return false;
      } else {
        return true;
      }
    };

    return filter;
  }

  private void createDocString(final Sequence[] args, final int rev1) {
    mBuf.append("sdb:doc('");
    mBuf.append(((Str) args[0]).stringValue());
    mBuf.append("','");
    mBuf.append(((Str) args[1]).stringValue());
    mBuf.append("', ");
    mBuf.append(rev1);
    mBuf.append(")");
  }


  @Override
  public void diffListener(final DiffType diffType, final long newNodeKey, final long oldNodeKey,
      final DiffDepth depth) {
    mDiffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {
    mLatch.countDown();
  }

  private static String printSubtreeNode(final XdmNodeReadTrx rtx) {
    switch (rtx.getKind()) {
      case ELEMENT:
        final OutputStream out = new ByteArrayOutputStream();
        final XMLSerializer serializer = XMLSerializer.newBuilder(rtx.getResourceManager(), out)
                                                      .startNodeKey(rtx.getNodeKey())
                                                      .build();
        serializer.call();
        return out.toString();
      case ATTRIBUTE:
        return "attribute { '" + rtx.getName() + "' } { " + rtx.getValue() + " }";
      // $CASES-OMITTED$
      default:
        return "\"" + rtx.getValue() + "\"";
    }
  }

  private static String printNode(final XdmNodeReadTrx rtx) {
    switch (rtx.getKind()) {
      case ELEMENT:
        return "<" + rtx.getName() + "/>";
      case ATTRIBUTE:
        return "attribute { '" + rtx.getName() + "' } { " + rtx.getValue() + " }";
      // $CASES-OMITTED$
      default:
        return "\"" + rtx.getValue() + "\"";
    }
  }
}
