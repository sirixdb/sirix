/**
 * Copyright (c) 2020, SirixDB
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
package org.sirix.xquery.function.jn.diff;

import com.google.api.client.util.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.diff.DiffObserver;
import org.sirix.diff.DiffTuple;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBItem;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    "$coll, $res, $rev1, $rev2" }) public final class Diff extends AbstractFunction implements DiffObserver {

    /**
     * Sort by document order name.
     */
    public final static QNm DIFF = new QNm(JNFun.JN_NSURI, JNFun.JN_PREFIX, "diff");

    private final List<DiffTuple> mDiffs;

    /**
     * Constructor.
     *
     * @param name      the name of the function
     * @param signature the signature of the function
     */
    public Diff(final QNm name, final Signature signature) {
        super(name, signature, true);

        mDiffs = new ArrayList<>();
    }

    @Override public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
        if (args.length != 4) {
            throw new QueryException(new QNm("No valid arguments specified!"));
        }

        final JsonDBCollection col = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

        if (col == null) {
            throw new QueryException(new QNm("No valid arguments specified!"));
        }

        final String expResName = ((Str) args[1]).stringValue();
        final int oldRevision = FunUtil.getInt(args, 2, "revision1", -1, null, false);
        final int newRevision = FunUtil.getInt(args, 3, "revision2", -1, null, false);
        final JsonDBItem doc = col.getDocument(expResName);

        mDiffs.clear();

        try (final JsonResourceManager resourceManager = doc.getTrx().getResourceManager()) {
            DiffFactory.invokeJsonDiff(new DiffFactory.Builder<>(resourceManager, newRevision, oldRevision,
                resourceManager.getResourceConfig().hashType == HashType.NONE ? DiffOptimized.NO : DiffOptimized.HASHED,
                ImmutableSet.of(this)).skipSubtrees(true));

            final var json = createMetaInfo(args, oldRevision, newRevision);

            if (mDiffs.size() == 1 && (mDiffs.get(0).getDiff() == DiffType.SAMEHASH || mDiffs.get(0).getDiff()
                == DiffType.SAME)) {
                return new Str(json.toString());
            }

            final var jsonDiffs = json.getAsJsonArray("diffs");

            try (final JsonNodeReadOnlyTrx oldRtx = resourceManager.beginNodeReadOnlyTrx(oldRevision);
                final JsonNodeReadOnlyTrx newRtx = resourceManager.beginNodeReadOnlyTrx(newRevision)) {

                final Iterator<DiffTuple> iter = mDiffs.iterator();
                while (iter.hasNext()) {
                    final DiffTuple tuple = iter.next();

                    final DiffType diffType = tuple.getDiff();

                    if (diffType == DiffType.SAME || diffType == DiffType.SAMEHASH
                        || diffType == DiffType.REPLACEDOLD) {
                        iter.remove();
                    } else if (diffType == DiffType.INSERTED || diffType == DiffType.REPLACEDNEW) {
                        newRtx.moveTo(tuple.getNewNodeKey());
                    }

                    if (diffType == DiffType.DELETED) {
                        oldRtx.moveTo(tuple.getOldNodeKey());
                    }
                }

                if (mDiffs.isEmpty())
                    return new Str(json.toString());

                for (final DiffTuple diffTuple : mDiffs) {
                    final DiffType diffType = diffTuple.getDiff();
                    newRtx.moveTo(diffTuple.getNewNodeKey());
                    oldRtx.moveTo(diffTuple.getOldNodeKey());

                    switch (diffType) {
                        case INSERTED:
                            final var insertedJson = new JsonObject();
                            final var jsonInsertDiff = new JsonObject();

                            final var insertPosition = newRtx.hasLeftSibling() ? "asRightSibling" : "asFirstChild";

                            jsonInsertDiff.addProperty("nodeKey", newRtx.hasLeftSibling() ? newRtx.getLeftSiblingKey() : newRtx.getParentKey());
                            jsonInsertDiff.addProperty("insertPosition", insertPosition);

                            serialize(newRevision, resourceManager, newRtx, jsonInsertDiff);

                            insertedJson.add("insert", jsonInsertDiff);
                            jsonDiffs.add(insertedJson);

                            break;
                        case DELETED:
                            final var deletedJson = new JsonObject();

                            deletedJson.addProperty("delete", diffTuple.getOldNodeKey());

                            jsonDiffs.add(deletedJson);
                            break;
                        case REPLACEDNEW:
                            final var replaceJson = new JsonObject();
                            final var jsonReplaceDiff = new JsonObject();

                            replaceJson.add("replace", jsonReplaceDiff);

                            jsonReplaceDiff.addProperty("oldNodeKey", diffTuple.getOldNodeKey());

                            serialize(newRevision, resourceManager, newRtx, jsonReplaceDiff);

                            jsonDiffs.add(replaceJson);
                            break;
                        case UPDATED:
                            final var updateJson = new JsonObject();
                            final var jsonUpdateDiff = new JsonObject();

                            jsonUpdateDiff.addProperty("nodeKey", diffTuple.getOldNodeKey());

                            if (!Objects.equal(oldRtx.getName(), newRtx.getName())) {
                                jsonUpdateDiff.addProperty("name", newRtx.getName().toString());
                            } else if (!Objects.equal(oldRtx.getValue(), newRtx.getValue())) {
                                jsonUpdateDiff.addProperty("value", newRtx.getValue());
                            }

                            updateJson.add("update", jsonUpdateDiff);
                            jsonDiffs.add(updateJson);

                            // $CASES-OMITTED$
                        default:
                            // Do nothing.
                    }
                }
            }

            return new Str(json.toString());
        }
    }

    private void serialize(int newRevision, JsonResourceManager resourceManager, JsonNodeReadOnlyTrx newRtx,
        JsonObject jsonReplaceDiff) {
        try (final var writer = new StringWriter()) {
            final var serializer = JsonSerializer.newBuilder(resourceManager, writer, newRevision).startNodeKey(newRtx.getNodeKey()).build();
            serializer.call();
            jsonReplaceDiff.addProperty("data", writer.toString());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JsonObject createMetaInfo(final Sequence[] args, final int oldRevision, final int newRevision) {
        final var json = new JsonObject();
        json.addProperty("database", ((Str) args[0]).stringValue());
        json.addProperty("resource", ((Str) args[1]).stringValue());
        json.addProperty("old-revision", oldRevision);
        json.addProperty("new-revision", newRevision);
        final var diffsArray = new JsonArray();
        json.add("diffs", diffsArray);
        return json;
    }

    @Override public void diffListener(@Nonnull final DiffType diffType, final long newNodeKey, final long oldNodeKey,
        @Nonnull final DiffDepth depth) {
        mDiffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
    }

    @Override public void diffDone() {
    }
}
