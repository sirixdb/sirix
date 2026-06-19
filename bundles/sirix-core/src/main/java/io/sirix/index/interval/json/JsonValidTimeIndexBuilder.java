/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.json.ObjectNamedObjectNode;

/**
 * Full-scan builder for a valid-time interval index.
 *
 * <p>Driven by {@code IndexBuilder.build}'s descendant walk, exactly like the CAS index builder.
 * Each record OBJECT (a plain {@link ImmutableObjectNode}, or the fused
 * {@link ObjectNamedObjectNode} that plays both the object-key and object role for a nested record)
 * is visited; {@link ValidTimeIntervalIndexWriter#indexObjectAtCursor} reads its
 * {@code validFrom}/{@code validTo} fields, maps them to a domain interval, and registers it in the
 * Relational-Interval-Tree. Objects without a registrable interval are skipped.</p>
 *
 * <p>The visitor reads the object's field children via the shared rtx (saving/restoring the cursor
 * inside the writer), so the descendant axis driving the build is undisturbed.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonValidTimeIndexBuilder extends AbstractJsonNodeVisitor {

  private final ValidTimeIntervalIndexWriter indexWriter;
  private final JsonNodeReadOnlyTrx rtx;

  public JsonValidTimeIndexBuilder(final ValidTimeIntervalIndexWriter indexWriter,
      final JsonNodeReadOnlyTrx rtx) {
    this.indexWriter = indexWriter;
    this.rtx = rtx;
  }

  @Override
  public VisitResult visit(final ImmutableObjectNode node) {
    rtx.moveTo(node.getNodeKey());
    indexWriter.indexObjectAtCursor(rtx);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ObjectNamedObjectNode node) {
    // Fused named-object record (a nested object field whose value is itself a record OBJECT).
    rtx.moveTo(node.getNodeKey());
    indexWriter.indexObjectAtCursor(rtx);
    return VisitResultType.CONTINUE;
  }
}
