package io.sirix.index.aggregate;

import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.Indexes;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexMetadata;
import org.jspecify.annotations.Nullable;
import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Public declaration API shared by query DDL and data loaders. */
public final class TemporalAggregateDeclarations {
  private TemporalAggregateDeclarations() {}

  public static IndexDef declare(final JsonIndexController controller, final JsonNodeTrx transaction, final String root,
      final List<String> groups, final List<String> aggregates) {
    Objects.requireNonNull(controller);
    Objects.requireNonNull(transaction);
    final Indexes indexes = controller.getIndexes();
    final TemporalAggregateSpec spec = resolve(indexes,
        transaction.getResourceSession().getResourceConfig().getValidTimeConfig(), root, groups, aggregates);
    final StorageEngineWriter writer = transaction.getStorageEngineWriter();
    ProjectionIndexMetadata.requireReady(writer, spec.projectionId());
    final IndexDef existing = find(indexes, spec);
    if (existing != null) {
      return existing;
    }
    final IndexDef projection = indexes.getIndexDef(spec.projectionId(), IndexType.PROJECTION);
    final int id = writer.getTemporalAggregateIndexPage(writer.getActualRevisionRootPage()).nextUnallocatedIndex();
    final IndexDef definition = IndexDefs.createTemporalAggregateIdxDef(projection.getProjectionRootPath(), spec, id);
    controller.createIndexes(Set.of(definition), transaction);
    return definition;
  }

  public static @Nullable IndexDef find(final Indexes indexes, final TemporalAggregateSpec spec) {
    Objects.requireNonNull(indexes);
    Objects.requireNonNull(spec);
    for (final IndexDef existing : indexes.getIndexDefs()) {
      if (existing.isTemporalAggregateIndex() && existing.getTemporalAggregateSpec().equals(spec)) {
        return existing;
      }
    }
    return null;
  }

  public static TemporalAggregateSpec resolve(final Indexes indexes, final @Nullable ValidTimeConfig config,
      final String root, final List<String> groups, final List<String> aggregates) {
    Objects.requireNonNull(indexes);
    Objects.requireNonNull(root);
    Objects.requireNonNull(groups);
    Objects.requireNonNull(aggregates);
    for (final IndexDef projection : indexes.getIndexDefs()) {
      if (!projection.isProjectionIndex() || !projection.getProjectionRootPath().toString().equals(root)) {
        continue;
      }
      final List<Integer> groupColumns = new ArrayList<>(groups.size());
      boolean covered = true;
      for (final String group : groups) {
        final int column = column(projection, Objects.requireNonNull(group));
        groupColumns.add(column);
        covered &= column >= 0;
      }
      final List<Lane> lanes = new ArrayList<>();
      for (final String aggregate : aggregates) {
        final String expression = Objects.requireNonNull(aggregate).trim();
        if (expression.equals("count(*)")) {
          continue;
        }
        final int open = expression.indexOf('(');
        if (open < 0 || !expression.endsWith(")")
            || !Set.of("count", "sum", "min", "max").contains(expression.substring(0, open))) {
          throw new IllegalArgumentException("Unsupported temporal aggregate declaration: " + expression);
        }
        final String[] fields = expression.substring(open + 1, expression.length() - 1).split("\\*", -1);
        if (fields.length < 1 || fields.length > 2) {
          throw new IllegalArgumentException("An aggregate operand must be a field or product of two fields");
        }
        final int left = column(projection, fields[0].trim());
        final int right = fields.length == 1
            ? -1
            : column(projection, fields[1].trim());
        if (left < 0 || (fields.length == 2 && right < 0)) {
          covered = false;
        } else {
          final Lane requested = new Lane(left, right, TemporalAggregateSpec.operation(expression.substring(0, open)));
          int existing = -1;
          for (int i = 0; i < lanes.size(); i++) {
            if (lanes.get(i).sameOperand(requested)) {
              existing = i;
              break;
            }
          }
          if (existing < 0) {
            lanes.add(requested);
          } else {
            lanes.set(existing, new Lane(requested.left(), requested.right(),
                lanes.get(existing).operations() | requested.operations()));
          }
        }
      }
      if (!covered) {
        continue;
      }
      final TemporalAggregateSpec spec =
          new TemporalAggregateSpec(projection.getID(), groupColumns, new ArrayList<>(lanes));
      TemporalAggregateIndexes.validate(
          IndexDefs.createTemporalAggregateIdxDef(projection.getProjectionRootPath(), spec, 0), projection, config);
      return spec;
    }
    throw new IllegalArgumentException("No covering projection for the temporal aggregate declaration at " + root);
  }

  private static int column(final IndexDef projection, final String field) {
    if (field.isEmpty()) {
      return -1;
    }
    final String path = projection.getProjectionRootPath() + "/" + field.replace('.', '/');
    for (int i = 0; i < projection.getProjectionFields().size(); i++) {
      if (projection.getProjectionFields().get(i).toString().equals(path)) {
        return i;
      }
    }
    return -1;
  }
}
