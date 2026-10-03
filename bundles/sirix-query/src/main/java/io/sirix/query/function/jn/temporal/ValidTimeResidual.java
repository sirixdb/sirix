package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Bool;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.Cmp;
import io.brackit.query.atomic.QNm;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.ExprUtil;
import io.sirix.query.json.JsonDBObject;

import java.util.function.Predicate;

/** The original dateTime comparison, retained for intervals the index cannot prove exactly. */
final class ValidTimeResidual implements Predicate<JsonDBObject> {
  private final StaticContext context;
  private final QueryContext queryContext;
  private final Sequence point;
  private final QNm field;
  private final Cmp comparison;
  private final boolean general;
  private final boolean fieldOnLeft;

  ValidTimeResidual(final StaticContext context, final QueryContext queryContext, final Sequence point,
      final String field, final boolean start, final boolean strict, final boolean general, final boolean fieldOnLeft) {
    this.context = context;
    this.queryContext = queryContext;
    this.point = point;
    this.field = new QNm(field);
    final Cmp ordered = strict ? Cmp.lt : Cmp.le;
    comparison = start == fieldOnLeft ? ordered : ordered.swap();
    this.general = general;
    this.fieldOnLeft = fieldOnLeft;
  }

  @Override
  public boolean test(final JsonDBObject object) {
    final Bool result;
    if (general) {
      final Item bound = bound(object);
      result = fieldOnLeft ? comparison.gCmpAsBool(queryContext, bound, point)
          : comparison.gCmpAsBool(queryContext, point, bound);
    } else if (fieldOnLeft) {
      final Item bound = bound(object);
      result = comparison.vCmpAsBool(queryContext, bound, ExprUtil.asItem(point));
    } else {
      final Item value = ExprUtil.asItem(point);
      result = comparison.vCmpAsBool(queryContext, value, bound(object));
    }
    return result != null && result.booleanValue();
  }

  private Item bound(final JsonDBObject object) {
    final Item value = ExprUtil.asItem(object.get(field));
    return value == null ? null : Cast.cast(context, value, Type.DATI, true);
  }
}
