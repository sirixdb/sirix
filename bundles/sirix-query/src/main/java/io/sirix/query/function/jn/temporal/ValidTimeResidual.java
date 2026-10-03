package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.DateTime;
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
  private final DateTime point;
  private final QNm field;
  private final boolean start;
  private final boolean strict;

  ValidTimeResidual(final StaticContext context, final DateTime point, final String field, final boolean start,
      final boolean strict) {
    this.context = context;
    this.point = point;
    this.field = new QNm(field);
    this.start = start;
    this.strict = strict;
  }

  @Override
  public boolean test(final JsonDBObject object) {
    final Item value = ExprUtil.asItem(object.get(field));
    if (value == null) {
      return false;
    }
    final DateTime bound = (DateTime) Cast.cast(context, value, Type.DATI, true);
    final int comparison = start
        ? bound.cmp(point)
        : point.cmp(bound);
    return strict
        ? comparison < 0
        : comparison <= 0;
  }
}
