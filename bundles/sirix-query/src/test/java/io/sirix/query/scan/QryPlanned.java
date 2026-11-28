/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

/**
 * Marks a spec-derived test of the QRY chapter whose behavior is not implemented yet. The chapter
 * labels each such test <em>Planned</em> (CONV-102: a planned test MUST be labelled planned); this
 * annotation is that label in code, and it keeps CI green until the named step lands: the test is
 * disabled with a reason naming the requirement and the step, and {@link QrySteps} enables it.
 *
 * <p>
 * The repository has no {@code @Verifies} annotation yet (CONV-104 lists it as planned work), so
 * the requirement IDs of this annotation, of the class Javadoc and of the {@code @DisplayName}s are
 * the traceability link.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@ExtendWith(QryPlanned.Condition.class)
public @interface QryPlanned {

  /** The QRY step ({@link QrySteps#BRACKIT_DETECTION} or {@link QrySteps#ROUTE}) that enables it. */
  int step();

  /** The requirement IDs the test verifies, e.g. {@code "QRY-202"}. */
  String[] value();

  /** Disables the annotated container or method until its step has landed. */
  final class Condition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
      final Optional<QryPlanned> planned =
          context.getElement().flatMap(element -> findAnnotation(element, QryPlanned.class));
      if (planned.isEmpty()) {
        return ConditionEvaluationResult.enabled("not a planned QRY test");
      }
      final QryPlanned annotation = planned.get();
      if (QrySteps.enabled(annotation.step())) {
        return ConditionEvaluationResult.enabled(
            String.join(", ", annotation.value()) + " enabled (step " + annotation.step() + ")");
      }
      return ConditionEvaluationResult.disabled(QrySteps.disabledReason(annotation.step(), annotation.value()));
    }
  }
}
