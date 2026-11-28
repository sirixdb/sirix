/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import net.jqwik.api.lifecycle.AddLifecycleHook;
import net.jqwik.api.lifecycle.LifecycleContext;
import net.jqwik.api.lifecycle.SkipExecutionHook;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link QryPlanned} for jqwik properties: jqwik runs on its own test engine, so the JUnit
 * extension of {@link QryPlanned} does not see it. The property is skipped, with the same reason,
 * until its step lands in {@link QrySteps}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@AddLifecycleHook(QryPlannedProperty.Hook.class)
@interface QryPlannedProperty {

  /** The QRY step whose landing enables the property. */
  int step();

  /** The requirement IDs the property verifies. */
  String[] value();

  /** Skips the annotated property until its step has landed. */
  final class Hook implements SkipExecutionHook {
    @Override
    public SkipResult shouldBeSkipped(final LifecycleContext context) {
      final QryPlannedProperty planned = context.findAnnotation(QryPlannedProperty.class).orElse(null);
      if (planned == null || QrySteps.enabled(planned.step())) {
        return SkipResult.doNotSkip();
      }
      return SkipResult.skip(QrySteps.disabledReason(planned.step(), planned.value()));
    }
  }
}
