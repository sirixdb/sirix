package org.sirix.dagger;

import org.sirix.api.ResourceManager;

import javax.inject.Scope;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The scope declaration for all the instances that should exist only once in the context of a {@link ResourceManager}.
 *
 * @author Joao Sousa
 */
@Scope
@Documented
@Retention(RUNTIME)
public @interface ResourceManagerScope {
}
