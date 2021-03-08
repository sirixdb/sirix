package org.sirix.dagger;

import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A Qualifier for the resource name.
 *
 * @author Joao Sousa
 */
@Qualifier
@Documented
@Retention(RUNTIME)
public @interface ResourceName {
}
