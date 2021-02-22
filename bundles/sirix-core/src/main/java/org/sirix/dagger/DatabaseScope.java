package org.sirix.dagger;

import javax.inject.Scope;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * TODO: Class DatabaseScope's description.
 *
 * @author Joao Sousa
 */
@Scope
@Documented
@Retention(RUNTIME)
public @interface DatabaseScope {
}
