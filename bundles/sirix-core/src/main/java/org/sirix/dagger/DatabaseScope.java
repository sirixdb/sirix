package org.sirix.dagger;

import javax.inject.Scope;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The dagger scope declaration for all the instances that should be exist only once in the context of a database
 * session.
 *
 * @author Joao Sousa
 */
@Scope
@Documented
@Retention(RUNTIME)
public @interface DatabaseScope {
}
