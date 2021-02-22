package org.sirix.dagger;

import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * TODO: Class DatabaseName's description.
 *
 * @author Joao Sousa
 */
@Qualifier
@Documented
@Retention(RUNTIME)
public @interface DatabaseName {
}
