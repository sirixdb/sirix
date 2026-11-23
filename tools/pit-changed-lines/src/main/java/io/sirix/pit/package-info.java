/**
 * PIT plugins for the SirixDB PR mutation lane: a mutant restriction to the lines a pull request
 * changed, and a result listener that writes every analysed class as it finishes.
 *
 * <p>
 * CI-only tooling. The rules are CONV-202 and CONV-205 in {@code spec/CONV-conventions.md}; the
 * lane that activates them is described in {@code docs/VERIFICATION.md}.
 */
@NullMarked
package io.sirix.pit;

import org.jspecify.annotations.NullMarked;
