package io.sirix.query.compiler.optimizer.mesh;

import io.brackit.query.compiler.AST;

/**
 * An alternative physical plan within an equivalence class.
 *
 * @param plan the AST subtree representing this alternative
 * @param cost the estimated cost of this alternative
 */
public record PlanAlternative(AST plan, double cost) {}
