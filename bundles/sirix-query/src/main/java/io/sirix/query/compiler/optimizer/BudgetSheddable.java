package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.optimizer.Stage;

/**
 * Marks an optimizer {@link Stage} the {@link SirixOptimizer} budget may skip for a query with more
 * join relations than {@code MAX_JOIN_RELATIONS_FOR_REORDER}. These are the EXPLORATORY join-reorder
 * and mesh stages — the DPhyp join reordering is the one stage whose cost grows unboundedly with
 * query size, and the mesh stages only re-derive and apply its join-order choices.
 *
 * <p>Crucially, shedding these stages can only change JOIN ORDER, never which INDEXES a query uses:
 * the index decision ({@code PREFER_INDEX}) is authored solely by {@link CostBasedStage}, which is
 * NOT sheddable and always runs; the mesh stages re-derive that same decision from the costs
 * {@code CostBasedStage} already computed and never contradict it. So marking a stage sheddable is
 * safe for index/plan DETERMINISM exactly when its omission leaves index selection unchanged.</p>
 *
 * <p>Stages that author or apply the index decision — {@code CostBasedStage}, cost-driven routing,
 * and index matching — must therefore NOT implement this. The budget is chosen from the query shape
 * (join-relation count), so it is identical on every machine: unlike the former wall-clock cutoff it
 * can never drop a stage, and thus change a query's plan, based only on how fast the runner was.</p>
 *
 * <p>Self-classification (rather than a central class allowlist in {@code SirixOptimizer}) is
 * deliberate: stages are also injected through the {@code addStageFirst}/{@code addStageAt}/
 * {@code addStageBeforeIndexMatching} seams (e.g. by enterprise subclasses). A new expensive stage
 * declares its own sheddability at its definition site; anything the budget has never heard of is
 * conservatively treated as mandatory, so an un-marked injected stage is never silently forced to
 * run past the budget by omission from a list someone forgot to update.</p>
 */
public interface BudgetSheddable extends Stage {
}
