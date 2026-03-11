package io.sirix.query.compiler.optimizer.stats;

/**
 * Types of indexes available in SirixDB for JSON query optimization.
 */
public enum IndexType {
  /** Path index — covers structural path lookups. */
  PATH,
  /** Content-and-structure (CAS) index — covers value predicates on paths. */
  CAS,
  /** Name index — covers object key name lookups. */
  NAME,
  /** No index available. */
  NONE
}
