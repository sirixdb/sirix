package io.sirix.node.interfaces;

/**
 * Marker for transaction-local node proxy instances that may be rebound and reused by node
 * factories.
 *
 * <p>
 * Records implementing this marker must never be retained as authoritative in-memory page state.
 * Writers persist them directly to slot storage and avoid keeping object references in record
 * arrays.
 * </p>
 */
public interface ReusableNodeProxy {
}
