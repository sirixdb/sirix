package io.sirix.query.stream.node;

import io.sirix.utils.ToStringHelper;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.AbstractTemporalNode;
import io.brackit.query.jdm.type.NodeType;
import io.sirix.api.Axis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;

import static java.util.Objects.requireNonNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public class TemporalSirixNodeStream implements Stream<AbstractTemporalNode<XmlDBNode>> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis;

  /** The {@link XmlDBCollection} reference. */
  private final XmlDBCollection collection;

  /** Optional node test whose rejected readers remain owned by this stream. */
  private final NodeType test;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public TemporalSirixNodeStream(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final XmlDBCollection collection) {
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
    test = null;
  }

  /**
   * Wrap a temporal axis with a complete node test. Accepted readers belong to the consumer; rejected
   * readers are closed here, including when matching throws.
   *
   * @param axis temporal axis
   * @param collection collection the nodes belong to
   * @param test complete node test
   */
  public TemporalSirixNodeStream(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final XmlDBCollection collection, final NodeType test) {
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
    this.test = requireNonNull(test);
  }

  @Override
  public AbstractTemporalNode<XmlDBNode> next() throws DocumentException {
    while (axis.hasNext()) {
      final var rtx = axis.next();
      boolean accepted = false;
      try {
        final XmlDBNode node = new XmlDBNode(rtx, collection);
        if (test == null || test.matches(node)) {
          accepted = true;
          return node;
        }
      } finally {
        if (!accepted) {
          rtx.close();
        }
      }
    }

    return null;
  }

  @Override
  public void close() {
    axis.close();
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("axis", axis).toString();
  }
}
