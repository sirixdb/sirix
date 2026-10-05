package io.sirix.query.stream.node;

import io.sirix.utils.ToStringHelper;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.AbstractTemporalNode;
import io.brackit.query.jdm.type.NodeType;
import io.brackit.query.jdm.type.AttributeType;
import io.brackit.query.jdm.type.CommentType;
import io.brackit.query.jdm.type.DocumentType;
import io.brackit.query.jdm.type.ElementType;
import io.brackit.query.jdm.type.TextType;
import io.sirix.axis.filter.xml.AttributeFilter;
import io.sirix.axis.filter.xml.CommentFilter;
import io.sirix.axis.filter.xml.DocumentRootNodeFilter;
import io.sirix.axis.filter.xml.ElementFilter;
import io.sirix.axis.filter.xml.TemporalXmlNodeReadFilterAxis;
import io.sirix.axis.filter.xml.TextFilter;
import io.sirix.axis.filter.xml.XmlNameFilter;
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

  public static TemporalSirixNodeStream create(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final XmlNodeReadOnlyTrx trx, final XmlDBCollection collection, final NodeType test) {
    requireNonNull(test);
    return isSimpleNodeTest(test)
        ? new TemporalSirixNodeStream(getTemporalAxis(test, trx, axis), collection)
        : new TemporalSirixNodeStream(axis, collection, test);
  }

  public static boolean isSimpleNodeTest(final NodeType test) {
    // Partial wildcards, type restrictions and nested document tests require NodeType.matches.
    return test.getType() == null && (test instanceof ElementType || test instanceof AttributeType
        || test instanceof TextType || test instanceof CommentType
        || (test instanceof DocumentType documentType && documentType.getElementType() == null));
  }

  private static AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> getTemporalAxis(final NodeType test,
      final XmlNodeReadOnlyTrx trx, final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> innerAxis) {
    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis;

    switch (test.getNodeKind()) {
      case COMMENT -> axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new CommentFilter(trx));
      case ELEMENT -> {
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new ElementFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new ElementFilter(trx),
              new XmlNameFilter(trx, test.getQName()));
        }
      }
      case TEXT -> axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new TextFilter(trx));
      case ATTRIBUTE -> {
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new AttributeFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new AttributeFilter(trx),
              new XmlNameFilter(trx, test.getQName()));
        }
      }
      case DOCUMENT -> {
        return new TemporalXmlNodeReadFilterAxis<>(innerAxis, new DocumentRootNodeFilter(trx));
      }
      default -> throw new AssertionError(); // Must not happen.
    }

    return axis;
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
