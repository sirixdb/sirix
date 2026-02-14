package io.sirix.node.layout;

import io.sirix.node.NodeKind;

import java.util.Objects;

/**
 * Registry for fixed-slot layout contracts per {@link NodeKind}.
 */
public final class NodeKindLayouts {
  private static final NodeKind[] NODE_KINDS = NodeKind.values();
  private static final NodeKindLayout[] LAYOUTS_BY_ORDINAL = new NodeKindLayout[NODE_KINDS.length];

  static {
    for (final NodeKind kind : NODE_KINDS) {
      LAYOUTS_BY_ORDINAL[kind.ordinal()] = NodeKindLayout.unsupported(kind);
    }

    registerXmlLayouts();
    registerJsonLayouts();
  }

  private NodeKindLayouts() {
  }

  public static NodeKindLayout layoutFor(final NodeKind nodeKind) {
    return LAYOUTS_BY_ORDINAL[Objects.requireNonNull(nodeKind, "nodeKind must not be null").ordinal()];
  }

  public static boolean hasFixedSlotLayout(final NodeKind nodeKind) {
    return layoutFor(nodeKind).isFixedSlotSupported();
  }

  private static void registerXmlLayouts() {
    put(NodeKind.XML_DOCUMENT, NodeKindLayout.builder(NodeKind.XML_DOCUMENT)
                                             .addField(StructuralField.FIRST_CHILD_KEY)
                                             .addField(StructuralField.LAST_CHILD_KEY)
                                             .addField(StructuralField.CHILD_COUNT)
                                             .addField(StructuralField.DESCENDANT_COUNT)
                                             .addField(StructuralField.HASH)
                                             .build());

    put(NodeKind.ELEMENT, NodeKindLayout.builder(NodeKind.ELEMENT)
                                        .addField(StructuralField.PARENT_KEY)
                                        .addField(StructuralField.RIGHT_SIBLING_KEY)
                                        .addField(StructuralField.LEFT_SIBLING_KEY)
                                        .addField(StructuralField.FIRST_CHILD_KEY)
                                        .addField(StructuralField.LAST_CHILD_KEY)
                                        .addField(StructuralField.PATH_NODE_KEY)
                                        .addField(StructuralField.PREFIX_KEY)
                                        .addField(StructuralField.LOCAL_NAME_KEY)
                                        .addField(StructuralField.URI_KEY)
                                        .addField(StructuralField.PREVIOUS_REVISION)
                                        .addField(StructuralField.LAST_MODIFIED_REVISION)
                                        .addField(StructuralField.HASH)
                                        .addField(StructuralField.CHILD_COUNT)
                                        .addField(StructuralField.DESCENDANT_COUNT)
                                        .addPayloadRef("attributes", PayloadRefKind.ATTRIBUTE_VECTOR)
                                        .addPayloadRef("namespaces", PayloadRefKind.NAMESPACE_VECTOR)
                                        .build());

    put(NodeKind.ATTRIBUTE, NodeKindLayout.builder(NodeKind.ATTRIBUTE)
                                          .addField(StructuralField.PARENT_KEY)
                                          .addField(StructuralField.PATH_NODE_KEY)
                                          .addField(StructuralField.PREFIX_KEY)
                                          .addField(StructuralField.LOCAL_NAME_KEY)
                                          .addField(StructuralField.URI_KEY)
                                          .addField(StructuralField.PREVIOUS_REVISION)
                                          .addField(StructuralField.LAST_MODIFIED_REVISION)
                                          .addField(StructuralField.HASH)
                                          .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                          .build());

    put(NodeKind.NAMESPACE, NodeKindLayout.builder(NodeKind.NAMESPACE)
                                          .addField(StructuralField.PARENT_KEY)
                                          .addField(StructuralField.PATH_NODE_KEY)
                                          .addField(StructuralField.PREFIX_KEY)
                                          .addField(StructuralField.LOCAL_NAME_KEY)
                                          .addField(StructuralField.URI_KEY)
                                          .addField(StructuralField.PREVIOUS_REVISION)
                                          .addField(StructuralField.LAST_MODIFIED_REVISION)
                                          .addField(StructuralField.HASH)
                                          .build());

    put(NodeKind.TEXT, NodeKindLayout.builder(NodeKind.TEXT)
                                     .addField(StructuralField.PARENT_KEY)
                                     .addField(StructuralField.RIGHT_SIBLING_KEY)
                                     .addField(StructuralField.LEFT_SIBLING_KEY)
                                     .addField(StructuralField.PREVIOUS_REVISION)
                                     .addField(StructuralField.LAST_MODIFIED_REVISION)
                                     .addField(StructuralField.HASH)
                                     .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                     .build());

    put(NodeKind.COMMENT, NodeKindLayout.builder(NodeKind.COMMENT)
                                        .addField(StructuralField.PARENT_KEY)
                                        .addField(StructuralField.RIGHT_SIBLING_KEY)
                                        .addField(StructuralField.LEFT_SIBLING_KEY)
                                        .addField(StructuralField.PREVIOUS_REVISION)
                                        .addField(StructuralField.LAST_MODIFIED_REVISION)
                                        .addField(StructuralField.HASH)
                                        .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                        .build());

    put(NodeKind.PROCESSING_INSTRUCTION, NodeKindLayout.builder(NodeKind.PROCESSING_INSTRUCTION)
                                                        .addField(StructuralField.PARENT_KEY)
                                                        .addField(StructuralField.RIGHT_SIBLING_KEY)
                                                        .addField(StructuralField.LEFT_SIBLING_KEY)
                                                        .addField(StructuralField.FIRST_CHILD_KEY)
                                                        .addField(StructuralField.LAST_CHILD_KEY)
                                                        .addField(StructuralField.PATH_NODE_KEY)
                                                        .addField(StructuralField.PREFIX_KEY)
                                                        .addField(StructuralField.LOCAL_NAME_KEY)
                                                        .addField(StructuralField.URI_KEY)
                                                        .addField(StructuralField.PREVIOUS_REVISION)
                                                        .addField(StructuralField.LAST_MODIFIED_REVISION)
                                                        .addField(StructuralField.HASH)
                                                        .addField(StructuralField.CHILD_COUNT)
                                                        .addField(StructuralField.DESCENDANT_COUNT)
                                                        .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                                        .build());
  }

  private static void registerJsonLayouts() {
    put(NodeKind.JSON_DOCUMENT, NodeKindLayout.builder(NodeKind.JSON_DOCUMENT)
                                              .addField(StructuralField.FIRST_CHILD_KEY)
                                              .addField(StructuralField.LAST_CHILD_KEY)
                                              .addField(StructuralField.CHILD_COUNT)
                                              .addField(StructuralField.DESCENDANT_COUNT)
                                              .addField(StructuralField.HASH)
                                              .build());

    put(NodeKind.OBJECT, NodeKindLayout.builder(NodeKind.OBJECT)
                                       .addField(StructuralField.PARENT_KEY)
                                       .addField(StructuralField.RIGHT_SIBLING_KEY)
                                       .addField(StructuralField.LEFT_SIBLING_KEY)
                                       .addField(StructuralField.FIRST_CHILD_KEY)
                                       .addField(StructuralField.LAST_CHILD_KEY)
                                       .addField(StructuralField.PREVIOUS_REVISION)
                                       .addField(StructuralField.LAST_MODIFIED_REVISION)
                                       .addField(StructuralField.HASH)
                                       .addField(StructuralField.CHILD_COUNT)
                                       .addField(StructuralField.DESCENDANT_COUNT)
                                       .build());

    put(NodeKind.ARRAY, NodeKindLayout.builder(NodeKind.ARRAY)
                                      .addField(StructuralField.PARENT_KEY)
                                      .addField(StructuralField.RIGHT_SIBLING_KEY)
                                      .addField(StructuralField.LEFT_SIBLING_KEY)
                                      .addField(StructuralField.FIRST_CHILD_KEY)
                                      .addField(StructuralField.LAST_CHILD_KEY)
                                      .addField(StructuralField.PATH_NODE_KEY)
                                      .addField(StructuralField.PREVIOUS_REVISION)
                                      .addField(StructuralField.LAST_MODIFIED_REVISION)
                                      .addField(StructuralField.HASH)
                                      .addField(StructuralField.CHILD_COUNT)
                                      .addField(StructuralField.DESCENDANT_COUNT)
                                      .build());

    put(NodeKind.OBJECT_KEY, NodeKindLayout.builder(NodeKind.OBJECT_KEY)
                                           .addField(StructuralField.PARENT_KEY)
                                           .addField(StructuralField.RIGHT_SIBLING_KEY)
                                           .addField(StructuralField.LEFT_SIBLING_KEY)
                                           .addField(StructuralField.FIRST_CHILD_KEY)
                                           .addField(StructuralField.LAST_CHILD_KEY)
                                           .addField(StructuralField.PATH_NODE_KEY)
                                           .addField(StructuralField.NAME_KEY)
                                           .addField(StructuralField.PREVIOUS_REVISION)
                                           .addField(StructuralField.LAST_MODIFIED_REVISION)
                                           .addField(StructuralField.HASH)
                                           .addField(StructuralField.DESCENDANT_COUNT)
                                           .build());

    put(NodeKind.STRING_VALUE, NodeKindLayout.builder(NodeKind.STRING_VALUE)
                                             .addField(StructuralField.PARENT_KEY)
                                             .addField(StructuralField.RIGHT_SIBLING_KEY)
                                             .addField(StructuralField.LEFT_SIBLING_KEY)
                                             .addField(StructuralField.PREVIOUS_REVISION)
                                             .addField(StructuralField.LAST_MODIFIED_REVISION)
                                             .addField(StructuralField.HASH)
                                             .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                             .build());

    put(NodeKind.NUMBER_VALUE, NodeKindLayout.builder(NodeKind.NUMBER_VALUE)
                                             .addField(StructuralField.PARENT_KEY)
                                             .addField(StructuralField.RIGHT_SIBLING_KEY)
                                             .addField(StructuralField.LEFT_SIBLING_KEY)
                                             .addField(StructuralField.PREVIOUS_REVISION)
                                             .addField(StructuralField.LAST_MODIFIED_REVISION)
                                             .addField(StructuralField.HASH)
                                             .addPayloadRef("number", PayloadRefKind.VALUE_BLOB)
                                             .build());

    put(NodeKind.BOOLEAN_VALUE, NodeKindLayout.builder(NodeKind.BOOLEAN_VALUE)
                                              .addField(StructuralField.PARENT_KEY)
                                              .addField(StructuralField.RIGHT_SIBLING_KEY)
                                              .addField(StructuralField.LEFT_SIBLING_KEY)
                                              .addField(StructuralField.PREVIOUS_REVISION)
                                              .addField(StructuralField.LAST_MODIFIED_REVISION)
                                              .addField(StructuralField.HASH)
                                              .addField(StructuralField.BOOLEAN_VALUE)
                                              .addPadding(7)
                                              .build());

    put(NodeKind.NULL_VALUE, NodeKindLayout.builder(NodeKind.NULL_VALUE)
                                           .addField(StructuralField.PARENT_KEY)
                                           .addField(StructuralField.RIGHT_SIBLING_KEY)
                                           .addField(StructuralField.LEFT_SIBLING_KEY)
                                           .addField(StructuralField.PREVIOUS_REVISION)
                                           .addField(StructuralField.LAST_MODIFIED_REVISION)
                                           .addField(StructuralField.HASH)
                                           .build());

    put(NodeKind.OBJECT_STRING_VALUE, NodeKindLayout.builder(NodeKind.OBJECT_STRING_VALUE)
                                                    .addField(StructuralField.PARENT_KEY)
                                                    .addField(StructuralField.PREVIOUS_REVISION)
                                                    .addField(StructuralField.LAST_MODIFIED_REVISION)
                                                    .addField(StructuralField.HASH)
                                                    .addPayloadRef("value", PayloadRefKind.VALUE_BLOB)
                                                    .build());

    put(NodeKind.OBJECT_NUMBER_VALUE, NodeKindLayout.builder(NodeKind.OBJECT_NUMBER_VALUE)
                                                    .addField(StructuralField.PARENT_KEY)
                                                    .addField(StructuralField.PREVIOUS_REVISION)
                                                    .addField(StructuralField.LAST_MODIFIED_REVISION)
                                                    .addField(StructuralField.HASH)
                                                    .addPayloadRef("number", PayloadRefKind.VALUE_BLOB)
                                                    .build());

    put(NodeKind.OBJECT_BOOLEAN_VALUE, NodeKindLayout.builder(NodeKind.OBJECT_BOOLEAN_VALUE)
                                                     .addField(StructuralField.PARENT_KEY)
                                                     .addField(StructuralField.PREVIOUS_REVISION)
                                                     .addField(StructuralField.LAST_MODIFIED_REVISION)
                                                     .addField(StructuralField.HASH)
                                                     .addField(StructuralField.BOOLEAN_VALUE)
                                                     .addPadding(7)
                                                     .build());

    put(NodeKind.OBJECT_NULL_VALUE, NodeKindLayout.builder(NodeKind.OBJECT_NULL_VALUE)
                                                  .addField(StructuralField.PARENT_KEY)
                                                  .addField(StructuralField.PREVIOUS_REVISION)
                                                  .addField(StructuralField.LAST_MODIFIED_REVISION)
                                                  .addField(StructuralField.HASH)
                                                  .build());
  }

  private static void put(final NodeKind kind, final NodeKindLayout layout) {
    LAYOUTS_BY_ORDINAL[kind.ordinal()] = layout;
  }
}
