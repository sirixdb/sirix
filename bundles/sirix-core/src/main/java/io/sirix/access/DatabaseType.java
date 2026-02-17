package io.sirix.access;

import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.Node;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.settings.Fixed;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum DatabaseType {
  XML("xml") {
    @Override
    public <R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> Database<R> createDatabase(
        DatabaseConfiguration dbConfig, User user) {
      return (Database<R>) Databases.MANAGER.xmlDatabaseFactory().createDatabase(dbConfig, user);
    }

    @Override
    public Node getDocumentNode(SirixDeweyID id) {
      return new XmlDocumentRootNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, null, id);
    }
  },

  JSON("json") {
    @Override
    public <R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> Database<R> createDatabase(
        DatabaseConfiguration dbConfig, User user) {
      return (Database<R>) Databases.MANAGER.jsonDatabaseFactory().createDatabase(dbConfig, user);
    }

    @Override
    public Node getDocumentNode(SirixDeweyID id) {
      return new JsonDocumentRootNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, null, id);
    }
  };

  private final String stringType;

  DatabaseType(String stringType) {
    this.stringType = stringType;
  }

  public String getStringType() {
    return stringType;
  }

  private static final Map<String, DatabaseType> stringToEnum =
      Stream.of(values()).collect(Collectors.toMap(Object::toString, e -> e));

  public static Optional<DatabaseType> fromString(String symbol) {
    return Optional.ofNullable(stringToEnum.get(symbol));
  }

  public abstract <R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> Database<R> createDatabase(
      DatabaseConfiguration dbConfig, User user);

  public abstract Node getDocumentNode(SirixDeweyID id);
}
