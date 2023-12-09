package io.sirix.access.trx.node.json;

public enum InsertOperations {
  INSERT("insert"),

  UPDATE("update"),

  DELETE("delete"),

  REPLACE("replace");

  private final String name;

  InsertOperations(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
