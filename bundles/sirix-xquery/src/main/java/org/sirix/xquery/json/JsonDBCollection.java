package org.sirix.xquery.json;

import java.nio.file.Path;
import java.time.Instant;
import org.brackit.xquery.jsonitem.AbstractJsonItemCollection;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.TemporalJsonCollection;

public final class JsonDBCollection extends AbstractJsonItemCollection<JsonDBNode>
    implements TemporalJsonCollection<JsonDBNode>, AutoCloseable {

  public JsonDBCollection(String name) {
    super(name);
  }

  @Override
  public void delete() throws DocumentException {
    // TODO Auto-generated method stub

  }

  @Override
  public void remove(long documentID) {
    // TODO Auto-generated method stub

  }

  @Override
  public JsonDBNode getDocument() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stream<? extends JsonDBNode> getDocuments() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stream<? extends JsonDBNode> getDocuments(boolean updatable) throws DocumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonDBNode add(Path file) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getDocumentCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public JsonDBNode getDocument(int revision) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonDBNode getDocument(Instant pointInTime) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonDBNode getDocument(String name, int revision) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonDBNode getDocument(String name, Instant pointInTime) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonDBNode getDocument(String name) {
    // TODO Auto-generated method stub
    return null;
  }


}
