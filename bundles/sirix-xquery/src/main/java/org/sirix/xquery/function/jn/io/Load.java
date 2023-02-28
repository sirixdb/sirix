package org.sirix.xquery.function.jn.io;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.*;
import org.brackit.xquery.jdm.type.AnyJsonItemType;
import org.brackit.xquery.jdm.type.AtomicType;
import org.brackit.xquery.jdm.type.Cardinality;
import org.brackit.xquery.jdm.type.SequenceType;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.FunctionConversionSequence;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.util.io.URIHandler;
import org.sirix.xquery.function.DateTimeToInstant;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBStore;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * Function for loading a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>jn:load($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as json-item()?</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionAnnotation(
    description = "Store the given fragments in a collection. "
        + "If explicitly required or if the collection does not exist, " + "a new collection will be created. ",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Load extends AbstractFunction {

  /** Load function name. */
  public final static QNm LOAD = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "load");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param createNew determines if a new collection has to be created or not
   */
  public Load(final boolean createNew) {
    this(LOAD, createNew);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createNew determines if a new collection has to be created or not
   */
  public Load(final QNm name, final boolean createNew) {
    super(name, createNew
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public Load(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, false);
      final Sequence resources = args[2];
      if (resources == null) {
        throw new QueryException(new QNm("No sequence of resources specified!"));
      }
      final boolean createNew = args.length < 4 || args[3].booleanValue();

      final String commitMessage = args.length >= 5 ? FunUtil.getString(args, 4, "commitMessage", null, null, false) : null;

      final DateTime dateTime = args.length == 6 ? (DateTime) args[5] : null;
      final Instant commitTimesstamp = args.length == 6 ? dateTimeToInstant.convert(dateTime) : null;

      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
      JsonDBCollection coll;
      if (createNew) {
        coll = create(store, collName, resName, resources, commitMessage, commitTimesstamp);
      } else {
        try {
          coll = store.lookup(collName);
          add(coll, resName, resources, commitMessage, commitTimesstamp);
        } catch (final DocumentException e) {
          // collection does not exist
          coll = create(store, collName, resName, resources, commitMessage, commitTimesstamp);
        }
      }

      return coll;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static void add(final JsonDBCollection coll, final String resName,
      final Sequence resources, final String commitMessage, final Instant commitTimestamp) {
    if (resources instanceof Atomic res) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(res.stringValue())))) {
        coll.add(resName, reader, commitMessage, commitTimestamp);
      } catch (final Exception e) {
        throw new QueryException(new QNm(e.getMessage()), e);
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        int size = coll.getDatabase().listResources().size();
        for (Item item; (item = iter.next()) != null; ) {
          try (final JsonReader reader = new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue())))) {
            coll.add("resource" + size++, reader);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      }
    }
  }

  private JsonDBCollection create(final JsonDBStore store, final String collName, final String resName,
      final Sequence resources, final String commitMessage, final Instant commitTimestamp) throws IOException {
    if (resources instanceof Str) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) resources).stringValue())))) {
        return store.create(collName, resName, reader, commitMessage, commitTimestamp);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        final Set<JsonReader> jsonReaders = new HashSet<>();

        for (Item item; (item = iter.next()) != null; ) {
          jsonReaders.add(new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue()))));
        }

        final JsonDBCollection collection = store.create(collName, jsonReaders);

        jsonReaders.forEach(this::closeReader);

        return collection;
      }
    }

    return null;
  }

  private void closeReader(JsonReader reader) {
    try {
      reader.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
