package org.sirix.xquery.function.jn.io;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.sequence.FunctionConversionSequence;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.*;
import org.brackit.xquery.xdm.type.AnyJsonItemType;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.function.DateTimeToInstant;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Function for storing a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>jn:store($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as json-item()</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionAnnotation(
    description = "Store the given fragments in a collection. "
        + "If explicitly required or if the collection does not exist, " + "a new collection will be created. ",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Store extends AbstractFunction {

  /** Store function name. */
  public final static QNm STORE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "store");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param createNew determines if a new collection has to be created or not
   */
  public Store(final boolean createNew) {
    this(STORE, createNew);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createNew determines if a new collection has to be created or not
   */
  public Store(final QNm name, final boolean createNew) {
    super(name, createNew
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public Store(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, false);
      final Sequence nodes = args[2];
      if (nodes == null) {
        throw new QueryException(new QNm("No sequence of nodes specified!"));
      }
      final boolean createNew = args.length < 4 || args[3].booleanValue();

      final String commitMessage = args.length >= 5 ? FunUtil.getString(args, 4, "commitMessage", null, null, false) : null;

      final DateTime dateTime = args.length == 6 ? (DateTime) args[5] : null;
      final Instant commitTimesstamp = args.length == 6 ? dateTimeToInstant.convert(dateTime) : null;

      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
      if (createNew) {
        create(store, collName, resName, nodes, commitMessage, commitTimesstamp);
      } else {
        try {
          final JsonDBCollection coll = store.lookup(collName);
          add(coll, resName, nodes, commitMessage, commitTimesstamp);
        } catch (final DocumentException e) {
          // collection does not exist
          create(store, collName, resName, nodes, commitMessage, commitTimesstamp);
        }
      }

      return null;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static void add(final JsonDBCollection coll, final String resName,
      final Sequence nodes, final String commitMessage, final Instant commitTimestamp) {
    if (nodes instanceof Str) {
      try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
        coll.add(resName, reader, commitMessage, commitTimestamp);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (nodes instanceof final FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        int size = coll.getDatabase().listResources().size();
        for (Item item; (item = iter.next()) != null; ) {
          try (final JsonReader reader = JsonShredder.createStringReader(((Str) item).stringValue())) {
            coll.add("resource" + size++, reader);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      }
    }
  }

  private static void create(final JsonDBStore store, final String collName, final String resName,
      final Sequence nodes, String commitMessage, Instant commitTimestamp) {
    if (nodes instanceof Str string) {
      if (string.stringValue().isEmpty()) {
        store.create(collName, resName, (String) null, commitMessage, commitTimestamp);
      } else {
        try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
          store.create(collName, resName, reader);
        } catch (final Exception e) {
          throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
        }
      }
    } else if (nodes instanceof final FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        final List<Str> list = new ArrayList<>();

        for (Item item; (item = iter.next()) != null; ) {
          list.add((Str) item);
        }

        store.createFromJsonStrings(collName, new ArrayStream<>(list.toArray(new Str[0])));
      }
    }
  }
}
