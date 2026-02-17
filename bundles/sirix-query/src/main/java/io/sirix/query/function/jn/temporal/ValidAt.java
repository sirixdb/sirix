package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;

import java.time.Instant;

/**
 * <p>
 * Function for querying data by valid time. Returns all records where the valid time interval
 * contains the specified timestamp. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:valid-at($coll as xs:string, $res as xs:string, $validTime as xs:dateTime) as json-item()*</code></li>
 * </ul>
 *
 * <p>
 * This function is part of the bitemporal query support in SirixDB. It requires the resource to be
 * configured with valid time paths via
 * {@link io.sirix.access.ResourceConfiguration.Builder#validTimePaths(String, String)}.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidAt extends AbstractFunction {

  /**
   * Function name.
   */
  public static final QNm VALID_AT = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "valid-at");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public ValidAt(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("Expected 3 arguments: collection, resource, validTime"));
    }

    final JsonDBCollection collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (collection == null) {
      throw new QueryException(new QNm("Collection not found: " + ((Str) args[0]).stringValue()));
    }

    final String resourceName = ((Str) args[1]).stringValue();
    final DateTime dateTime = (DateTime) args[2];
    final Instant validTime = dateTimeToInstant.convert(dateTime);

    // Get the document at the most recent revision
    final JsonDBItem document = collection.getDocument(resourceName);
    if (document == null) {
      throw new QueryException(new QNm("Resource not found: " + resourceName));
    }

    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();
    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();

    if (validTimeConfig == null) {
      throw new QueryException(new QNm("Resource does not have valid time configuration. "
          + "Configure valid time paths when creating the resource."));
    }

    // Return a lazy sequence that filters by valid time
    return new ValidTimeFilterSequence(document, validTime, validTimeConfig);
  }

  /**
   * A lazy sequence that filters items by valid time.
   */
  private static class ValidTimeFilterSequence extends LazySequence {
    private final JsonDBItem document;
    private final Instant validTime;
    private final ValidTimeConfig validTimeConfig;

    ValidTimeFilterSequence(JsonDBItem document, Instant validTime, ValidTimeConfig validTimeConfig) {
      this.document = document;
      this.validTime = validTime;
      this.validTimeConfig = validTimeConfig;
    }

    @Override
    public Iter iterate() {
      return new ValidTimeFilterIter(document, validTime, validTimeConfig);
    }
  }

  /**
   * Iterator that filters items by valid time.
   */
  private static class ValidTimeFilterIter extends BaseIter {
    private final JsonDBItem document;
    private final Instant validTime;
    private final ValidTimeConfig validTimeConfig;
    private final JsonItemFactory jsonItemFactory = new JsonItemFactory();
    private Iter childIter;
    private boolean initialized;

    ValidTimeFilterIter(JsonDBItem document, Instant validTime, ValidTimeConfig validTimeConfig) {
      this.document = document;
      this.validTime = validTime;
      this.validTimeConfig = validTimeConfig;
      this.initialized = false;
    }

    @Override
    public Item next() {
      if (!initialized) {
        initialized = true;
        // Check if the document itself matches the valid time criteria
        if (isValidAtTime(document)) {
          return document;
        }
        // If the document is an array or object, iterate its children
        if (document instanceof io.brackit.query.jdm.json.Array array) {
          childIter = array.iterate();
        }
      }

      if (childIter != null) {
        Item item;
        while ((item = childIter.next()) != null) {
          if (item instanceof JsonDBItem jsonItem && isValidAtTime(jsonItem)) {
            return item;
          }
        }
      }

      return null;
    }

    /**
     * Checks if the given item is valid at the specified time.
     *
     * @param item the item to check
     * @return true if the item is valid at the specified time
     */
    private boolean isValidAtTime(JsonDBItem item) {
      if (!(item instanceof io.brackit.query.jdm.json.Object obj)) {
        return false;
      }

      try {
        final var validFromField = new QNm(validTimeConfig.getNormalizedValidFromPath());
        final var validToField = new QNm(validTimeConfig.getNormalizedValidToPath());

        final Sequence validFromSeq = obj.get(validFromField);
        final Sequence validToSeq = obj.get(validToField);

        if (validFromSeq == null || validToSeq == null) {
          return false;
        }

        final Instant validFrom = parseInstant(validFromSeq);
        final Instant validTo = parseInstant(validToSeq);

        if (validFrom == null || validTo == null) {
          return false;
        }

        // Check if validFrom <= validTime <= validTo
        return !validTime.isBefore(validFrom) && !validTime.isAfter(validTo);
      } catch (Exception e) {
        return false;
      }
    }

    /**
     * Parses an Instant from a sequence.
     */
    private Instant parseInstant(Sequence seq) {
      if (seq instanceof DateTime dt) {
        return new DateTimeToInstant().convert(dt);
      }
      if (seq instanceof Str str) {
        try {
          return Instant.parse(str.stringValue());
        } catch (Exception e) {
          return null;
        }
      }
      return null;
    }

    @Override
    public void close() {
      if (childIter != null) {
        childIter.close();
      }
    }
  }
}
