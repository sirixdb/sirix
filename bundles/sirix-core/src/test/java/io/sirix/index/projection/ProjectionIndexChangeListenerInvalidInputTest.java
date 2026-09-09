package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Invalid-input contract of {@link ProjectionIndexChangeListener}'s public constructor.
 *
 * <p>
 * The public constructor delegates to the injecting one, and {@code this(...)} must be the first
 * statement — so the argument expression that resolves the armed bulk load runs BEFORE the
 * delegated constructor validates anything. That ordering is the hazard this pins:
 *
 * <ul>
 * <li>resolving first would dereference arguments nobody has checked, so identical bad input would
 * fail differently depending on whether the GLOBAL bulk-load registry happened to hold an entry —
 * an NPE from inside the resolver with a load armed, the intended exception without one;</li>
 * <li>and validating in a DIFFERENT order from the delegated constructor silently changes which
 * exception wins when more than one argument is bad. A non-projection {@link IndexDef} together
 * with a null transaction is the case that exposes it: checking the transaction first turns an
 * {@link IllegalArgumentException} into a {@link NullPointerException}.</li>
 * </ul>
 *
 * <p>
 * Every case here is asserted against the PUBLIC constructor, because the delegating hop is exactly
 * what could regress it.
 */
final class ProjectionIndexChangeListenerInvalidInputTest {

  private static IndexDef projectionDef() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
  }

  private static IndexDef nonProjectionDef() {
    return IndexDefs.createCASIdxDef(false, Type.STR, java.util.Set.of(parse("/[]/value", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
  }

  @Test
  @DisplayName("a non-projection IndexDef is rejected with the type message, not a null dereference")
  void nonProjectionIndexDefIsRejected() {
    final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class), mock(PathSummaryReader.class),
            nonProjectionDef(), mock(NodeReadOnlyTrx.class)));
    org.junit.jupiter.api.Assertions.assertTrue(
        thrown.getMessage().contains("requires an IndexType.PROJECTION IndexDef"), thrown.getMessage());
  }

  @Test
  @DisplayName("the projection-type check wins over a null transaction, as it always has")
  void projectionTypeIsCheckedBeforeTheTransaction() {
    // THE ordering case. The delegated constructor checks the index type first and the transaction
    // last; a resolver that null-checked the transaction early would report NPE here instead.
    final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class), mock(PathSummaryReader.class),
            nonProjectionDef(), null));
    org.junit.jupiter.api.Assertions.assertTrue(
        thrown.getMessage().contains("requires an IndexType.PROJECTION IndexDef"), thrown.getMessage());
  }

  @Test
  @DisplayName("null arguments are reported in the delegated constructor's own order")
  void nullArgumentsAreReportedInOrder() {
    final IndexDef def = projectionDef();
    assertEquals("storageEngineWriter",
        assertThrows(NullPointerException.class, () -> new ProjectionIndexChangeListener(null,
            mock(PathSummaryReader.class), def, mock(NodeReadOnlyTrx.class))).getMessage());
    assertEquals("pathSummary",
        assertThrows(NullPointerException.class,
            () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class), null, def,
                mock(NodeReadOnlyTrx.class))).getMessage());
    assertEquals("maintenanceTrx",
        assertThrows(NullPointerException.class,
            () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class), mock(PathSummaryReader.class), def,
                null)).getMessage());
    // storageEngineWriter outranks maintenanceTrx when BOTH are null — the delegated order.
    assertEquals("storageEngineWriter", assertThrows(NullPointerException.class,
        () -> new ProjectionIndexChangeListener(null, mock(PathSummaryReader.class), def, null)).getMessage());
  }

  @Test
  @DisplayName("invalid input is rejected by validation, with no bulk load armed")
  void invalidInputIsRejectedByValidation() {
    // SCOPE, stated honestly: this exercises the UNARMED registry only. It does NOT prove the
    // armed case, and it deliberately does not arm one — planting a global ProjectionBulkLoad from
    // a unit test is the same cross-test contamination that made a sibling test in this package
    // fail in package order while passing alone, and it would be a poor trade for one assertion.
    //
    // What still pins validation-before-resolution is structural and is covered above: the checks
    // run in the delegated constructor's exact order, so a resolver that dereferenced unchecked
    // arguments would have to change one of those outcomes to slip through. The ordering guards
    // fail the moment that order moves — verified by mutation.
    assertFalse(ProjectionBulkLoad.anyActive(),
        "precondition: no bulk load armed, so these outcomes are the unarmed ones");

    final IndexDef def = projectionDef();
    assertEquals("maintenanceTrx",
        assertThrows(NullPointerException.class,
            () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class), mock(PathSummaryReader.class), def,
                null)).getMessage(),
        "a null transaction must be reported as such, not dereferenced by the bulk-load resolver");
    assertThrows(
        IllegalArgumentException.class, () -> new ProjectionIndexChangeListener(mock(StorageEngineWriter.class),
            mock(PathSummaryReader.class), nonProjectionDef(), null),
        "the projection-type refusal must precede every null check");
  }
}
