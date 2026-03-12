package io.sirix.query.compiler.optimizer.mesh;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link Mesh} and {@link EquivalenceClass} — the search space
 * management data structure (Graefe/DeWitt Exodus).
 */
final class MeshTest {

  @Test
  void createClassAndRetrieveBestPlan() {
    final var mesh = new Mesh(4);
    final var plan = new AST(XQ.ForBind, null);

    final int classId = mesh.createClass(plan, 10.0);

    assertEquals(0, classId);
    assertEquals(plan, mesh.getBestPlan(classId));
    assertEquals(10.0, mesh.getBestCost(classId));
    assertEquals(1, mesh.classCount());
  }

  @Test
  void addCheaperAlternativeUpdatesBest() {
    final var mesh = new Mesh(4);
    final var plan1 = new AST(XQ.ForBind, null);
    final var plan2 = new AST(XQ.LetBind, null);

    final int classId = mesh.createClass(plan1, 100.0);
    mesh.addAlternative(classId, plan2, 25.0);

    assertEquals(plan2, mesh.getBestPlan(classId),
        "Cheaper alternative should become the best plan");
    assertEquals(25.0, mesh.getBestCost(classId));
  }

  @Test
  void addExpensiveAlternativeKeepsOriginal() {
    final var mesh = new Mesh(4);
    final var plan1 = new AST(XQ.ForBind, null);
    final var plan2 = new AST(XQ.LetBind, null);

    final int classId = mesh.createClass(plan1, 10.0);
    mesh.addAlternative(classId, plan2, 50.0);

    assertEquals(plan1, mesh.getBestPlan(classId),
        "Original cheaper plan should remain the best");
    assertEquals(10.0, mesh.getBestCost(classId));
  }

  @Test
  void multipleClassesIndependent() {
    final var mesh = new Mesh(4);
    final var plan1 = new AST(XQ.ForBind, null);
    final var plan2 = new AST(XQ.LetBind, null);

    final int class1 = mesh.createClass(plan1, 10.0);
    final int class2 = mesh.createClass(plan2, 20.0);

    assertEquals(0, class1);
    assertEquals(1, class2);
    assertEquals(plan1, mesh.getBestPlan(class1));
    assertEquals(plan2, mesh.getBestPlan(class2));
    assertEquals(2, mesh.classCount());
  }

  @Test
  void getBestPlanForNonexistentClassReturnsNull() {
    final var mesh = new Mesh(4);
    assertNull(mesh.getBestPlan(999));
    assertEquals(Double.MAX_VALUE, mesh.getBestCost(999));
  }

  @Test
  void addAlternativeToNonexistentClassThrows() {
    final var mesh = new Mesh(4);
    assertThrows(IllegalArgumentException.class,
        () -> mesh.addAlternative(999, new AST(XQ.ForBind, null), 5.0));
  }

  @Test
  void childClassReferences() {
    final var mesh = new Mesh(4);
    final int parent = mesh.createClass(new AST(XQ.Join, null), 50.0);
    final int child1 = mesh.createClass(new AST(XQ.ForBind, null), 10.0);
    final int child2 = mesh.createClass(new AST(XQ.LetBind, null), 20.0);

    mesh.setChildClasses(parent, new int[]{child1, child2});

    final int[] children = mesh.getChildClasses(parent);
    assertNotNull(children);
    assertEquals(2, children.length);
    assertEquals(child1, children[0]);
    assertEquals(child2, children[1]);
  }

  @Test
  void getChildClassesReturnsNullForNoChildren() {
    final var mesh = new Mesh(4);
    final int classId = mesh.createClass(new AST(XQ.ForBind, null), 10.0);
    assertNull(mesh.getChildClasses(classId));
  }

  @Test
  void addAlternativeViaExistingClass() {
    final var mesh = new Mesh(4);
    final var original = new AST(XQ.ForBind, null);
    final var rewritten = new AST(XQ.LetBind, null);

    final int classId = mesh.createClass(original, 100.0);
    mesh.addAlternative(classId, rewritten, 30.0);

    assertEquals(rewritten, mesh.getBestPlan(classId));
    assertEquals(30.0, mesh.getBestCost(classId));

    final var eqClass = mesh.getClass(classId);
    assertNotNull(eqClass);
    assertEquals(2, eqClass.size());
  }

  @Test
  void clearResetsAllState() {
    final var mesh = new Mesh(4);
    mesh.createClass(new AST(XQ.ForBind, null), 10.0);
    mesh.createClass(new AST(XQ.LetBind, null), 20.0);

    mesh.clear();

    assertEquals(0, mesh.classCount());
    assertEquals(0, mesh.nextClassId());
    assertNull(mesh.getBestPlan(0));
  }

  @Test
  void equivalenceClassTracksAllAlternatives() {
    final var eqClass = new EquivalenceClass(0);
    final var plan1 = new AST(XQ.ForBind, null);
    final var plan2 = new AST(XQ.LetBind, null);
    final var plan3 = new AST(XQ.Join, null);

    eqClass.addAlternative(plan1, 50.0);
    eqClass.addAlternative(plan2, 10.0);
    eqClass.addAlternative(plan3, 30.0);

    assertEquals(3, eqClass.size());
    assertEquals(plan2, eqClass.getBestPlan());
    assertEquals(10.0, eqClass.getBestCost());
    assertEquals(0, eqClass.classId());

    assertEquals(plan1, eqClass.getAlternative(0).plan());
    assertEquals(50.0, eqClass.getAlternative(0).cost());
    assertEquals(plan2, eqClass.getAlternative(1).plan());
    assertEquals(10.0, eqClass.getAlternative(1).cost());
  }

  @Test
  void emptyEquivalenceClassReturnsNull() {
    final var eqClass = new EquivalenceClass(0);
    assertNull(eqClass.getBestPlan());
    assertEquals(Double.MAX_VALUE, eqClass.getBestCost());
    assertEquals(0, eqClass.size());
  }

  @Test
  void rejectsNullPlan() {
    final var eqClass = new EquivalenceClass(0);
    assertThrows(IllegalArgumentException.class,
        () -> eqClass.addAlternative(null, 10.0));
  }

  @Test
  void rejectsNaNCost() {
    final var eqClass = new EquivalenceClass(0);
    assertThrows(IllegalArgumentException.class,
        () -> eqClass.addAlternative(new AST(XQ.ForBind, null), Double.NaN));
  }

  @Test
  void rejectsNegativeCost() {
    final var eqClass = new EquivalenceClass(0);
    assertThrows(IllegalArgumentException.class,
        () -> eqClass.addAlternative(new AST(XQ.ForBind, null), -1.0));
  }
}
