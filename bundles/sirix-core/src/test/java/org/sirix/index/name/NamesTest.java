package org.sirix.index.name;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import org.junit.Test;

public final class NamesTest {
  @Test
  public void whenIndexExistsForAnotherString_createNewIndex() {
    final var names = Names.getInstance();

    final var fbIndex = names.setName("FB");
    final var EaIndex = names.setName("Ea");

    assertNotEquals(fbIndex, EaIndex);
  }

  @Test
  public void whenIndexExistsForSameString_createNoNewIndex() {
    final var names = Names.getInstance();

    final var fbIndex = names.setName("FB");
    final var EaIndex = names.setName("FB");

    assertEquals(fbIndex, EaIndex);
  }

  @Test
  public void whenIndexExistsForSameString_increaseCounter() {
    final var names = Names.getInstance();

    final var index = names.setName("FB");
    names.setName("FB");

    assertEquals(2, names.getCount(index));
  }

  @Test
  public void whenIndexDoesNotExistsForString_counterIsZero() {
    final var names = Names.getInstance();

    assertEquals(0, names.getCount(5));
  }

  @Test
  public void whenNameIsSet_getNameReturnsName() {
    final var names = Names.getInstance();

    final var testName = "FB";
    final var index = names.setName(testName);

    final var name = names.getName(index);

    assertEquals(testName, name);
  }
}
