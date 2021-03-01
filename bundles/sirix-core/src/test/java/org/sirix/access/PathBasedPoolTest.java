package org.sirix.access;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests the behavior of {@link PathBasedPool}.
 *
 * @author João Sousa
 */
class PathBasedPoolTest {

    private PathBasedPool<Object> sessions;

    @BeforeEach
    public void setup() {
        this.sessions = new PathBasedPool<>();
    }

    /**
     * Tests that {@link PathBasedPool#putObject(Path, Object)} matches the following requirements:
     * <ul>
     *     <li>Entries are persisted</li>
     *     <li>Entries from different keys are independent</li>
     *     <li>Entries from the same key are grouped in the same {@link Set}</li>
     * </ul>
     */
    @Test
    public final void putWillPersistTheObjectInMap() {
        final var key1 = mock(Path.class);
        final var object1 = new Object();
        final var object2 = new Object();

        final var key2 = mock(Path.class);
        final var object3 = new Object();

        this.sessions.putObject(key1, object1);
        this.sessions.putObject(key1, object2);
        this.sessions.putObject(key2, object3);

        final Map<Path, Set<Object>> expected = ImmutableMap.of(
                key1, ImmutableSet.of(object1, object2),
                key2, ImmutableSet.of(object3)
        );

        assertEquals(expected, this.sessions.asMap());
    }

    /**
     * Tests that {@link PathBasedPool#containsAnyEntry(Path)} reports {@code true} for at least one
     * object, and {@code false} for no objects.
     */
    @Test
    public final void containsShouldReportExistingKeys() {
        final var keySingle = mock(Path.class);
        final var keyMulti = mock(Path.class);
        final var keyNone = mock(Path.class);

        final var object1 = new Object();
        final var object2 = new Object();
        final var object3 = new Object();

        this.sessions.putObject(keySingle, object1);
        this.sessions.putObject(keyMulti, object2);
        this.sessions.putObject(keyMulti, object3);

        assertTrue(this.sessions.containsAnyEntry(keySingle));
        assertTrue(this.sessions.containsAnyEntry(keyMulti));
        assertFalse(this.sessions.containsAnyEntry(keyNone));
    }

    /**
     * Tests that {@link PathBasedPool#removeObject(Path, Object)} will eliminate the
     * entry from the pool.
     */
    @Test
    public final void removeShouldEliminateEntries() {
        final var key = mock(Path.class);
        final var object1 = new Object();

        this.sessions.putObject(key, object1);
        this.sessions.removeObject(key, object1);

        assertFalse(this.sessions.containsAnyEntry(key));
    }

}
