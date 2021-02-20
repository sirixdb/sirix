package org.sirix.access;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.api.Database;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DatabaseSessionPoolTest {

    private DatabaseSessionPool sessions;

    @BeforeEach
    public void setup() {

        this.sessions = new DatabaseSessionPool();
    }

    /**
     * Tests that {@link DatabaseSessionPool#putDatabase(Path, Database)} matches the following requirements:
     * <ul>
     *     <li>Entries are persisted</li>
     *     <li>Entries from different keys are independent</li>
     *     <li>Entries from the same key are grouped in the same {@link Set}</li>
     * </ul>
     */
    @Test
    public final void putWillPersistTheDatabaseInMap() {

        final Path key1 = mock(Path.class);
        final Database<?> db1 = mock(Database.class);
        final Database<?> db2 = mock(Database.class);

        final Path key2 = mock(Path.class);
        final Database<?> db3 = mock(Database.class);

        this.sessions.putDatabase(key1, db1);
        this.sessions.putDatabase(key1, db2);
        this.sessions.putDatabase(key2, db3);

        final Map<Path, Set<Database<?>>> expected = ImmutableMap.of(
                key1, ImmutableSet.of(db1, db2),
                key2, ImmutableSet.of(db3)
        );

        assertEquals(expected, this.sessions.asMap());
    }

    /**
     * Tests that {@link DatabaseSessionPool#containsSessions(Path)} reports {@code true} for at least one
     * session, and {@code false} for no sessions.
     */
    @Test
    public final void containsShouldReportExistingKeys() {

        final Path keySingle = mock(Path.class);
        final Path keyMulti = mock(Path.class);
        final Path keyNone = mock(Path.class);

        final Database<?> db1 = mock(Database.class);
        final Database<?> db2 = mock(Database.class);
        final Database<?> db3 = mock(Database.class);

        this.sessions.putDatabase(keySingle, db1);
        this.sessions.putDatabase(keyMulti, db2);
        this.sessions.putDatabase(keyMulti, db3);

        assertTrue(this.sessions.containsSessions(keySingle));
        assertTrue(this.sessions.containsSessions(keyMulti));
        assertFalse(this.sessions.containsSessions(keyNone));
    }

    /**
     * Tests that {@link DatabaseSessionPool#removeDatabase(Path, Database)} will eliminate the
     * entry from the pool.
     */
    @Test
    public final void removeShouldEliminateEntries() {

        final Path key = mock(Path.class);
        final Database<?> db1 = mock(Database.class);

        this.sessions.putDatabase(key, db1);
        this.sessions.removeDatabase(key, db1);

        assertFalse(this.sessions.containsSessions(key));
    }

}
