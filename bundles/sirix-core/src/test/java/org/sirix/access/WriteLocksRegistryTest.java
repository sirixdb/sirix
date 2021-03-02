package org.sirix.access;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests the behavior of {@link WriteLocksRegistry}.
 *
 * @author Joao Sousa
 */
class WriteLocksRegistryTest {

    private WriteLocksRegistry registry;

    @TempDir
    private Path resourcePath;

    @TempDir
    private Path altResourcePath;

    @BeforeEach
    public void setup() {
        this.registry = new WriteLocksRegistry();
    }

    /**
     * Tests that calling {@link WriteLocksRegistry#getWriteLock(Path)} on
     * an empty {@link WriteLocksRegistry} will still return a valid lock.
     */
    @Test
    public final void getOnEmptyRegistryReturnsLock() {
        assertNotNull(this.registry.getWriteLock(this.resourcePath));
    }

    /**
     * Tests that calling {@link WriteLocksRegistry#getWriteLock(Path)} multiple
     * times for the same resource path outputs the same object.
     */
    @Test
    public final void multipleGetsOnSamePathReturnSameLock() {
        final var lock = this.registry.getWriteLock(this.resourcePath);
        final var sameLock = this.registry.getWriteLock(this.resourcePath);

        assertSame(lock, sameLock);
    }

    /**
     * Tests that calling {@link WriteLocksRegistry#getWriteLock(Path)} with different
     * resource paths will lead to different locks.
     */
    @Test
    public final void differentPathsCreateDifferentLocks() {
        final var oneLock = this.registry.getWriteLock(this.resourcePath);
        final var anotherLock = this.registry.getWriteLock(this.altResourcePath);

        assertNotSame(oneLock, anotherLock);
    }

    /**
     * Tests that calling {@link WriteLocksRegistry#getWriteLock(Path)} after
     * {@link WriteLocksRegistry#removeWriteLock(Path)} returns a different lock that the initial one.
     */
    @Test
    public final void getAfterRemoveResetsLock() {
        final var initialLock = this.registry.getWriteLock(this.resourcePath);
        this.registry.removeWriteLock(this.resourcePath);
        final var anotherLock = this.registry.getWriteLock(this.resourcePath);

        assertNotSame(initialLock, anotherLock);
    }

    /**
     * Tests that calling {@link WriteLocksRegistry#removeWriteLock(Path)} on an empty
     * {@link WriteLocksRegistry} does not throw an exception.
     */
    @Test
    public final void removeOnEmptyRegistryDoesNotThrowException() {
        this.registry.removeWriteLock(this.resourcePath);
    }

}
