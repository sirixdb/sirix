/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.fault;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.StorageProvider;
import io.sirix.io.StorageProviders;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import io.sirix.io.fault.FaultInjectingWriter.SimulatedCrashException;

/**
 * Test-only {@link IOStorage} that wraps the REAL built-in {@code FILE_CHANNEL} storage and hands
 * out {@link FaultInjectingWriter}s, so a live commit of a real resource session can be made to
 * fail at a chosen {@link FaultInjectingWriter.Point}.
 *
 * <p>
 * Installation mirrors {@code PowerLossRecordingStorage}: {@link #install(Path)} registers a
 * {@link StorageProvider} named {@code FILE_CHANNEL} in the {@link StorageProviders} registry,
 * which {@code StorageType.getStorage} consults BEFORE the built-in types. Resources under the
 * registered root get an injecting storage; every other path gets the untouched built-in storage,
 * so unrelated opens in the same JVM are unaffected. {@link #uninstall()} removes the provider
 * again.
 *
 * <p>
 * Readers are never wrapped — the durability contracts under test turn on write ordering only. Each
 * {@link IOStorage#createWriter()} hands back a freshly wrapped writer and publishes it as
 * {@link #lastWriter()}, which is how a test arms an injection point for the writer a transaction
 * has already taken: arm after {@code beginNodeTrx()} and the very next commit runs into it.
 *
 * <p>
 * Not thread-safe across concurrently opened write transactions: {@link #lastWriter()} is a single
 * slot, so a test that arms it must own the only writer being created.
 */
public final class FaultInjectingStorage implements IOStorage {

  private final IOStorage delegate;

  private FaultInjectingStorage(final IOStorage delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public Reader createReader() {
    return delegate.createReader();
  }

  @Override
  public Writer createWriter() {
    if (FAIL_NEXT_WRITER.compareAndSet(true, false)) {
      throw new SimulatedCrashException("successor writer creation");
    }
    final FaultInjectingWriter writer = new FaultInjectingWriter(delegate.createWriter());
    LAST_WRITER.set(writer);
    return writer;
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  @Override
  public ByteHandler getByteHandler() {
    return delegate.getByteHandler();
  }

  @Override
  public RevisionIndexHolder getRevisionIndexHolder() {
    return delegate.getRevisionIndexHolder();
  }

  // ---- provider installation ----

  private static final String PROVIDER_NAME = "FILE_CHANNEL";

  private static final AtomicBoolean FAIL_NEXT_WRITER = new AtomicBoolean();

  public static void failNextWriterCreation() {
    FAIL_NEXT_WRITER.set(true);
  }

  public static boolean isWriterCreationFailureArmed() {
    return FAIL_NEXT_WRITER.get();
  }

  private static final AtomicReference<Path> INJECTING_ROOT = new AtomicReference<>();

  /** The provider instance currently registered, for exact removal on uninstall. */
  private static final AtomicReference<StorageProvider> INSTALLED = new AtomicReference<>();

  private static final AtomicReference<FaultInjectingWriter> LAST_WRITER = new AtomicReference<>();

  /**
   * Registers the injecting provider; resources whose {@code resourcePath} lives under
   * {@code injectingRoot} get injecting storages, everything else gets the untouched built-in
   * FILE_CHANNEL storage.
   *
   * @param injectingRoot the directory whose resources get injecting storages
   * @throws IllegalStateException if a FILE_CHANNEL provider is already registered
   */
  public static void install(final Path injectingRoot) {
    INJECTING_ROOT.set(resolveReal(injectingRoot));
    LAST_WRITER.set(null);

    final StorageProvider provider = new StorageProvider() {
      @Override
      public String getName() {
        return PROVIDER_NAME;
      }

      @Override
      public boolean isAvailable() {
        return true;
      }

      @Override
      public IOStorage createStorage(final ResourceConfiguration resourceConfig) {
        final Path root = INJECTING_ROOT.get();
        final IOStorage builtIn = StorageType.FILE_CHANNEL.getInstance(resourceConfig);
        if (root != null && resolveReal(resourceConfig.resourcePath).startsWith(root)) {
          return new FaultInjectingStorage(builtIn);
        }
        return builtIn;
      }
    };

    final Map<String, StorageProvider> providers = providersMap();
    final StorageProvider previous = providers.putIfAbsent(PROVIDER_NAME, provider);
    if (previous != null) {
      throw new IllegalStateException(
          "A FILE_CHANNEL storage provider is already registered: " + previous + " — refusing to shadow it");
    }
    INSTALLED.set(provider);
  }

  /** Removes the provider again; idempotent. */
  public static void uninstall() {
    final StorageProvider installed = INSTALLED.getAndSet(null);
    if (installed != null) {
      providersMap().remove(PROVIDER_NAME, installed);
    }
    FAIL_NEXT_WRITER.set(false);
    INJECTING_ROOT.set(null);
    LAST_WRITER.set(null);
  }

  /**
   * The most recently created injecting writer.
   *
   * @return that writer
   * @throws IllegalStateException if no writer has been created since {@link #install(Path)}
   */
  public static FaultInjectingWriter lastWriter() {
    final FaultInjectingWriter writer = LAST_WRITER.get();
    if (writer == null) {
      throw new IllegalStateException("No injecting writer has been created yet");
    }
    return writer;
  }

  /**
   * The path with every symbolic link in it resolved, so that a root and a resource path can be
   * compared as prefixes. {@link Path#normalize()} alone cannot do this: it is purely lexical, and
   * macOS hands out temporary directories under {@code /var}, which is a symlink to
   * {@code /private/var}. {@link io.sirix.access.Databases} stores the resolved form, so a root left
   * unresolved never prefixes it and every resource would silently get a non-injecting storage.
   *
   * <p>
   * The path need not exist yet: this resolves the nearest existing ancestor and re-appends the
   * remainder, the same way {@code MutationGate} keys a database path.
   *
   * @param path the path to resolve
   * @return the resolved absolute path, or its lexically normalized form if resolution fails
   */
  private static Path resolveReal(final Path path) {
    Path ancestor = Objects.requireNonNull(path, "path").toAbsolutePath();
    Path remaining = ancestor.getFileSystem().getPath("");
    try {
      while (true) {
        try {
          return ancestor.toRealPath().resolve(remaining).normalize();
        } catch (final NoSuchFileException missing) {
          final Path parent = ancestor.getParent();
          if (parent == null) {
            throw missing;
          }
          remaining = ancestor.getFileName().resolve(remaining);
          ancestor = parent;
        }
      }
    } catch (final IOException failure) {
      return path.toAbsolutePath().normalize();
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, StorageProvider> providersMap() {
    try {
      final Field field = StorageProviders.class.getDeclaredField("PROVIDERS");
      field.setAccessible(true);
      return (Map<String, StorageProvider>) field.get(null);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot access StorageProviders.PROVIDERS registry", e);
    }
  }
}
