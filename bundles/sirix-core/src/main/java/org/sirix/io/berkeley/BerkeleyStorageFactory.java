package org.sirix.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandlePipeline;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Berkeley-DB storage factory.
 *
 * @author Johannes
 */
public final class BerkeleyStorageFactory {

	/**
	 * Name for the database.
	 */
	private static final String NAME = "berkeleyDatabase";

	/**
	 * Create a new storage.
	 *
	 * @param resourceConfig the resource configuration
	 * @return the berkeley DB storage
	 * @throws NullPointerException if {@link ResourceConfiguration} is {@code null}
	 * @throws SirixIOException if the storage couldn't be created because of an I/O exception
	 */
	public BerkeleyStorage createStorage(final ResourceConfiguration resourceConfig) {
		try {
			final Path repoFile =
					resourceConfig.mPath.resolve(ResourceConfiguration.ResourcePaths.DATA.getFile());
			if (!Files.exists(repoFile)) {
				Files.createDirectories(repoFile);
			}

			final ByteHandlePipeline byteHandler = checkNotNull(resourceConfig.mByteHandler);

			final DatabaseConfig conf = generateDBConf();
			final EnvironmentConfig config = generateEnvConf();

			final List<Path> path;

			try (final Stream<Path> stream = Files.list(repoFile)) {
				path = stream.collect(toList());
			}

			if (path.isEmpty()
					|| (path.size() == 1 && "sirix.data".equals(path.get(0).getFileName().toString()))) {
				conf.setAllowCreate(true);
				config.setAllowCreate(true);
			}

			final Environment env = new Environment(repoFile.toFile(), config);
			final Database database = env.openDatabase(null, NAME, conf);

			return new BerkeleyStorage(env, database, byteHandler);
		} catch (final DatabaseException | IOException e) {
			throw new SirixIOException(e);
		}
	}

	/**
	 * Generate {@link EnvironmentConfig} reference.
	 *
	 * @return transactional environment configuration
	 */
	private static EnvironmentConfig generateEnvConf() {
		final EnvironmentConfig config = new EnvironmentConfig();
		config.setTransactional(true);
		config.setCacheSize(1024 * 1024);
		return config;
	}

	/**
	 * Generate {@link DatabaseConfig} reference.
	 *
	 * @return transactional database configuration
	 */
	private static DatabaseConfig generateDBConf() {
		final DatabaseConfig conf = new DatabaseConfig();
		conf.setTransactional(true);
		conf.setKeyPrefixing(true);
		return conf;
	}
}
