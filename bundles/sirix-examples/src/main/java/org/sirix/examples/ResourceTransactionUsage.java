package org.sirix.examples;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;

import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.shredder.XMLShredder;

public final class ResourceTransactionUsage {

	/** User home directory. */
	private static final String USER_HOME = System.getProperty("user.home");

	/** Storage for databases: Sirix data in home directory. */
	private static final File LOCATION = new File(USER_HOME, "sirix-data");

	public static void main(final String[] args) {
		final File file = new File(LOCATION, "db");
		final DatabaseConfiguration config = new DatabaseConfiguration(file);
		if (file.exists()) {
			Databases.truncateDatabase(config);
		}
		Databases.createDatabase(config);
		try (final Database database = Databases.openDatabase(file)) {
			database.createResource(new ResourceConfiguration.Builder("resource",
					config).build());
			try (final Session session = database.getSession(new SessionConfiguration.Builder("resource").build())) {
				final NodeWriteTrx wtx = session.beginNodeWriteTrx();
				wtx.insertSubtreeAsFirstChild(XMLShredder
						.createFileReader(new File(LOCATION, "input.xml")));
				wtx.moveTo(2);
				wtx.moveSubtreeToFirstChild(4).commit();	
				
				final OutputStream out = new ByteArrayOutputStream();
				new XMLSerializer.XMLSerializerBuilder(session, out).prettyPrint().build().call();
				
				System.out.println(out);
			}
		} catch (final SirixException | IOException | XMLStreamException e) {
			// LOG or do anything, the database is closed properly.
		}
	}
}
