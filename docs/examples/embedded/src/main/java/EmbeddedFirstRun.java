import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;

public final class EmbeddedFirstRun {
    public static void main(final String[] args) throws Exception {
        final Path dbPath = Files.createTempDirectory("sirix-first-run-").resolve("database");
        Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
        try (final var database = Databases.openJsonDatabase(dbPath)) {
            database.createResource(ResourceConfiguration.newBuilder("message").build());
            try (final var session = database.beginResourceSession("message")) {
                try (final var wtx = session.beginNodeTrx()) {
                    wtx.insertStringValueAsFirstChild("hello");
                    wtx.commit(); // revision 1
                    wtx.setStringValue("updated");
                    wtx.commit(); // revision 2
                }
                try (final var rtx = session.beginNodeReadOnlyTrx(1)) {
                    if (!rtx.moveToFirstChild() || !"hello".equals(rtx.getValue())) {
                        throw new IllegalStateException("Revision 1 did not retain hello");
                    }
                    System.out.println(rtx.getValue()); // hello
                }
                try (final var rtx = session.beginNodeReadOnlyTrx()) {
                    if (!rtx.moveToFirstChild() || !"updated".equals(rtx.getValue())) {
                        throw new IllegalStateException("Latest revision did not retain updated");
                    }
                    System.out.println(rtx.getValue()); // updated
                }
            }
        }
        System.out.println("Database retained at " + dbPath);
    }
}
