package io.sirix.tutorial.xml;

import io.sirix.access.Databases;
import io.sirix.api.ResourceSession;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.temporal.PastAxis;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.immutable.xml.ImmutableText;

import io.sirix.tutorial.Constants;

public final class CreateVersionedXmlResourceAndQuery {

	public static void main(String[] args) {
		CreateVersionedXmlResource.createXmlDatabaseWithVersionedResource();

		queryXmlDatabaseWithVersionedResource();
	}

	static void queryXmlDatabaseWithVersionedResource() {
		final var databaseFile = Constants.SIRIX_DATA_LOCATION.resolve("xml-database-versioned");

		try (final var database = Databases.openXmlDatabase(databaseFile)) {
			try (final var manager = database.beginResourceSession("resource");
					// Starts a read only transaction on the most recent revision.
					final var rtx = manager.beginNodeReadOnlyTrx()) {

				final var axis = VisitorDescendantAxis.newBuilder(rtx).includeSelf()
						.visitor(new MyXmlNodeVisitor(manager, rtx)).build();

				while (axis.hasNext()) {
					axis.nextLong();
				}
			}
		}

		System.out.println("Database with versioned resource created.");
	}

	private static final class MyXmlNodeVisitor implements XmlNodeVisitor {

		private final ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> manager;

		private final XmlNodeReadOnlyTrx trx;

		public MyXmlNodeVisitor(final ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> manager,
				final XmlNodeReadOnlyTrx rtx) {
			this.manager = manager;
			this.trx = rtx;
		}

		@Override
		public VisitResult visit(ImmutableElement node) {
			System.out.println("Element (most recent revision " + trx.getRevisionNumber() + "):" + node.getName());

			// Axis to iterate over the node in past revisions (if the node existed back
			// then).
			final var pastAxis = new PastAxis<>(manager, trx);
			pastAxis.forEachRemaining((trx) -> System.out
					.println("Element in the past (revision " + trx.getRevisionNumber() + ": " + trx.getName()));

			return VisitResultType.CONTINUE;
		}

		@Override
		public VisitResult visit(ImmutableText node) {
			System.out.println("Text (most recent revision " + trx.getRevisionNumber() + "):" + node.getValue());

			// Axis to iterate over the node in past revisions (if the node existed back
			// then).
			final var pastAxis = new PastAxis<>(manager, trx);
			pastAxis.forEachRemaining((trx) -> System.out
					.println("Text in the past (revision " + trx.getRevisionNumber() + ": " + trx.getValue()));

			return VisitResultType.CONTINUE;
		}
	}
}
