package org.sirix.index;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnegative;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.node.parser.FragmentHelper;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Stream;

import com.google.common.base.Optional;

/**
 * @author Karsten Schmidt
 * @author Sebastian Baechle
 * 
 */
public final class Indexes implements Materializable {
	public static final QNm INDEXES_TAG = new QNm("indexes");

	private final Set<IndexDef> mIndexes;

	public Indexes() {
		mIndexes = new HashSet<>();
	}

	public synchronized Set<IndexDef> getIndexDefs() {
		return new HashSet<>(mIndexes);
	}

	public synchronized IndexDef getIndexDef(final @Nonnegative int indexNo) {
		checkArgument(indexNo >= 0, "indexNo must be >= 0!");
		for (final IndexDef sid : mIndexes) {
			if (sid.getID() == indexNo) {
				return sid;
			}
		}
		return null;
	}

	@Override
	public synchronized void init(final Node<?> root) throws DocumentException {
		final QNm name = root.getName();
		if (!INDEXES_TAG.equals(name)) {
			throw new DocumentException("Expected tag '%s' but found '%s'",
					INDEXES_TAG, name);
		}

		final Stream<? extends Node<?>> children = root.getChildren();

		try {
			Node<?> child;
			while ((child = children.next()) != null) {
				QNm childName = child.getName();

				if (!childName.equals(IndexDef.INDEX_TAG)) {
					throw new DocumentException(
							"Expected tag '%s' but found '%s'",
							IndexDef.INDEX_TAG, childName);
				}

				IndexDef indexDefinition = new IndexDef();
				indexDefinition.init(child);
				mIndexes.add(indexDefinition);
			}
		} finally {
			children.close();
		}
	}

	@Override
	public synchronized Node<?> materialize() throws DocumentException {
		FragmentHelper helper = new FragmentHelper();
		helper.openElement(INDEXES_TAG);

		for (IndexDef idxDef : mIndexes) {
			helper.insert(idxDef.materialize());
		}

		helper.closeElement();
		return helper.getRoot();
	}

	public synchronized void add(IndexDef indexDefinition) {
		mIndexes.add(indexDefinition);
	}

	public synchronized void removeIndex(final @Nonnegative int indexID) {
		checkArgument(indexID >= 0, "indexID must be >= 0!");
		for (final IndexDef indexDef : mIndexes) {
			if (indexDef.getID() == indexID) {
				mIndexes.remove(indexDef);
				return;
			}
		}
	}

	public Optional<IndexDef> findPathIndex(final Path<QNm> path) throws DocumentException {
		checkNotNull(path);
		try {
			for (final IndexDef index : mIndexes) {
				if (index.isPathIndex()) {
					if (index.getPaths().isEmpty()) {
						return Optional.of(index);
					}
					
					for (final Path<QNm> indexedPath : index.getPaths()) {
						if (indexedPath.matches(path)) {
							return Optional.of(index);
						}
					}
				}
			}
			return Optional.absent();
		} catch (PathException e) {
			throw new DocumentException(e);
		}
	}

	public Optional<IndexDef> findCASIndex(final Path<QNm> path) throws DocumentException {
		checkNotNull(path);
		try {
			for (final IndexDef index : mIndexes) {
				if (index.isCasIndex()) {
					if (index.getPaths().isEmpty()) {
						return Optional.of(index);
					}
					
					for (final Path<QNm> indexedPath : index.getPaths()) {
						if (indexedPath.matches(path)) {
							return Optional.of(index);
						}
					}
				}
			}
			return Optional.absent();
		} catch (PathException e) {
			throw new DocumentException(e);
		}
	}

	public Optional<IndexDef> findNameIndex(final QNm... names) throws DocumentException {
		out: for (final IndexDef index : mIndexes) {
			if (index.isNameIndex()) {
				final Set<QNm> incl = index.getIncluded();
				final Set<QNm> excl = index.getExcluded();
				if (names.length == 0 && incl.isEmpty() && excl.isEmpty()) {
					// Require generic name index
					return Optional.of(index);
				}
				
				for (final QNm name : names) {
					if (!incl.isEmpty() && !incl.contains(name) 
							|| !excl.isEmpty() && excl.contains(name)) {
						continue out;
					}
				}
				return Optional.of(index);
			}
		}
		return Optional.absent();
	}
}