package org.sirix.fs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * Process file system attributes.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class ProcessFileSystemAttributes implements Visitor<XdmNodeWriteTrx> {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(ProcessFileSystemAttributes.class));

	@Override
	public void processDirectory(final XdmNodeWriteTrx transaction, final Path dir,
			final Optional<BasicFileAttributes> attrs) {
	}

	@Override
	public void processFile(final XdmNodeWriteTrx trx, Path path,
			final Optional<BasicFileAttributes> attrs) {
		if (Files.exists(path)) {
			final String file = path.getFileName().toString();
			final int index = file.lastIndexOf('.');
			if (index > 0) {
				final String suffix = file.substring(index + 1);
				if (!suffix.isEmpty()) {
					try {
						trx.insertAttribute(new QNm("suffix"), file.substring(index + 1));
						trx.moveToParent();
					} catch (SirixException e) {
						LOGWRAPPER.error(e.getMessage(), e);
					}
				}
			}
		}
	}
}
