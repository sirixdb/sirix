/**
 * Copyright (c) 2010, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */
package org.sirix.gui;

import java.io.File;

import javax.swing.filechooser.FileFilter;

/**
 * XML file filter.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLFileFilter extends FileFilter {
	/** File extensions which can be shreddered. */
	private static final String[] ALLOWEDEXTENSIONS = new String[] { "xml",
			"xhtml", "svg", "txt" };

	/** {@inheritDoc} */
	@Override
	public boolean accept(final File paramFile) {
		boolean retVal = false;

		for (final String extension : ALLOWEDEXTENSIONS) {
			final String fileName = paramFile.getName().toLowerCase();
			if (fileName.endsWith(extension) || fileName.indexOf('.') == -1) {
				retVal = true;
				break;
			}
		}

		return retVal;
	}

	/** {@inheritDoc} */
	@Override
	public String getDescription() {
		return "XML file filter";
	}
}
