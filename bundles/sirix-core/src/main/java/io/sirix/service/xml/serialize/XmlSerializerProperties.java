/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.service.xml.serialize;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * <p>
 * XmlSerializer properties.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class XmlSerializerProperties {

	// ============== Class constants. =================

	/** NO maps to false. */
	private static final boolean NO = false;

	// ============ Shredding constants. ===============

	/** Serialize TT-ID: yes/no. */
	public static final Object[] S_ID = {"serialize-id", NO};

	/** Serialization parameter: yes/no. */
	public static final Object[] S_INDENT = {"indent", NO};

	/** Specific serialization parameter: number of spaces to indent. */
	public static final Object[] S_INDENT_SPACES = {"indent-spaces", 2};

	/** Serialize REST: yes/no. */
	public static final Object[] S_REST = {"serialize-rest", NO};

	/** Serialize XML declaration: yes/no. */
	public static final Object[] S_XMLDECL = {"xmldecl", NO};

	/** Properties. */
	private final ConcurrentMap<String, Object> mProps = new ConcurrentHashMap<>();

	/**
	 * Constructor.
	 */
	public XmlSerializerProperties() {
		try {
			for (final Field f : getClass().getFields()) {
				final Object obj = f.get(null);
				if (!(obj instanceof final Object[] arr)) {
					continue;
				}
				mProps.put(arr[0].toString(), arr[1]);
			}
		} catch (final IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get properties map.
	 *
	 * @return ConcurrentMap with key/value property pairs.
	 */
	public ConcurrentMap<String, Object> getProps() {
		return mProps;
	}

}
