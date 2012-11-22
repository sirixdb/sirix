package org.sirix.node;

/**
 * Determines the kind of value.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public enum ValueKind {
	/** Text value. */
	TEXT {
		@Override
		public Kind getKind() {
			return Kind.TEXT_VALUE;
		}
	},

	/** Attribute value. */
	ATTRIBUTE {
		@Override
		public Kind getKind() {
			return Kind.ATTRIBUTE_VALUE;
		}
	};

	/**
	 * Get the kind of value.
	 * 
	 * @return the kind of value
	 */
	public abstract Kind getKind();
}
