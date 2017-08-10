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

package org.sirix.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import org.slf4j.Logger;

/**
 * Provides some logging helper methods.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class LogWrapper {

	/** Logger. */
	private final Logger mLogger;

	/**
	 * Constructor.
	 * 
	 * @param pLogger logger
	 */
	public LogWrapper(final Logger pLogger) {
		mLogger = checkNotNull(pLogger);
	}

	/**
	 * Log error information.
	 * 
	 * @param pMessage Message to log.
	 * @param pObjects Objects for message
	 */
	public void error(final String pMessage, final Object... pObjects) {
		if (mLogger.isErrorEnabled()) {
			mLogger.error(pMessage, pObjects);
		}
	}

	/**
	 * Log error information.
	 * 
	 * @param pExc Exception to log.
	 */
	public void error(final Exception pExc) {
		if (mLogger.isErrorEnabled()) {
			mLogger.error(pExc.getMessage(), pExc);
		}
	}

	/**
	 * Log debugging information.
	 * 
	 * @param pMessage Message to log.
	 * @param pObjects objects for data
	 */
	public void debug(final String pMessage, final Object... pObjects) {
		if (mLogger.isDebugEnabled()) {
			mLogger.debug(pMessage, pObjects);
		}
	}

	/**
	 * Log information.
	 * 
	 * @param pMessage Message to log.
	 * @param pObjects objects for data
	 */
	public void info(final String pMessage, final Object... pObjects) {
		if (mLogger.isInfoEnabled()) {
			mLogger.info(pMessage, pObjects);
		}
	}

	/**
	 * Warn information.
	 * 
	 * @param pMessage Message to log.
	 * @param pObjects objects for data
	 */
	public void warn(final String pMessage, final Object... pObjects) {
		if (mLogger.isWarnEnabled()) {
			mLogger.warn(pMessage, pObjects);
		}
	}

}
