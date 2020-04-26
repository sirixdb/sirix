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

package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 *
 * <p>
 * Implements an XMLEventReader based on a Queue of XMLEvents.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class QueueEventReader implements XMLEventReader {

  /**
   * List of {@link XMLEvent}s.
   */
  private final Queue<XMLEvent> mEvents;

  /**
   * Constructor.
   * 
   * @param events List of XMLEvents.
   */
  public QueueEventReader(final Queue<XMLEvent> events) {
    mEvents = checkNotNull(events);
  }

  @Override
  public void close() throws XMLStreamException {
    // Do nothing.
  }

  @Override
  public String getElementText() throws XMLStreamException {
    final StringBuffer buffer = new StringBuffer();

    if (mEvents.peek().getEventType() != XMLStreamConstants.START_ELEMENT) {
      throw new XMLStreamException("Current event is not a start tag!");
    }

    final QName root = mEvents.poll().asStartElement().getName();
    int level = 0;
    while (level >= 0 && !(mEvents.peek().isEndElement()
        && mEvents.peek().asEndElement().getName().equals(root))) {
      final XMLEvent event = mEvents.poll();
      switch (event.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          level++;
          break;
        case XMLStreamConstants.CHARACTERS:
          buffer.append(event.asCharacters().getData());
          break;
        case XMLStreamConstants.END_ELEMENT:
          level--;
          break;
        default:
          // Do nothing.
      }
    }
    return buffer.toString();
  }

  @Override
  public Object getProperty(final String paramName) throws IllegalArgumentException {
    // Do nothing.
    return null;
  }

  @Override
  public boolean hasNext() {
    boolean retVal = true;
    if (mEvents.isEmpty()) {
      retVal = false;
    }
    return retVal;
  }

  @Override
  public XMLEvent nextEvent() throws XMLStreamException {
    XMLEvent retVal;
    try {
      retVal = mEvents.poll();
    } catch (final IndexOutOfBoundsException e) {
      throw new NoSuchElementException();
    }
    return retVal;
  }

  @Override
  public XMLEvent nextTag() throws XMLStreamException {
    XMLEvent event = mEvents.poll();
    while (!event.isStartElement() || !event.isEndElement()) {
      if (event.isCharacters() && !event.asCharacters().isWhiteSpace()) {
        throw new XMLStreamException("Encountered anything different from a whitespace!");
      }
      event = mEvents.poll();
    }
    return mEvents.peek();
  }

  @Override
  public XMLEvent peek() throws XMLStreamException {
    XMLEvent retVal = null;
    try {
      retVal = mEvents.peek();
    } catch (final IndexOutOfBoundsException e) {
      retVal = null;
    }
    return retVal;
  }

  /**
   * Just calls nextEvent().
   * 
   * @return null if an XMLStreamException occured
   */
  @Override
  public Object next() {
    Object retVal = null;
    try {
      retVal = nextEvent();
    } catch (final XMLStreamException e) {
      retVal = null;
    }
    return retVal;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported!");
  }

  /**
   * Create a copy, but reset index.
   * 
   * @return copied {@link QueueEventReader}.
   */
  public XMLEventReader copy() {
    return new QueueEventReader(mEvents);
  }

}
