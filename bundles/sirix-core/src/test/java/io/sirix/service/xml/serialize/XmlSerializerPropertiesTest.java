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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import io.sirix.exception.SirixException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.XmlTestHelper;

public class XmlSerializerPropertiesTest {

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testXMLSerializerProp() {
    final XmlSerializerProperties props = new XmlSerializerProperties();

    final ConcurrentHashMap<String, Object> map =
        (ConcurrentHashMap<String, Object>) props.getProps();
    final Enumeration<String> keys = map.keys();

    assertNotNull(keys);

    while (keys.hasMoreElements()) {
      final String key = keys.nextElement();

      if (key.equals(XmlSerializerProperties.S_ID[0])) {
        assertEquals(false, map.get(XmlSerializerProperties.S_ID[0]));
      } else if (key.equals(XmlSerializerProperties.S_REST[0])) {
        assertEquals(false, map.get(XmlSerializerProperties.S_REST[0]));
      } else if (key.equals(XmlSerializerProperties.S_INDENT[0])) {
        assertEquals(false, map.get(XmlSerializerProperties.S_INDENT[0]));
      } else if (key.equals(XmlSerializerProperties.S_INDENT_SPACES[0])) {
        assertSame(2, map.get(XmlSerializerProperties.S_INDENT_SPACES[0]));
      } else if (key.equals(XmlSerializerProperties.S_XMLDECL[0])) {
        assertEquals(false, map.get(XmlSerializerProperties.S_XMLDECL[0]));
      }
    }
  }
}
