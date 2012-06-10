/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.encryption;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.axis.AbsAxis;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTEncryptionException;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.service.xml.xpath.XPathStringChecker;

public class EncryptionMainTest {

  private static final String XML = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "auction.xml";

  private static Holder holder;

//  @Before
//  public void setUp() throws Exception {
//    TestHelper.deleteEverything();
//
//    new EncryptionController().clear();
//    new EncryptionController().setEncryptionOption(true);
//    new EncryptionController().init();
//    XMLShredder.main(XML, PATHS.PATH1.getFile().getAbsolutePath());
//    holder = Holder.generateRtx();
//
//    new EncryptionController().setUser((holder.getSession().getUser()));
//  }
//
//  @After
//  public void tearDown() throws AbsTTException {
//    holder.close();
//    new EncryptionController().setEncryptionOption(false);
//    // new EncryptionController().print();
//    TestHelper.closeEverything();
//  }
//
//  @Test
//  @Ignore
//  public void executeEncryption() throws AbsTTException, TTEncryptionException {
//
//    String[] nodes = new String[] {
//      "Inf", "Disy", "TT", "Group1"
//    };
//    EncryptionOperator op = new EncryptionOperator();
//    op.join("ROOT", nodes);
//
//    String[] nodes2 = new String[] {
//      "BaseX", "Group2"
//    };
//    EncryptionOperator op2 = new EncryptionOperator();
//    op2.join("Inf", nodes2);
//
//    String[] nodes3 = new String[] {
//      "RZ", "Waldvogel"
//    };
//    EncryptionOperator op3 = new EncryptionOperator();
//    op3.join("ROOT", nodes3);
//
//    String[] nodes4 = new String[] {
//      "Waldvogel"
//    };
//    EncryptionOperator op4 = new EncryptionOperator();
//    op4.join("TT", nodes4);
//
//    EncryptionOperator op10 = new EncryptionOperator();
//    op10.leave("Group2", new String[] {
//      "BaseX"
//    });
//
//    EncryptionOperator op9 = new EncryptionOperator();
//    op9.leave("Waldvogel", new String[] {});
//
//    AbsAxis axis = new XPathAxis(holder.getRtx(), "/site/people/person[@id=\"person0\"]/name/text()");
//
//    XPathStringChecker.testIAxisConventions(axis, new String[] {
//      "Sinisa Farrel"
//    });
//
//  }

}
