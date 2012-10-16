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

package org.sirix.service.jaxrx.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.access.DatabaseImpl;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.service.jaxrx.implementation.DatabaseRepresentation;
import org.sirix.service.xml.shredder.Insert;

/**
 * This class is responsible to test the {@link WorkerHelper} class.
 * 
 * @author Patrick Lang, Lukas Lewandowski, University of Konstanz
 * 
 */
public class WorkerHelperTest {
    /**
     * The WorkerHelper reference.
     */
    private transient static WorkerHelper workerHelper;
    /**
     * The sirix reference.
     */
    private transient static DatabaseRepresentation sirix;
    /**
     * The resource name.
     */
    private static final transient String RESOURCENAME = "factyTest";
    /**
     * The test file that has to be saved on the server.
     */
    private final static File DBFILE = new File(TestHelper.PATHS.PATH1.getFile(), RESOURCENAME);

    /**
     * The test file that has to be saved on the server.
     */
    private final transient InputStream INPUTFILE = WorkerHelperTest.class.getClass().getResourceAsStream(
        "/factbook.xml");

    /**
     * A simple set up.
     * 
     * @throws FileNotFoundException
     */
    @Before
    public void setUp() throws FileNotFoundException, SirixException {
        TestHelper.closeEverything();
        TestHelper.deleteEverything();
        TestHelper.getDatabase(TestHelper.PATHS.PATH1.getFile());
        workerHelper = WorkerHelper.getInstance();
        sirix = new DatabaseRepresentation(TestHelper.PATHS.PATH1.getFile());
        sirix.shred(INPUTFILE, RESOURCENAME);
    }

    @After
    public void after() throws SirixException {
        TestHelper.closeEverything();
        TestHelper.deleteEverything();
    }

    /**
     * This method tests {@link WorkerHelper#checkExistingResource(File)}
     */
    @Test
    public void testCheckExistingResource() {
        assertEquals("test check existing resource", true, WorkerHelper.checkExistingResource(
            TestHelper.PATHS.PATH1.getFile(), RESOURCENAME));
    }

    /**
     * This method tests {@link WorkerHelper#createStringBuilderObject()}
     */
    @Test
    public void testCreateStringBuilderObject() {
        assertNotNull("test create string builder object", workerHelper.createStringBuilderObject());
    }

    /**
     * This method tests {@link WorkerHelper#serializeXML(Session, OutputStream, boolean, boolean,Long)}
     */
    @Test
    public void testSerializeXML() throws SirixException, IOException {
        final Database database = DatabaseImpl.openDatabase(DBFILE.getParentFile());
        final Session session =
            database.getSession(new SessionConfiguration.Builder(DBFILE.getName()).build());
        final OutputStream out = new ByteArrayOutputStream();

        assertNotNull("test serialize xml", WorkerHelper.serializeXML(session, out, true, true, null));
        session.close();
        database.close();
        out.close();
    }

    /**
     * This method tests {@link WorkerHelper#shredInputStream(NodeWriteTrx, InputStream, Insert)}
     */
    @Test
    public void testShredInputStream() throws SirixException, IOException {
        long lastRevision = sirix.getLastRevision(RESOURCENAME);
        final Database database = DatabaseImpl.openDatabase(DBFILE.getParentFile());
        final Session session =
            database.getSession(new SessionConfiguration.Builder(DBFILE.getName()).build());
        final NodeWriteTrx wtx = session.beginNodeWriteTrx();
        wtx.moveToFirstChild();
        final InputStream inputStream = new ByteArrayInputStream("<testNode/>".getBytes());
        WorkerHelper.shredInputStream(wtx, inputStream, Insert.ASFIRSTCHILD);
        assertEquals("test shred input stream", sirix.getLastRevision(RESOURCENAME), ++lastRevision);
        wtx.close();
        session.close();
        database.close();
        inputStream.close();
    }

    /**
     * This method tests {@link WorkerHelper#closeWTX(boolean, NodeWriteTrx, Session, Database)}
     */
    @Test(expected = IllegalStateException.class)
    public void testClose() throws SirixException {
        Database database = DatabaseImpl.openDatabase(DBFILE.getParentFile());
        Session session = database.getSession(new SessionConfiguration.Builder(DBFILE.getName()).build());
        final NodeWriteTrx wtx = session.beginNodeWriteTrx();

        WorkerHelper.closeWTX(false, wtx, session, database);

        wtx.commit();

        database = DatabaseImpl.openDatabase(DBFILE.getParentFile());
        session = database.getSession(new SessionConfiguration.Builder(DBFILE.getName()).build());
        final NodeReadTrx rtx = session.beginNodeReadTrx();
        WorkerHelper.closeRTX(rtx, session, database);

        rtx.moveTo(11);

    }

}