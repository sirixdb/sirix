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

package org.treetank.service.xml.shredder;

/**
 * <h1>XMLImport</h1>
 * 
 * <p>
 * Import of temporal data, which is either available as exactly one file which includes several revisions or
 * many files, whereas one file represents exactly one revision. Beforehand one or more <code>RevNode</code>
 * have to be instanciated.
 * </p>
 * 
 * <p>
 * Usage example:
 * 
 * <code><pre>
 * final File file = new File("database.xml");
 * new XMLImport(file).check(new RevNode(new QName("timestamp")));
 * </pre></code>
 * 
 * <code><pre>
 * final List<File> list = new ArrayList<File>();
 * list.add("rev1.xml");
 * list.add("rev2.xml");
 * ...
 * new XMLImport(file).check(new RevNode(new QName("timestamp")));
 * </pre></code>
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLImport {

  // /**
  // * Log wrapper for better output.
  // */
  // private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XMLImport.class));
  //
  // /** {@link Session}. */
  // private transient ISession mSession;
  //
  // /** {@link WriteTransaction}. */
  // private transient IWriteTransaction mWtx;
  //
  // /** Path to Treetank storage. */
  // private transient File mTT;
  //
  // /** Log helper. */
  // private transient LogWrapper mLog;
  //
  // /** Revision nodes {@link RevNode}. */
  // private transient List<RevNode> mNodes;
  //
  // /** File to shredder. */
  // private transient File mXml;
  //
  // /**
  // * Constructor.
  // *
  // * @param mTt
  // * Treetank file.
  // */
  // public XMLImport(final File mTt) {
  // try {
  // mTT = mTt;
  // mNodes = new ArrayList<RevNode>();
  // final IDatabase database = Database.openDatabase(mTT);
  // mSession = database.getSession();
  // } catch (final TreetankException e) {
  // LOGWRAPPER.error(e);
  // }
  // }
  //
  // @SuppressWarnings("unchecked")
  // @Override
  // public void check(final Object mDatabase, final Object mObj) {
  // try {
  // // Setup executor service.
  // final ExecutorService execService =
  // Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  //
  // if (mDatabase instanceof File) {
  // // Single file.
  // mXml = (File)mDatabase;
  // if (mObj instanceof RevNode) {
  // mNodes.add((RevNode)mObj);
  // } else if (mObj instanceof List<?>) {
  // mNodes = (List<RevNode>)mObj;
  // }
  // execService.submit(this);
  // } else if (mDatabase instanceof List<?>) {
  // // List of files.
  // final List<?> files = (List<?>)mDatabase;
  // if (mObj instanceof RevNode) {
  // mNodes.add((RevNode)mObj);
  // for (final File xmlFile : files.toArray(new File[files.size()])) {
  // mXml = xmlFile;
  // execService.submit(this);
  // }
  // } else if (mObj instanceof List<?>) {
  // mNodes = (List<RevNode>)mObj;
  // for (final File xmlFile : files.toArray(new File[files.size()])) {
  // mXml = xmlFile;
  // execService.submit(this);
  // }
  // }
  // }
  //
  // // Shutdown executor service.
  // execService.shutdown();
  // execService.awaitTermination(10, TimeUnit.MINUTES);
  // } catch (final InterruptedException e) {
  // LOGWRAPPER.error(e);
  // } finally {
  // try {
  // mWtx.close();
  // mSession.close();
  // Database.forceCloseDatabase(mTT);
  // } catch (final TreetankException e) {
  // LOGWRAPPER.error(e);
  // }
  // }
  // }
  //
  // @Override
  // public Void call() throws Exception {
  // // Setup StAX parser.
  // final XMLEventReader reader = XMLShredder.createReader(mXml);
  // final XMLEvent mEvent = reader.nextEvent();
  // final IWriteTransaction wtx = mSession.beginWriteTransaction();
  //
  // // Parse file.
  // boolean first = true;
  // do {
  // mLog.debug(mEvent.toString());
  //
  // if (XMLStreamConstants.START_ELEMENT == mEvent.getEventType()
  // && checkTimestampNodes((StartElement)mEvent, mNodes.toArray(new RevNode[mNodes.size()]))) {
  // // Found revision node.
  // wtx.moveToDocumentRoot();
  //
  // if (first) {
  // first = false;
  //
  // // Initial shredding.
  // new XMLShredder(wtx, reader, true).call();
  // } else {
  // // Subsequent shredding.
  // // new XMLUpdateShredder(wtx, reader, true, true).call();
  // }
  // }
  //
  // reader.nextEvent();
  // } while (reader.hasNext());
  // return null;
  // }
  //
  // /**
  // * Check if current start element matches one of the timestamp/revision
  // * nodes.
  // *
  // * @param mEvent
  // * Current parsed start element.
  // * @param mTsns
  // * Timestamp nodes.
  // * @return true if they match, otherwise false.
  // */
  // private boolean checkTimestampNodes(final StartElement mEvent, final RevNode... mTsns) {
  // boolean mRetVal = false;
  //
  // for (final RevNode tsn : mTsns) {
  // tsn.toString();
  // // TODO
  // }
  //
  // return mRetVal;
  // }
  //
  // /**
  // * <h1>RevNode</h1>
  // *
  // * <p>
  // * Container which holds the full qualified name of a "timestamp" node.
  // * </p>
  // *
  // * @author Johannes Lichtenberger, University of Konstanz
  // *
  // */
  // final static class RevNode {
  // /** QName of the node, which has the timestamp attribute. */
  // private transient final QName mQName;
  //
  // /** Attribute which specifies the timestamp value. */
  // private transient final Attribute mAttribute;
  //
  // /**
  // * Constructor.
  // *
  // * @param mQName
  // * Full qualified name of the timestamp node.
  // */
  // public RevNode(final QName mQName) {
  // this(mQName, null);
  // }
  //
  // /**
  // * Constructor.
  // *
  // * @param mQName
  // * Full qualified name of the timestamp node.
  // * @param mAtt
  // * Attribute which specifies the timestamp value.
  // */
  // public RevNode(final QName mQName, final Attribute mAtt) {
  // this.mQName = mQName;
  // this.mAttribute = mAtt;
  // }
  //
  // /**
  // * Get mQName.
  // *
  // * @return the full qualified name.
  // */
  // public QName getQName() {
  // return mQName;
  // }
  //
  // /**
  // * Get attribute.
  // *
  // * @return the attribute.
  // */
  // public Attribute getAttribute() {
  // return mAttribute;
  // }
  // }
}
