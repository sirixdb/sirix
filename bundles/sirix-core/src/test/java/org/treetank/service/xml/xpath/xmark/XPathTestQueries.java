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

package org.treetank.service.xml.xpath.xmark;

public class XPathTestQueries {

  // final private static String XMLFILE = "src" + File.separator + "test" +
  // File.separator + "resources"
  // + File.separator + "factbook.xml";
  //
  // final private static File OUTPUTFILE = new File(File.separatorChar +
  // "tmp" + File.separatorChar + "tt"
  // + File.separatorChar + "factbook.tnk");
  //
  // final private static String QUERY = "//country/name";
  //
  // public void executeQuery() {
  // IReadTransaction rtx = null;
  // IDatabase db = null;
  // ISession session = null;
  //
  // try {
  // db = FileDatabase.openDatabase(OUTPUTFILE);
  // session = db.getSession(new SessionConfiguration());
  // rtx = session.beginReadTransaction();
  //
  // AbsAxis axis = new XPathAxis(rtx, QUERY);
  // int resultSize = 0;
  // while (axis.hasNext()) {
  // axis.next();
  // resultSize++;
  // System.out.println(rtx.getItem().getRawValue());
  // }
  // System.out.println("Gefundene Ergebnisse: " + resultSize);
  // rtx.close();
  // session.close();
  //
  // } catch (AbsTTException e) {
  // e.printStackTrace();
  // }
  //
  // }
  //
  // public void shredXML() {
  // long startTime = System.currentTimeMillis();
  //
  // IWriteTransaction wtx = null;
  // IDatabase database = null;
  // ISession session = null;
  //
  // try {
  //
  // final Properties dbProps = new Properties();
  // dbProps.setProperty(EDatabaseSetting.REVISION_TO_RESTORE.name(), "1");
  // final DatabaseConfiguration conf = new DatabaseConfiguration(OUTPUTFILE,
  // dbProps);
  //
  // Database.createDatabase(conf);
  // database = Database.openDatabase(OUTPUTFILE);
  // session = database.getSession();
  // wtx = session.beginWriteTransaction();
  // wtx.moveToDocumentRoot();
  // final boolean exist = wtx.moveToFirstChild();
  // if (exist) {
  // wtx.remove();
  // wtx.commit();
  // }
  //
  // final XMLEventReader reader = createReader(new File(XMLFILE));
  // final XMLShredder shredder = new XMLShredder(wtx, reader,
  // EShredderInsert.ADDASFIRSTCHILD);
  // shredder.call();
  //
  // wtx.close();
  // session.close();
  // database.close();
  // long endTime = System.currentTimeMillis();
  //
  // System.out
  // .println("Datenbank in " + ((endTime - startTime) / 1000) +
  // " sec erfolgreich angelegt");
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }
  //
  // public static synchronized XMLEventReader createReader(final File
  // paramFile) throws IOException,
  // XMLStreamException {
  // final XMLInputFactory factory = XMLInputFactory.newInstance();
  // final InputStream in = new FileInputStream(paramFile);
  // return factory.createXMLEventReader(in);
  // }
  //
  // public static void main(String[] args) {
  //
  // XPathTestQueries xptq = new XPathTestQueries();
  // xptq.shredXML();
  // xptq.executeQuery();
  // }

}
