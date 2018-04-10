[![Build Status](https://travis-ci.org/sirixdb/sirix.png)](https://travis-ci.org/sirixdb/sirix)
[![Coverage Status](https://coveralls.io/repos/sirixdb/sirix/badge.svg)](https://coveralls.io/r/sirixdb/sirix)
[![CodeFactor](https://www.codefactor.io/repository/github/sirixdb/sirix/badge)](https://www.codefactor.io/repository/github/sirixdb/sirix)

# Sirix - Beyond Versioning of Persistent Trees

"Remember that you're lucky, even if you don't think you are, because there's always something that you can be thankful for." - Esther Grace Earl (http://tswgo.org)

## Storage system for temporal data
Sirix is a storage system, which brings versioning to a sub-file granular level taking full advantage of flash based disks as for instance SSDs. As such per revision as well as per page deltas are stored. Currently we provide a low-level API to store key (long) / value pairs as well as an XML layer on top of it. Our goal is to provide a seamless integration of a native JSON layer besides the XML layer, that is extending the XQuery Data Model (XDM) with other node types (support for JSONiq through the XQuery processor Brackit). We aim to provide

1. The current revision of the resource or any subset thereof;
2. The full revision history of the resource or any subset thereof;
3. The full modification history of the resource or any subset thereof.

We not only support all XPath axis (as well as a few more) to query a resource in one revision but also novel temporal axis which allow the navigation in time, A transaction on a resource can be started either by specifying a revision number to open or by a given point in time. The latter starts a transaction on the revision number which was committed closest to the given timestamp.

## Simple Example
<pre><code>final Path file = Paths.get("sirix-database");

// Create the database.
final DatabaseConfiguration config = new DatabaseConfiguration(file);
Databases.createDatabase(config);

// Open the database.
try (final Database database = Databases.openDatabase(file)) {
  // Create a resource in the database with the name "resource1".
  database.createResource(new ResourceConfiguration.Builder("resource1", config).build());

  try (
      // Start a resource manager on the given resource.
      final ResourceManager resource = database.getResourceManager(
          new ResourceManagerConfiguration.Builder("resource").build());
      // Start the single read/write transaction.
      final XdmNodeWriteTrx wtx = resource.beginNodeWriteTrx()) {
    // Import an XML-document.
    wtx.insertSubtreeAsFirstChild(XMLShredder.createFileReader(LOCATION.resolve("input.xml")));
    
    // Move to the node which automatically got the node-key 2 from Sirix during the import of the XML-document.
    wtx.moveTo(2);
    
    // Then move the subtree located at this node to the first child of node 4.
    wtx.moveSubtreeToFirstChild(4)
    
    // Commit revision 1.
    wtx.commit();
    
    // Reuse transaction handle and insert an element to the first child the current transaction cursor resides.
    wtx.insertElementAsFirstChild(new QName("foo"));
    
    // Commit revision 2.
    wtx.commit();

    // Serialize the revision back to XML.
    final OutputStream out = new ByteArrayOutputStream();
    new XMLSerializer.XMLSerializerBuilder(resource, out).prettyPrint().build().call();

    System.out.println(out);
  }
} catch (final SirixException | IOException | XMLStreamException e) {
  // LOG or do anything, the database is closed properly.
}
</code></pre>

There are N reading transactions as well as one write-transaction permitted on a resource.

A read-only transaction can be opened through:

<pre><code>final XdmNodeReadTrx rtx = resource.beginNodeReadTrx()</code></pre>

This starts a transaction on the most recent revision.

<pre><code>final XdmNodeReadTrx rtx = resource.beginNodeReadTrx(1)</code></pre>

This starts a transaction at revision 1.

<pre><code>final LocalDateTime time = LocalDateTime.of(2018, Month.APRIL, 28, 23, 30);
final XdmNodeReadTrx rtx = resource.beginNodeReadTrx(time.toInstant())</code></pre>

This starts a transaction on the revision, which has been committed at the closest timestamp to the given point in time.

There are also several ways to start the single write-transaction:

<pre><code>
  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @param trx the transaction to use
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx();

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final @Nonnegative int maxNodes);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final TimeUnit timeUnit, final int maxTime);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final @Nonnegative int maxNodes, final TimeUnit timeUnit,
      final int maxTime);
</code></pre>

## Simple XQuery Examples 
Test if fragments of the resource are not present in the past. In this example they are appended to a node in the most recent revision and stored in a subsequent revision)
<pre><code>(* Loading document: *)
bit:load('mydoc.xml', '/tmp/sample8721713104854945959.xml')

(* Update loaded document: *)
let $doc := doc('mydoc.xml')
INSERT NODES &lt;a&gt;&lt;b/&gt;test&lt;/a&gt; INTO $doc/log

(* intermediate explicit commit *)
sdb:commit($doc)

(* Query loaded document: *)
doc('mydoc.xml')/log/all-time::*
(* First version: *)
&lt;log tstamp="Fri Jun 14 07:59:08 CEST 2013" severity="low"&gt;
  &lt;src&gt;192.168.203.49&lt;/src&gt;
  &lt;msg&gt;udic fyllwx abrjxa apvd&lt;/msg&gt;
&lt;/log&gt;
(* Second version: *)
&lt;log tstamp="Fri Jun 14 07:59:08 CEST 2013" severity="low"&gt;
  &lt;src&gt;192.168.203.49&lt;/src&gt;
  &lt;msg&gt;udic fyllwx abrjxa apvd&lt;/msg&gt;
  &lt;a&gt;
    &lt;b/&gt;
    test
  &lt;/a&gt;
&lt;/log&gt;

(* Query loaded document (nodes, which are children of the log-element but did not exist in the past): *)
(* The second revision is initially loaded *)
doc('mydoc.xml', 2)/log/*[not(past::*)]
&lt;a&gt;
  &lt;b/&gt;
  test
&lt;/a&gt;
</code></pre>

Creation of a path index for all paths (note that we already can keep a path summary):
<pre><code>// Create and commit path index on all elements.
try (final DBStore store = DBStore.newBuilder().build()) {
  final QueryContext ctx3 = new QueryContext(store);
  final XQuery q = new XQuery(new SirixCompileChain(store),
      "let $doc := sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
          + "let $stats := sdb:create-path-index($doc, '//*') "
          + "return <rev>{sdb:commit($doc)}</rev>");
  q.serialize(ctx3, System.out);
}
</code></pre>

Many more examples of creating name indexes, content and structure indexes and how to query them can be found in the examples module.

Have a look into the wiki for examples regarding a lower level (really powerful) cursor based API to navigate/and or modify  the tree structure or to navigate in time.

A lot of the ideas still stem from the Ph.D. thesis of Marc Kramis: Evolutionary Tree-Structured Storage: Concepts, Interfaces, and Applications

http://www.uni-konstanz.de/mmsp/pubsys/publishedFiles/Kramis2014.pdf

As well as from Sebastian Graft's work and thesis:

https://kops.uni-konstanz.de/handle/123456789/27250


## Maven artifacts
At this stage of development please use the latest SNAPSHOT artifacts from https://oss.sonatype.org/content/repositories/snapshots/com/github/sirixdb/sirix/.
Just add the following repository section to your POM file:
<pre><code>&lt;repository&gt;
  &lt;id&gt;sonatype-nexus-snapshots&lt;/id&gt;
  &lt;name&gt;Sonatype Nexus Snapshots&lt;/name&gt;
  &lt;url&gt;https://oss.sonatype.org/content/repositories/snapshots&lt;/url&gt;
  &lt;releases&gt;
    &lt;enabled&gt;false&lt;/enabled&gt;
  &lt;/releases&gt;
  &lt;snapshots&gt;
    &lt;enabled&gt;true&lt;/enabled&gt;
  &lt;/snapshots&gt;
&lt;/repository&gt;
</code></pre>

Maven artifacts are deployed to the central maven repository (however please use the SNAPSHOT-variants as of now). Currently the following artifacts are available:

Core project:
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-core&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

Examples:
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-example&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

Brackit binding:
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-xquery&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency>
</pre></code>

Other modules are currently not available (namely the GUI, the distributed package as well as an outdated Saxon binding as well as a RESTful-API which currently is refactored).

## Introduction
Do you have to handle irregular data without knowing the schema before storing the data? You currently store this data in a relational DBMS? Maybe a tree-structured (XML) storage system much better suits your needs as it doesn't require a predefined schema before even knowing the structure of the data which has to be persisted.
Do you have to store a snapshot of this irregular data? Furthermore questions such as 

- How do we store snapshots of time varying data effectively and efficiently?
- How do we know which data has been modified ever since a specified snapshot/revision?
- How do we store the differences between two XML documents? Is the storage system able to determine the differences itself?
- Which item has been sold the most during the last month/year?
- Which item has the most sold copies?
- Which items have been added?
- Which items have been deleted?

Sirix might be a good fit if you have to answer any of these questions as it stores data efficiently and effectively. 
Furthermore Sirix handles the import of differences between a Sirix-resource and a new version thereof in the form of 
an XML-document (soon JSON as well). Thus, an algorithm takes care of determining the differences and transforms 
the stored resource into a new snapshot/revision/version, which is the same as the new XML document once 
the newest revision is serialized (despite whitespace). Despite this, we also support the import of a series of snapshots of
temporal data, whereas the detection of the differences is completely up to Sirix. Specifying unique node-IDs to match pairs
of nodes is not required. 

Once several (for instance at the very minimum two) versions of resources have been stored in Sirix it's possible to determine
the differences of subtrees or the whole resource/tree-structure.

Furthermore you are encouraged to navigate and query a Sirix resource not only in space but also in time.

Opening a specific version is possible with XQuery, the Java-API or a RESTful Web-Service. Serializing either a single version or a bunch of versions is also supported. Despite, future work includes the specification
of a delta-format.

In addition Sirix provides a very powerful axis-API and exposes each XPath-axis as well as all temporal axis (to navigate in time), a LevelOrderAxis, a PostorderAxis and a special DescendantVisitorAxis which is able to use a visitor, skip whole subtrees from traversal (might also depend on the depth), terminate the processing and to skip the traversal of sibling nodes. Furthermore all filters for instance to filter specific nodes, QNames, text-values and so on are exposed. In contrast to other XML database systems we also support the movement of whole subtrees, without having to delete and reinsert the subtree (which would also change unique node-IDs).
Furthermore it is easy to store other record-types as the built-in (XDM) types.

All index-structures are always kept up-to-date and versioned just as the data itself. A path summary stores reference-counters, that is how many nodes are stored on a specific path.

In contrast to some other approaches we also store path class records (PCR), that is the node-IDs of path summary nodes in the value indexes.

Furthermore in stark contrast to all other approaches the authors are aware of moves are supported, which preserve node-identity and aren't simple combinations of insert/delete-subtree operations. Instead only local changes take place. However with the path summary and other index-structures enabled the operation is likewise costly.

## Features in a nutshell
- Transactional, versioned, typed user-defined index-structures, which are automatically updated once a transaction commits.
- Through XPath-axis extensions we support the navigation not only in space but also in time (future::, past::, first::, last::...). Furthermore we provide several temporal XQuery functions due to our integral versioning approach.
- An in memory path summary, which is persisted during a transaction commit and always kept up-to-date.
- Configurable versioning at the database level (full, incremental, differential and a new sliding snapshot algorithm which balances reads and writes without introducing write-peaks, which are usually generated during intermediate full dumps, which are usually written to).
- Log-structured sequential writes and random reads due to transactional copy-on-write (COW) semantics. This offers nice benefits as for instance no locking for concurrent reading-transactions and it takes full advantage of flash disks while avoiding their weaknesses.
- Complete isolation of currently N read-transactions and a single write-transaction per resource.
- The page-structure is heavily inspired by ZFS and therefore also forms a tree. We'll implement a similar merkle-tree and store hashes of each page in parent-pointers for integrity checks.
- Support of XQuery and XQuery Update due to a slightly modified version of brackit(.org).
- Moves are additionally supported.
- Automatic path-rewriting of descendant-axis to child-axis if appropriate.
- Import of differences between two XML-documents, that is after the first version of an XML-document is imported an algorithm tries to update the Sirix resource with a minimum of operations to change the first version into the new version.
- A fast ID-based diff-algorithm which is able to determine differences between any two versions of a resource stored in Sirix optionally taking hashes of a node into account.
- The number of children of a node, the number of descendants, a hash as well as an ORDPATH / DeweyID label which is compressed on disk to efficiently determine document order as well as to support other nice properties of hierarchical node labels is optionally stored with each node. Currently the number of children is always stored and the number of descendants is stored if hashing is enabled.
- Flexible backend.
- Optional encryption and/or compression of each page on disk.

Currently we are refactoring a RESTful-API and we'll explore how to efficiently distribute Sirix. Furthermore we aim to support an extended XDM in order to store JSON natively with additional node-types in Sirix. The implementation should be straight forward.

Besides, the architecture for versioning data is not restricted to tree-structures by all means as demonstrated in the Ph.D. Thesis of Sebastian Graf (Sirix originated a few years ago as a fork of Treetank going back to its roots and focusing on the versioning of tree-structured data): http://nbn-resolving.de/urn:nbn:de:bsz:352-272505

Storing files natively is also on our agenda. Furthermore a key management schema similar to the one described in Sebastian's Thesis has to be implemented. 

## First steps
Please have a look into our sirix-example project how to use Sirix. We'll shortly provide a refactored RESTful-API to interact with a Sirix-Server.

## Developers
Developers which are eager to put forth the idea of a versioned, secure database system especially suitable, but not restricted to rooted trees (serialized form as XML/JSON) are always welcome. The idea is not only to support (and extend querying) as for instance via XQuery efficiently, but also to support other datamining tasks such as the comparison of hierarchical tree-structures.

## Documentation
We are currently working on the documentation. You may find first drafts and snippets in the Wiki. Furthermore you are kindly invited to ask any question you might have (and you likely have many questions) in the mailinglist. 
Please also have a look at and play with our sirix-example bundle which is available via maven.

## Mailinglist
Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-discuss to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

## GUI
A screencast is available depicting the SunburstView and the TextView side by side: 
http://www.youtube.com/watch?v=l9CXXBkl5vI

![Moves visualized through Hierarchical Edge Bundles](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/moves-cut.png "Moves visualized through Hierarchical Edge Bundles")
![SunburstView](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png "SunburstView")
![Wikipedia / SunburstView comparison mode / TextView comparison mode](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-scrolled.png "Wikipedia / SunburstView comparison mode / TextView comparison mode")
![Small Multiple Displays (incremental variant)](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-incremental.png "Small Multiple Displays (incremental variant)")

## Further information

Sirix was initially forked from Treetank (https://github.com/disy/treetank) due to the specialization on XML, which isn't the focus of Treetank anymore.
As such my deepest gratitude to all the other students who worked on the project.

First of all:

- Marc Kramis for his first drafts,
- Sebastian Graf for his almost complete rewrite of Treetank,
- Patrick Lang (RESTful API),
- Lukas Lewandowski (RESTful API),
- Tina Scherer (XPath engine)

and all the others who worked on the project.

## License

This work is released in the public domain under the BSD 3-clause license

## Involved People

Sirix is maintained by:

* Johannes Lichtenberger

Your name might follow? ;-)
