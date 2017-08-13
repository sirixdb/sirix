# Sirix - Beyond Versioning of Persistent Trees

"Remember that you're lucky, even if you don't think you are, because there's always something that you can be thankful for." - Esther Grace Earl (http://tswgo.org)

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

## Simple Example 
Test if fragments are not present in the past. In this example they are appended to a node in the most recent revision and stored in a subsequent revision)
<pre><code>(* Loading document: *)
bit:load('mydoc.xml', '/tmp/sample8721713104854945959.xml')

(* Update loaded document: *)
INSERT NODES &lt;a&gt;&lt;b/&gt;test&lt;/a&gt; INTO doc('mydoc.xml')/log

(* intermediate commit *)

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


## First steps
Please have a look into our sirix-example project how to use Sirix. We'll shortly provide a refactored RESTful-API to interact with a Sirix-Server.

## Developers
Developers which are eager to put forth the idea of a versioned, secure database system especially suitable, but not restricted to rooted trees (serialized form as XML/JSON) are always welcome. The idea is not only to support (and extend querying) as for instance via XQuery efficiently, but also to support other datamining tasks such as the comparison of hierarchical tree-structures.

## Documentation
We are currently working on the documentation. You may find first drafts and snippets in the Wiki. Furthermore you are kindly invited to ask any question you might have (and you likely have many questions) in the mailinglist. 
Please also have a look at and play with our sirix-example bundle which is available via maven.

## Mailinglist
Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-discuss to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

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

[![Build Status](https://travis-ci.org/sirixdb/sirix.png)](https://travis-ci.org/sirixdb/sirix)
[![Coverage Status](https://coveralls.io/repos/sirixdb/sirix/badge.svg)](https://coveralls.io/r/sirixdb/sirix)

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
