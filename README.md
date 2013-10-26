#Sirix - Beyond Versioning of Persistent Trees

"Don't forget to be awesome." - Esther Earl (http://tswgo.org)

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

Further use cases, development news and other stuff related to Sirix: http://sirixdb.tumblr.com

<a href="http://twitter.com/home/?status=Thanks @https%3A%2F%2Ftwitter.com%2Fsirixdb for making Sirix: https%3A%2F%2Fgithub.com%2Fsirixdb%2Fsirix"><img src="https://s3.amazonaws.com/github-thank-you-button/thank-you-button.png" alt="Say Thanks" /></a>

## Developers
First of all, I'm searching for interested open source developers which are eager to put forth the idea of a versioned, secure database system especially suitable, but not restricted to rooted trees (serialized form as XML/JSON). The idea is not only to support (and extend querying) as for instance via XQuery efficiently, but also to support other datamining tasks such as the comparison of hierarchical tree-structures.

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

## Documentation
We are currently working on the documentation. You may find first drafts and snippets in the Wiki. Furthermore you are kindly invited to ask any question you might have (and you likely have many questions) in the mailinglist. 
Please also have a look at and play with our sirix-example bundle which is available via maven.

## Mailinglist
Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-users to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

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
  &lt;artifactId&gt;sirix-examples&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

JAX-RX interface (RESTful API):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-jax-rx&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

Brackit(.org) interface (use Brackit to query data -- in the future should be preferable to Saxon (as we will include index rewriting rules... and it supports many build-in XQuery functions as well as the XQuery Update Facility), however as of now it is our very first (unstable) version):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-xquery&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency>
</pre></code>

Saxon interface (use Saxon to query data):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.sirixdb.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-saxon&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency>
</pre></code>


Other modules are currently not available (namely the GUI, the distributed package) due to dependencies to processing.org which isn't available from a maven repository and other dependencies.

## Technical details and Features

[![Build Status](https://travis-ci.org/sirixdb/sirix.png)](https://travis-ci.org/sirixdb/sirix)

The architecture supports the well known ACID-properties (durability currently isn't guaranted if the transaction crashes) and Snapshot Isolation through MVCC which in turn supports N-reading transactions in parallel to currently 1-write transaction without any locking. Supporting N-write transactions is in the works as well as the current work on indexes to support a Brackit binding which in turn supports XQuery and the XQuery Update Facility. The CoW-approach used for providing Snapshot Isolation through MVCC is especially well suited for flash-disks (sequential writes and random reads). We support several well known versioning strategies (incremental, differential, full).

The main features in a nutshell are:
* import of differences between a resource stored in Sirix and a new version thereof in the form of an XML-document, another Sirix resource or in the future a JSON-document
(for instance we imported two versions of an AST, a small set of sorted Wikipedia articles by revision-timestamps and a predefined time interval
which is used to decide when to store a new revision)
* an ID-based diff-algorithm which detects differences between two revisions/versions of a single resource in Sirix (for instance used by
interactive visualizations thereof)
* several well known versioning strategies which might adapt themselves in the future according to different workloads 
=> thus any revision can simply be serialized, queried...
* supports indexes (whereas we are currently working on more sophisticated typed, user defined or "learned" CoW B+-indexes -- also needs to be integrated into the Brackit-binding)
* supports XQuery (through Brackit(.org))
* supports the XQuery Update Facility (through Brackit(.org))
* supports temporal axis as for instance all-time::, past::, past-or-self::, future::, future-or-self::, first::, last::, next::, previous:: as well as extended functions, as for instance doc(xs:string, xs:int) to specify a revision to restore with the second argument.
* Path rewrite optimizations for the descendant::- and descendant-or-self:: axis to support the Brackit-binding
* a GUI incorporates several views which are
visualizing either a single revision or the differences between two or
more revisions of a resource (an XML-document imported into the native format in Sirix)
* (naturally) implements a form of MVCC and thus readers are never blocked
* single write-transaction in parallel to N read-transactions on the
same resource 
* in-memory- or on-disk-storage
* the page-size isn't padded until a predefined size is reached. Instead only necessary bytes are written.

## Donations
If you ever think this software does anything good and is of use to you, you might consider donating something to this wonderful "This Star Won't Go Out" Foundation at http://tswgo.org.

## Future
A bunch of work includes the current index-structures:

- We are currently refactoring our work on index-structures. Instead of providing text-value/attribute-value and an element index, we will shortly provide an infrastructure which allows the specification of path-restricted indexes, restricted on specific atomic values/QNames and so on. Furthermore the index-structures will be typed indexes. Once these additions have been added, we will work hard on the integration in Brackit and build a sophisticated set of rewriting-rules.
- Besides, we will probably change the index-structures based on a threshold value to switch between an AVLTree (for instance which is much better suited for updates in comparison to B+-trees and derivates) or a special version of a CoW-B+-tree, which trades a possibly small reading bottleneck for faster updates (path-copying will be much cheaper).

Furthermore we would like to research how to implement sharding.

If anyone is interested to cooperate or contribute, further topics are:

- providing multiple write-transactions as well as a simple recovery-mechanism as well as checkpointing
- providing encryption bound to specific subtrees/revisions/user groups

##GUI
A screencast is available depicting the SunburstView and the TextView side by side: 
http://www.youtube.com/watch?v=l9CXXBkl5vI

![Moves visualized through Hierarchical Edge Bundles](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/moves-cut.png "Moves visualized through Hierarchical Edge Bundles")
![SunburstView](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png "SunburstView")
![Wikipedia / SunburstView comparison mode / TextView comparison mode](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-scrolled.png "Wikipedia / SunburstView comparison mode / TextView comparison mode")
![Small Multiple Displays (incremental variant)](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-incremental.png "Small Multiple Displays (incremental variant)")

##Further information

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
