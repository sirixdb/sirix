#Sirix - a versioned storage system for hierarchical data (rooted labeled trees -- XML (and JSON in the future))

The architecture supports the well known ACID-properties (durability currently isn't guaranted if the transaction crashes) and Snapshot Isolation through MVCC which in turn supports N-reading transactions in parallel to currently 1-write transaction without any locking. Supporting N-write transactions is in the works as well as the current work on indexes to support a Brackit binding which in turn supports XQuery and the XQuery Update Facility. The CoW-approach used for providing Snapshot Isolation through MVCC is especially well suited for flash-disks (sequential writes and random reads). We support several well known versioning strategies (incremental, differential, full).

The GUI provides interactive visualizations of the differences between either 2 or more versions of a resource in Sirix. Please have a look into my master-thesis for screenshots.

Some examples of the Java-API and the Brackit binding to use XQuery and the XQuery Update Facility (with temporal extensions) are explained in the wiki. Stay tuned for a maven bundle with examples and more elaborate examples.

Sirix will be nothing without interested developers (contributors). Any kind of contribution is highly welcome. Once a few (regular) contributors are found, we will create an organization for Sirix on github.

Note that it is based on Treetank (http://treetank.org / http://github.com/disy/treetank).

[![Build Status](https://travis-ci.org/JohannesLichtenberger/sirix.png?branch=master)](https://travis-ci.org/JohannesLichtenberger/sirix)

##Features

The main features are:
* it provides a diff-algorithm to import differences between two
XML-documents (I've also imported a small set of sorted wikipedia
articles by their revision-timestamps and a predefined time interval
which decides when to store a new revision)
* an ID-based diff-algorithm which detects differences between the
revisions/versions of a single resource (for instance used for
interactive visualizations thereof)
* several well known versioning strategies which might adapt themselves
in the future according to different workloads
=> thus any revision can be simply serialized, queried...
* supports XQuery (through Brackit(.org))
* supports the XQuery Update Facility (through Brackit(.org))
* supports temporal axis as for instance all-time::, past::, past-or-self::, future::, future-or-self::, first::, last::, next::, previous:: as well as extended functions, as for instance doc(xs:string, xs:int) to specify a revision to restore with the secon argument.
* a GUI incorporates several views which are
visualizing either a single revision or the differences between two or
more revisions of a resource (an XML-document imported into the native format in Sirix)
* (naturally) implements a form of MVCC and thus readers are never blocked
* single write-transaction in parallel to N read-transactions on the
same resource 

##GUI
A screencast is available depicting the SunburstView and the TextView side by side: 
http://www.youtube.com/watch?v=l9CXXBkl5vI

![Moves visualized through Hierarchical Edge Bundles](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/moves-cut.png "Moves visualized through Hierarchical Edge Bundles")
![SunburstView](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png "SunburstView")
![Wikipedia / SunburstView comparison mode / TextView comparison mode](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-scrolled.png "Wikipedia / SunburstView comparison mode / TextView comparison mode")
![Small Multiple Displays (incremental variant)](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-incremental.png "Small Multiple Displays (incremental variant)")

##Maven artifacts
At this stage of development please use the latest SNAPSHOT artifacts from https://oss.sonatype.org/content/repositories/snapshots/com/github/johanneslichtenberger/sirix/.
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
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-core&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

JAX-RX interface (RESTful API):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-jax-rx&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

Brackit(.org) interface (use Brackit to query data -- in the future should be preferable to Saxon (as we will include index rewriting rules... and it supports many build-in XQuery functions as well as the XQuery Update Facility), however as of now it is our very first (unstable) version):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-xquery&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency>
</pre></code>

Saxon interface (use Saxon to query data):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-saxon&lt;/artifactId&gt;
  &lt;version&gt;0.1.2-SNAPSHOT&lt;/version&gt;
&lt;/dependency>
</pre></code>


Other modules are currently not available (namely the GUI, the distributed package) due to dependencies to processing.org which isn't available from a maven repository and other dependencies.

##API Examples
(Currently) a small set of API-examples is provided in the wiki: [Simple Usage](https://github.com/JohannesLichtenberger/sirix/wiki/Simple-usage)

##Mailinglist
Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-users to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

##Content

* README:					this readme file
* LICENSE:	 			license file
* bundles					all available bundles
* pom.xml:				Simple pom (yes we do use Maven)

##Further information

As Sirix is a relatively young fork of Treetank documentation besides the one in the wiki thus is currently only available from this location.

The documentation so far is accessible under http://treetank.org (pointing to http://disy.github.com/treetank/).

The framework was presented at various conferences and acted as base for multiple publications and reports:

* A legal and technical perspective on secure cloud Storage; DFN Forum'12: [PDF](http://nbn-resolving.de/urn:nbn:de:bsz:352-192389)
* A Secure Cloud Gateway based upon XML and Web Services; ECOWS'11, PhD Symposium: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-154112)
* Treetank, Designing a Versioned XML Storage; XMLPrague'11: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-126912)
* Rolling Boles, Optimal XML Structure Integrity for Updating Operations; WWW'11, Poster: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-126226)
* Hecate, Managing Authorization with RESTful XML; WS-REST'11: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-126237)
* Integrity Assurance for RESTful XML; WISM'10: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-123507)
* JAX-RX - Unified REST Access to XML Resources; TechReport'10: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-120511)
* Distributing XML with focus on parallel evaluation; DBISP2P'08: [PDF](http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-84487)

##License

This work is released in the public domain under the BSD 3-clause license


##Involved People

Sirix is maintained by:

* Johannes Lichtenberger
