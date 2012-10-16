#Sirix - a versioned, treebased storage system

The architecture supports the well known ACID-properties (durability currently isn't guaranted if the transaction crashes) and Snapshot Isolation through MVCC which in turn supports N-reading transactions in parallel to currently 1-write transaction. Supporting N-write transactions is planned as well as the current work on indexes to support a binding to Brackit which in turn supports XQuery/XQuery Update Facility. The COW-approach used for providing Snapshot Isolation through MVCC is especially well suited for flash-disks. We support several well known versioning strategies.

The GUI provides interactive visualizations of the differences between either 2 or more versions of a resource in Sirix. Please have a look into my master-thesis for screenshots.

Some examples of the Java-API are explained in the wiki. Stay tuned for a maven bundle with examples and more elaborate examples.

Sirix will be nothing without interested developers (contributors). Any kind of contribution is highly welcome. Once a few (regular) contributors are found, we will create an organization for Sirix on github.

Note that it is based on Treetank (http://treetank.org / http://github.com/disy/treetank).

[![Build Status](https://secure.travis-ci.org/JohannesLichtenberger/sirix.png)](http://travis-ci.org/JohannesLichtenberger/sirix)

##GUI
A screencast is available depicting the SunburstView and the TextView side by side: 
http://www.youtube.com/watch?v=l9CXXBkl5vI

![Moves visualized through Hierarchical Edge Bundles](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/moves-cut.png "Moves visualized through Hierarchical Edge Bundles")
![SunburstView](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png "SunburstView")
![Wikipedia / SunburstView comparison mode / TextView comparison mode](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-scrolled.png "Wikipedia / SunburstView comparison mode / TextView comparison mode")
![Small Multiple Displays (incremental variant)](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-incremental.png "Small Multiple Displays (incremental variant)")

##Maven artifacts
At this stage of development please use the latest SNAPSHOT artifacts from https://oss.sonatype.org/content/repositories/snapshots/com/github/johanneslichtenberger/sirix/.

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

Maven artifacts are deployed to the central maven repository. Currently the following artifacts are available:

Core project:
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-core&lt;/artifactId&gt;
  &lt;version&gt;0.1.1&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

JAX-RX interface (RESTful API):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-jax-rx&lt;/artifactId&gt;
  &lt;version&gt;0.1.1&lt;/version&gt;
&lt;/dependency&gt;
</code></pre>

Saxon interface (use Saxon to query data):
<pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.github.johanneslichtenberger.sirix&lt;/groupId&gt;
  &lt;artifactId&gt;sirix-saxon&lt;/artifactId&gt;
  &lt;version&gt;0.1.1&lt;/version&gt;
&lt;/dependency>
</pre></code>

Other modules are currently not available (namely the GUI, the distributed package) due to dependencies to processing.org which isn't available from a maven repository and other dependencies.

##API Examples
(Currently) a small set of API-examples is provided in the wiki: [Simple Usage](https://github.com/JohannesLichtenberger/sirix/wiki/Simple-usage.)

##Mailinglist
Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-users to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

##Content

* README:					this readme file
* LICENSE:	 			license file
* bundles					all available bundles
* pom.xml:				Simple pom (yes we do use Maven)

##Further information

As Sirix is based on Treetank current documentation thus is currently only available from this location.

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

* Johannes Lichtenberger (Sirix Core & Project Lead based on Treetank Core)
