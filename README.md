#Sirix - a versioned, treebased storage system

The architecture supports the well known ACID-properties (durability currently isn't guaranted if the transaction crashes) and Snapshot Isolation through MVCC which in turn supports N-reading transactions in parallel to currently 1-write transaction. Supporting N-write transactions is planned as well as the current work on indexes to support a binding to Brackit which in turn supports XQuery/XQuery Update Facility. The COW-approach used for providing Snapshot Isolation through MVCC is especially well suited for flash-disks. We support several well known versioning strategies.

The GUI provides interactive visualizations of the differences between either 2 or more versions of a resource in Sirix. Please have a look into my master-thesis for screenshots.

Some examples of the Java-API are explained in the wiki. Stay tuned for a maven bundle with examples and more elaborate examples.

Any questions or even consider to contribute or use Sirix? Use https://groups.google.com/d/forum/sirix-users to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases... Besides, suggestions for improvements are highly welcome. 

Sirix will be nothing without interested developers (contributors). Any kind of contribution is highly welcome. Once a few (regular) contributors are found, we will create an organization for Sirix on github.

Note that it is based on Treetank (http://treetank.org / http://github.com/disy/treetank).

[![Build Status](https://secure.travis-ci.org/JohannesLichtenberger/sirix.png)](http://travis-ci.org/JohannesLichtenberger/sirix)

##Maven artifacts
Maven artifacts are deployed to the OSS nexus repository. Currently the following artifacts are available:

Core project:
<pre><code>
    <dependency>
      <groupId>com.github.johanneslichtenberger.sirix</groupId>
      <artifactId>sirix-core</artifactId>
      <version>0.1.0</version>
    </dependency>
</code></pre>

JAX-RX interface (RESTful API):
    <dependency>
      <groupId>com.github.johanneslichtenberger.sirix</groupId>
      <artifactId>sirix-jax-rx</artifactId>
      <version>0.1.0</version>
    </dependency>

Saxon interface (use Saxon to query data):
    <dependency>
      <groupId>com.github.johanneslichtenberger.sirix</groupId>
      <artifactId>sirix-saxon</artifactId>
      <version>0.1.0</version>
    </dependency>

Other modules are currently not available (namely the GUI, the distributed package) due to dependencies to processing.org which isn't available from a maven repository and other dependencies.

##Content

* README:					this readme file
* LICENSE:	 				license file
* bundles					all available bundles
* pom.xml:					Simple pom (yes we do use Maven)

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
