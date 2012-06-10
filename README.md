#Sirix - Secure Treebased Storage

Sirix stores data securely by applying different layers on the stored data. Flexible handling of flat as well as tree-based data is supported by a native encoding of the tree-structures ongoing with suitable paging supporting integrity and confidentiality to provide throughout security.

Secure in this context includes integrity-checks, confidentiality with tree-aware key handling, versioning to provide accountability.
Furthermore, different backends are provided while a binding to different cloud-infrastructures is in progress.

[![Build Status](https://secure.travis-ci.org/johannes.lichtenberger/sirix.png)](http://travis-ci.org/johannes.lichtenberger/sirix)

##Content

* README:					this readme file
* LICENSE:	 				license file
* coremodules:				Bundles containing main treetank functionality
* interfacemodules:			Bundles implementing third-party interfaces
* studentmodules:			Bundles containing student projects
* scripts:					bash scripts for syncing against disy-internal repo.
* pom.xml:					Simple pom (yes we do use Maven)

##Further information

The documentation so far is accessible under http://treetank.org (pointing to http://disy.github.com/treetank/).

The framework was presented at various conferences and acted as base for multiple publications and reports:

* A Secure Cloud Gateway based upon XML and Web Services; ECOWS'11, PhD Symposium: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-154112 
* Treetank, Designing a Versioned XML Storage; XMLPrague'11: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-126912
* Rolling Boles, Optimal XML Structure Integrity for Updating Operations; WWW'11, Poster: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-126226
* Hecate, Managing Authorization with RESTful XML; WS-REST'11: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-126237 
* Integrity Assurance for RESTful XML; WISM'10: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-123507
* JAX-RX - Unified REST Access to XML Resources; TechReport'10: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-120511
* Distributing XML with focus on parallel evaluation; DBISP2P'08: http://kops.ub.uni-konstanz.de/handle/urn:nbn:de:bsz:352-opus-84487

Any questions, just contact sebastian.graf AT uni-konstanz.de

##License

This work is released in the public domain under the BSD 3-clause license


##Involved People

Treetank is maintained by:

* Sebastian Graf (Treetank Core & Project Lead)

Current subprojects are:

* Patrick Lang (Encryption layer)
* Johannes Lichtenberger (Visualization of temporal trees)
* Wolfgang Miller (Binding of Treetank to cloud backends)

Concluded subprojects were:

* Johannes Lichtenberger (Evaluation of several versioning approaches)
* Patrick Lang (Encryption layer)
* Lukas Lewandowski (Jax-RX binding)
* Tina Scherer (native XPath2 engine)
* Marc Kramis (first drafts of Treetank core)

