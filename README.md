<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/logo.png"/></p>

<h1 align="center">An Embeddable, Evolutionary, Append-Only Database System</h1>
<p align="center">Stores small-sized, immutable snapshots of your data in an append-only manner. It facilitates querying and reconstructing the entire history as well as easy audits.</p>

<p align="center">
    <a href="https://github.com/sirixdb/sirix/actions" alt="CI Build Status"><img src="https://github.com/sirixdb/sirix/workflows/Java%20CI%20with%20Gradle/badge.svg"/></a>
    <a href="#contributors-" alt="All Contributors"><img src="https://img.shields.io/badge/all_contributors-23-orange.svg?style=flat-square"></img></a>
    <a href="https://www.codefactor.io/repository/github/sirixdb/sirix" alt="Code Factor"><img src="ttps://www.codefactor.io/repository/github/sirixdb/sirix/badge"></img></a>
    <a href="http://makeapullrequest.com" alt="PRs Welcome"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square"></img></a>
    <a href="https://search.maven.org/search?q=g:io.sirix" alt="Maven Central"><img src="https://img.shields.io/maven-central/v/io.sirix/sirix-core.svg"></img></a>
</p>

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=SirixDB+-+a+storage+system%2C+which+creates+%28very+small-sized%29+snapshots+of+your+data+on+every+transaction-commit+through+the+implementation+of+a+novel+sliding+snapshot+algorithm.&url=http://sirix.io&via=sirix&hashtags=versioning,diffing,xml,kotlin,coroutines,vertx)

[![Follow](https://img.shields.io/twitter/follow/sirixdb.svg?style=social)](https://twitter.com/sirixdb)

[Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) | [Join us on Discord](https://discord.gg/yC33wVpv7t) | [Community Forum](https://sirix.discourse.group/) | [Documentation](https://sirix.io/documentation.html)

**Working on your first Pull Request?** You can learn how from this *free* series [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github) and another tutorial: [How YOU can contribute to OSS, a beginners guide](https://dev.to/itnext/how-you-can-contribute-to-oss-36id)

>"Remember that you're lucky, even if you don't think you are, because there's always something that you can be thankful for." - Esther Grace Earl (http://tswgo.org)

**We want to build the database system together with you. Help us and become a maintainer yourself. Why? Maybe you like the software and want to help us. Furthermore, you'll learn a lot. Maybe you want to fix a bug or add a feature? You want to add an awesome project to your portfolio? You want to grow your network?... All of this are valid reasons besides probably many more**: [Collaborating on Open Source Software](https://youtu.be/TQN8BIxUmRo)

SirixDB appends data to an indexed log file without the need of a WAL. It can simply be embedded and used as a library from your favorite language on the JVM to store and query data locally or by using a simple CLI. An asynchronous HTTP-server, which adds the core and query modules as dependencies can alternatively be used to interact with SirixDB over the network using Keycloak for authentication/authorization. One file stores the data with all revisions and possibly secondary indexes. A second file stores offsets into the file to quickly search for a revision by a given timestamp using an in-memory binary search. Furthermore a few maintenance files exist, which store the configuration of a resource and the definitions of secondary indexes (if any are configured). Other JSON files keep track of changes in delta files if enabled.

It currently supports the storage and (time travel) querying of both XML - and JSON-data in its binary encoding, tailored to support versioning. The index-structures and the whole storage engine has been written from scratch to support versioning natively. We might also implement the storage and querying of other data formats as relational data.

SirixDB uses a huge persistent (in the functional sense) tree of tries, wherein the committed snapshots share unchanged pages and even common records in changed pages. The system only stores page-fragments during a copy-on-write out-of-place operation instead of full pages during a commit to reduce write-amplification. During read operations, the system reads the page-fragments in parallel to reconstruct an in-memory page (thus, a fast, random access storage device as a PCIe SSD is best suited or even byte addressable storage as Intel DC optane memory in the near future -- as SirixDB stores fine granular cache-size (not page) aligned modifications in a single file.

<!--
<p>&nbsp;</p>

<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/showcase/screencast-three-revisions-faster.gif"/></p>

<p>&nbsp;</p>
-->

**Please consider [sponsoring](https://opencollective.com/sirixdb/) our Open Source work if you like the project.**

**Note: Let us know if you'd like to build a brand-new frontend with for instance [SolidJS](https://solidjs.com), [D3.js](https://d3js), and Typescript.**

**Discuss it in the [Community Forum](https://sirix.discourse.group).**

## Table of contents
-   [Keeping All Versions of Your Data By Sharing Structure](#keeping-all-versions-of-your-data-by-sharing-structure)
-   [JSONiq examples](#jsoniq-examples)
-   [SirixDB Features](#sirixdb-features)
    -   [Design Goals](#design-goals)
    -   [Revision Histories](#revision-histories)
-   [Getting Started](#getting-started)
    -   [Download ZIP or Git Clone](#download-zip-or-git-clone)
    -   [Maven Artifacts](#maven-artifacts)
    -   [Setup of the SirixDB HTTP-Server and Keycloak to use the REST-API](#setup-of-the-sirixdb-http-server-and-keycloak-to-use-the-rest-api)
    -   [Command line tool](#command-line-tool)
    -   [Documentation](#documentation)
-   [Getting Help](#getting-help)
    -   [Community Forum](#community-forum)
    -   [Join us on Discord](#join-us-on-discord)
-   [Contributors](#contributors-)
-   [License](#license)

## Keeping All Versions of Your Data By Sharing Structure
We could write quite a bunch of stuff, why it's often of great value to keep all states of your data in a storage system. Still, recently we stumbled across an excellent [blog post](https://www.hadoop360.datasciencecentral.com/blog/temporal-databases-why-you-should-care-and-how-to-get-started-par), which explains the advantages of keeping historical data very well. In a nutshell, it's all about looking at the evolution of your data, finding trends, doing audits, implementing efficient undo-/redo-operations. The [Wikipedia page](https://en.wikipedia.org/wiki/Temporal_database) has a bunch of examples. We recently also added use cases over [here](https://sirix.io/documentation.html).

Our firm belief is that a temporal storage system must address the issues, which arise from keeping past states way better than traditional approaches. Usually, storing time-varying, temporal data in database systems that do not support the storage thereof natively results in many unwanted hurdles. They waste storage space, query performance to retrieve past states of your data is not mostideal, and usually, temporal operations are missing altogether.

The DBS must store data in a way that storage space is used as effectively as possible while supporting the reconstruction of each revision, as the database saw it during the commits. All this should be handled in linear time, whether it's the first revision or the most recent revision. Ideally, query time of old/past revisions and the most recent revision should be in the same runtime complexity (logarithmic when querying for specific records).

SirixDB not only supports snapshot-based versioning on a record granular level through a novel versioning algorithm called sliding snapshot, but also time travel queries, efficient diffing between revisions and the storage of semi-structured data to name a few.

Executing the following time-travel query on our binary JSON representation of [Twitter sample data](https://raw.githubusercontent.com/sirixdb/sirix/master/bundles/sirix-core/src/test/resources/json/twitter.json) gives an initial impression of the possibilities:

```xquery
let $statuses := jn:open('mycol.jn','mydoc.jn', xs:dateTime('2019-04-13T16:24:27Z')).statuses
let $foundStatus := for $status in $statuses
  let $dateTimeCreated := xs:dateTime($status.created_at)
  where $dateTimeCreated > xs:dateTime("2018-02-01T00:00:00") and not(exists(jn:previous($status)))
  order by $dateTimeCreated
  return $status
return {"revision": sdb:revision($foundStatus), $foundStatus{text}}
```
The query opens a database/resource in a specific revision based on a timestamp (`2019–04–13T16:24:27Z`) and searches for all statuses, which have a `created_at` timestamp, which has to be greater than the 1st of February in 2018 and did not exist in the previous revision. `.` is a dereferencing operator used to dereference keys in JSON objects, array values can be accessed as shown looping over the values or through specifying an index, starting with zero: `array[[0]]` for instance specifies the first value of the array. [Brackit](https://github.com/sirixdb/brackit), our query processor also supports Python-like array slices to simplify tasks.

## JSONiq examples

In order to verify changes in a node or its subtree, first select the node in the revision and then
query for changes using our stored merkle hash tree, which builds and updates hashes for each node and it's subtree and check the hashes with `sdb:hash($item)`. The function `jn:all-times` delivers the node in all revisions in which it exists. `jn:previous` delivers
the node in the previous revision or an empty sequence if there's none.

```xquery
let $node := jn:doc('mycol.jn','mydoc.jn').fieldName[[1]]
let $result := for $node-in-rev in jn:all-times($node)
               let $nodeInPreviousRevision := jn:previous($node-in-rev)
               return
                 if ((not(exists($nodeInPreviousRevision)))
                      or (sdb:hash($node-in-rev) ne sdb:hash($nodeInPreviousRevision))) then
                   $node-in-rev
                 else
                   ()
return [
  for $jsonItem in $result
  return { "node": $jsonItem, "revision": sdb:revision($jsonItem) }
]
```

Emit all diffs between the revisions in a JSON format:

```xquery
let $maxRevision := sdb:revision(jn:doc('mycol.jn','mydoc.jn'))
let $result := for $i in (1 to $maxRevision)
               return
                 if ($i > 1) then
                   jn:diff('mycol.jn','mydoc.jn',$i - 1, $i)
                 else
                   ()
return [
  for $diff at $pos in $result
  return {"diffRev" || $pos || "toRev" || $pos + 1: jn:parse($diff).diffs}
]
```

We support easy updates as in

```xquery
let $array := jn:doc('mycol.jn','mydoc.jn')
return insert json {"bla":true} into $array at position 0
```

to insert a JSON object into a resource, whereas the root node is an array at the first position (0). The transaction is implicitly committed, thus a new revision is created and the specific revision can be queried using a single third argument, either a simple integer ID or a timestamp. The following query issues a query on the first revision (thus without the changes). 
```xquery
jn:doc('mycol.jn','mydoc.jn',1)
```

Omitting the third argument simply opens the resource in the most recent revision, but you could in this case also specify revision number 2. You can also use a timestamp as in:

```xquery
jn:open('mycol.jn','mydoc.jn',xs:dateTime('2022-03-01T00:00:00Z'))
```

A simple join (whereas joins are optimized in our query processor called Brackit):

```xquery
(* first: store stores in a stores resource *)
sdb:store('mycol.jn','stores','
[
  { "store number" : 1, "state" : "MA" },
  { "store number" : 2, "state" : "MA" },
  { "store number" : 3, "state" : "CA" },
  { "store number" : 4, "state" : "CA" }
]')


(* second: store sales in a sales resource *)
sdb:store('mycol.jn','sales','
[
  { "product" : "broiler", "store number" : 1, "quantity" : 20  },
  { "product" : "toaster", "store number" : 2, "quantity" : 100 },
  { "product" : "toaster", "store number" : 2, "quantity" : 50 },
  { "product" : "toaster", "store number" : 3, "quantity" : 50 },
  { "product" : "blender", "store number" : 3, "quantity" : 100 },
  { "product" : "blender", "store number" : 3, "quantity" : 150 },
  { "product" : "socks", "store number" : 1, "quantity" : 500 },
  { "product" : "socks", "store number" : 2, "quantity" : 10 },
  { "product" : "shirt", "store number" : 3, "quantity" : 10 }
]')

let $stores := jn:doc('mycol.jn','stores')
let $sales := jn:doc('mycol.jn','sales')
let $join :=
  for $store in $stores, $sale in $sales
  where $store."store number" = $sale."store number"
  return {
    "nb" : $store."store number",
    "state" : $store.state,
    "sold" : $sale.product
  }
return [$join]
```

SirixDB through Brackit also supports array slices. Start index is 0, step is 1 and end index is 1 (exclusive) in the next query:

```xquery
let $array := [{"foo": 0}, "bar", {"baz": true}]
return $array[[0:1:1]]
```

The query returns the first object `{"foo":0}`. 

With the function `sdb:nodekey` you can find out the internal unique node key of a node, which will never change. You for instance might be interested in which revision it has been removed. The following query uses the function `sdb:select-item` which as the first argument needs a context node and as the second argument the key of the item or node to select. `jn:last-existing` finds the most recent version and `sdb:revision` retrieves the revision number.

```xquery
sdb:revision(jn:last-existing(sdb:select-item(jn:doc('mycol.jn','mydoc.jn',1), 26)))
```

### Index types

SirixDB has three types of indexes along with a path summary tree, which is basically a tree of all distinct paths:

- name indexes, to index a set of object fields
- path indexes, to index a set of paths (or all paths in a resource)
- CAS indexes, so called content-and-structure indexes, which index paths and typed values (for instance all xs:integers). In this case on the paths specified only integer values are indexed on the path, but no other types

We base the indexes on the following serialization of three revisions of a very small SirixDB ressource.

```json
{
  "sirix": [
    {
      "revisionNumber": 1,
      "revision": {
        "foo": [
          "bar",
          null,
          2.33
        ],
        "bar": {
          "hello": "world",
          "helloo": true
        },
        "baz": "hello",
        "tada": [
          {
            "foo": "bar"
          },
          {
            "baz": false
          },
          "boo",
          {},
          []
        ]
      }
    },
    {
      "revisionNumber": 2,
      "revision": {
        "tadaaa": "todooo",
        "foo": [
          "bar",
          null,
          103
        ],
        "bar": {
          "hello": "world",
          "helloo": true
        },
        "baz": "hello",
        "tada": [
          {
            "foo": "bar"
          },
          {
            "baz": false
          },
          "boo",
          {},
          []
        ]
      }
    },
    {
      "revisionNumber": 3,
      "revision": {
        "tadaaa": "todooo",
        "foo": [
          "bar",
          null,
          23.76
        ],
        "bar": {
          "hello": "world",
          "helloo": true
        },
        "baz": "hello",
        "tada": [
          {
            "foo": "bar"
          },
          {
            "baz": false
          },
          "boo",
          {},
          [
            {
              "foo": "bar"
            }
          ]
        ]
      }
    }
  ]
}
```

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $stats := jn:create-name-index($doc, ('foo','bar'))
return {"revision": sdb:commit($doc)}
```
The index is created for "foo" and "bar" object fields. You can query for "foo" fields as for instance:

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $nameIndexNumber := jn:find-name-index($doc, 'foo')
for $node in jn:scan-name-index($doc, $nameIndexNumber, 'foo')
order by sdb:revision($node), sdb:nodekey($node)
return {"nodeKey": sdb:nodekey($node), "path": sdb:path($node), "revision": sdb:revision($node)}
```

Second, whole paths are indexable.

Thus, the following path index is applicable to both queries: `.sirix[].revision.tada[].foo` and `.sirix[].revision.tada[][[4]].foo`. Thus, essentially both foo nodes are indexed and the first child has to be fetched afterwards. For the second query also the array index 4 has to be checked if the indexed node is really on index 4.

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada//[]/foo')
return {"revision": sdb:commit($doc)}
```

The index might be scanned as follows:

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $pathIndexNumber := jn:find-path-index($doc, '/sirix/[]/revision/tada//[]/foo')
for $node in jn:scan-path-index($doc, $pathIndexNumber, '/sirix/[]/revision/tada//[]/foo')
order by sdb:revision($node), sdb:nodekey($node)
return {"nodeKey": sdb:nodekey($node), "path": sdb:path($node)}
```

CAS indexes index a path plus the value. The value itself must be typed (so in this case we index only decimals on a path).

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $stats := jn:create-cas-index($doc, 'xs:decimal', '/sirix/[]/revision/foo/[]')
return {"revision": sdb:commit($doc)}
```

We can do an index range-scan as for instance via the next query (2.33 and 100 are the min and max, the next two arguments are two booleans which denote if the min and max should be retrieved or if it's >min and <max). The last argument is usually a path if we index more paths in the same index (in this case we only index `/sirix/[]/revision/foo/[]`).

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $casIndexNumber := jn:find-cas-index($doc, 'xs:decimal', '/sirix/[]/revision/foo/[]')
for $node in jn:scan-cas-index-range($doc, $casIndexNumber, 2.33, 100, false(), true(), ())
order by sdb:revision($node), sdb:nodekey($node)
return {"nodeKey": sdb:nodekey($node), "node": $node}
```

You can also create a CAS index on all string values on all paths (all object fields: `//*`; all arrays: `//[]`):

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $stats := jn:create-cas-index($doc,'xs:string',('//*','//[]'))
return {"revision": sdb:commit($doc)}
```

To query for a string values with a certain name (`bar`) on all paths (empty sequence `()`):

```xquery
let $doc := jn:doc('mycol.jn','mydoc.jn')
let $casIndexNumber := jn:find-cas-index($doc, 'xs:string', '//*')
for $node in jn:scan-cas-index($doc, $casIndexNumber, 'bar', '==', ())
order by sdb:revision($node), sdb:nodekey($node)
return {"nodeKey": sdb:nodekey($node), "node": $node, "path": sdb:path(sdb:select-parent($node))}
```

The argument `==` means check for equality of the string. Other values which might make more sense for integers, decimals... are `<`, `<=`, `>=` and `>`.

## SirixDB Features
SirixDB is a log-structured, temporal NoSQL document store, which stores evolutionary data. It never overwrites any data on-disk. Thus, we're able to restore and query the full revision history of a resource in the database.

### Design Goals
Some of the most important core principles and design goals are:

<dl>
  <dt>Embeddable</dt>
  <dd>Similar to SQLite and DucksDB SirixDB is embeddable at its core. Other APIs as the non-blocking REST-API are built on top.</dd>
  <dt>Minimize Storage Overhead</dt>
  <dd>SirixDB shares unchanged data pages as well as records between revisions, depending on a chosen versioning algorithm during the initial bootstrapping of a resource. SirixDB aims to balance read and writer performance in its default configuration.</dd>
  <dt>Concurrent</dt>
  <dd>SirixDB contains very few locks and aims to be as suitable for multithreaded systems as possible.</dd>
  <dt>Asynchronous</dt>
  <dd>Operations can happen independently; each transaction is bound to a specific revision and only one read/write-transaction on a resource is permitted concurrently to N read-only-transactions.</dd>
  <dt>Versioning/Revision history</dt>
  <dd>SirixDB stores a revision history of every resource in the database without imposing extra overhead. It uses a huge persistent, durable page-tree for indexing revisions and data.</dd>
  <dt>Data integrity</dt>
  <dd>SirixDB, like ZFS, stores full checksums of the pages in the parent pages. That means that almost all data corruption can be detected upon reading in the future, we aim to partition and replicate databases in the future.</dd>
  <dt>Copy-on-write semantics</dt>
  <dd>Similarly to the file systems Btrfs and ZFS, SirixDB uses CoW semantics, meaning that SirixDB never overwrites data. Instead, database-page fragments are copied/written to a new location. SirixDB does not simply copy whole pages. Instead, it only copies changed records plus records, which fall out of a sliding window.</dd>
  <dt>Per revision and page versioning</dt>
  <dd>SirixDB does not only version on a per revision, but also on a per page-base. Thus, whenever we change a potentially small fraction
of records in a data-page, it does not have to copy the whole page and write it to a new location on a disk or flash drive. Instead, we can specify one of several versioning strategies known from backup systems or a novel sliding snapshot algorithm during the creation of a database resource. The versioning-type we specify is used by SirixDB to version data-pages.</dd>
  <dt>Guaranteed atomicity and consistency (without a WAL)</dt>
  <dd>The system will never enter an inconsistent state (unless there is hardware failure), meaning that unexpected power-off won't ever damage the system. This is accomplished without the overhead of a write-ahead-log. 
(<a href="https://en.wikipedia.org/wiki/Write-ahead_logging">WAL</a>)</dd>
  <dt>Log-structured and SSD friendly</dt>
  <dd>SirixDB batches writes and syncs everything sequentially to a flash drive
during commits. It never overwrites committed data.</dd>
</dl>

### Revision Histories
**Keeping the revision history is one of the main features in
SirixDB.** You can revert any revision into an earlier version or back up the system automatically without the overhead of copying. SirixDB only ever copies changed database-pages and, depending on the versioning algorithm you chose during the creation of a database/resource, only page-fragments, and ancestor index-pages to create a new revision.

You can reconstruct every revision in <em>O(n)</em>, where <em>n</em>
denotes the number of nodes in the revision. Binary search is used on
an in-memory (linked) map to load the revision, thus finding the
revision root page has an asymptotic runtime complexity of <em>O(log
n)</em>, where <em>n</em>, in this case, is the number of stored
revisions.

Currently, SirixDB offers two built-in native data models, namely a
binary XML store and a JSON store.

<p>&nbsp;&nbsp;</p>

<p align="center"><img src="https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png"/></p>

<p>&nbsp;&nbsp;</p>

Articles published on Medium:
- [Asynchronous, Temporal  REST With Vert.x, Keycloak and Kotlin Coroutines](https://medium.com/hackernoon/asynchronous-temporal-rest-with-vert-x-keycloak-and-kotlin-coroutines-217b25756314)
- [Pushing Database Versioning to Its Limits by Means of a Novel Sliding Snapshot Algorithm and Efficient Time Travel Queries](https://medium.com/sirixdb-sirix-io-how-we-built-a-novel-temporal/why-and-how-we-built-a-temporal-database-system-called-sirixdb-open-source-from-scratch-a7446f56f201)
- [How we built an asynchronous, temporal RESTful API based on Vert.x, Keycloak and Kotlin/Coroutines for Sirix.io (Open Source)](https://medium.com/sirixdb-sirix-io-how-we-built-a-novel-temporal/how-we-built-an-asynchronous-temporal-restful-api-based-on-vert-x-4570f681a3)
- [Why Copy-on-Write Semantics and Node-Level-Versioning are Key to Efficient Snapshots](https://hackernoon.com/sirix-io-why-copy-on-write-semantics-and-node-level-versioning-are-key-to-efficient-snapshots-754ba834d3bb)

## Status
SirixDB as of now has not been tested in production. It is recommended for experiments, testing, benchmarking, etc., but is not recommended for production usage. Let us know if you'd like to use SirixDB in production and get in touch. We'd like to test real-world datasets and fix issues we encounter along the way.

Please also get in touch if you like our vision and you want to sponsor us or help with man-power or if you want to use SirixDB as a research system. We'd be glad to get input from the database and scientific community.

## Getting started

### [Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) or Git Clone

```
git clone https://github.com/sirixdb/sirix.git
```

or use the following dependencies in your Maven or Gradle project.

**SirixDB uses Java 19, thus you need an up-to-date Gradle (if you want to work on SirixDB) and an IDE (for instance IntelliJ or Eclipse).**

### Maven artifacts
At this stage of development, you should use the latest SNAPSHOT artifacts from [the OSS snapshot repository](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/) to get the most recent changes.

Just add the following repository section to your POM or build.gradle file:
```xml
<repository>
  <id>sonatype-nexus-snapshots</id>
  <name>Sonatype Nexus Snapshots</name>
  <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  <releases>
    <enabled>false</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```
```groovy
repository {
    maven {
        url "https://oss.sonatype.org/content/repositories/snapshots/"
        mavenContent {
            snapshotsOnly()
        }
    }
}
```

<strong>Note that we changed the groupId from `com.github.sirixdb.sirix` to `io.sirix`. Most recent version is 0.9.7-SNAPSHOT.</strong>

Maven artifacts are deployed to the central maven repository (however please use the SNAPSHOT-variants as of now). Currently, the following artifacts are available:

Core project:
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-core</artifactId>
  <version>0.9.7-SNAPSHOT</version>
</dependency>
```
```groovy
compile group:'io.sirix', name:'sirix-core', version:'0.9.7-SNAPSHOT'
```

Brackit binding:
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-xquery</artifactId>
  <version>0.9.7-SNAPSHOT</version>
</dependency>
```
```groovy
compile group:'io.sirix', name:'sirix-xquery', version:'0.9.7-SNAPSHOT'
```

Asynchronous, RESTful API with Vert.x, Kotlin and Keycloak (the latter for authentication via OAuth2/OpenID-Connect):
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-rest-api</artifactId>
  <version>0.9.7-SNAPSHOT</version>
</dependency>
```

```groovy
compile group: 'io.sirix', name: 'sirix-rest-api', version: '0.9.7-SNAPSHOT'
```

Other modules are currently not available (namely the GUI, the distributed package as well as an outdated Saxon binding).

You have to add the following JVM parameters currently:

```
-ea
--enable-preview
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

Plus we recommend using the Shenandoah GC:

```
-XX:+UseShenandoahGC
-Xlog:gc
-XX:+AlwaysPreTouch
-XX:+UseLargePages
-XX:-UseBiasedLocking
-XX:+DisableExplicitGC
```

### Setup of the SirixDB HTTP-Server and Keycloak to use the REST-API

The REST-API is asynchronous at its very core. We use Vert.x, which is a toolkit built on top of Netty. It is heavily inspired by Node.js but for the JVM. As such, it uses event loop(s), which is thread(s), which never should by blocked by long-running CPU tasks or disk-bound I/O. We are using Kotlin with coroutines to keep the code simple. SirixDB uses OAuth2 (Password Credentials/Resource Owner Flow) using a Keycloak authorization server instance.

### Keycloak setup (Standalone Setup / Docker Hub Image)

You can set up Keycloak as described in this excellent [tutorial](
https://piotrminkowski.wordpress.com/2017/09/15/building-secure-apis-with-vert-x-and-oauth2/). Our [docker-compose](https://raw.githubusercontent.com/sirixdb/sirix/master/docker-compose.yml) file imports a sirix realm with a default admin user with all available roles assigned. You can skip steps 3 - 7 and 10, 11, and simply recreate a `client-secret` and change `oAuthFlowType` to "PASSWORD". If you want to run or modify the integration tests, the client secret must not be changed. Make sure to delete the line "build: ." in the `docker-compse.yml` file for the server image if you want to use the Docker Hub image.

1. Open your browser. URL: http://localhost:8080
2. Login with username "admin", password "admin"
3. Create a new **realm** with the name **"sirixdb"**
4. Go to `Clients` => `account`
5. Change client-id to "sirix"
6. Make sure `access-type` is set to `confidential`
7. Go to `Credentials` tab
8. Put the `client secret` into the SirixDB HTTP-Server [configuration file]( https://raw.githubusercontent.com/sirixdb/sirix/master/bundles/sirix-rest-api/src/main/resources/sirix-conf.json). Change the value of "client.secret" to whatever Keycloak set up.
9. If "oAuthFlowType" is specified in the same configuration file change the value to "PASSWORD" (if not default is "PASSWORD").
10. Regarding Keycloak the `direct access` grant on the settings tab must be `enabled`.
11. Our (user-/group-)roles are "create" to allow creating databases/resources, "view" to allow to query database resources, "modify" to modify a database resource and "delete" to allow deletion thereof. You can also assign `${databaseName}-` prefixed roles.
 
### Start Docker Keycloak-Container using docker-compose
For setting up the SirixDB HTTP-Server and a basic Keycloak-instance with a test realm:

1. `git clone https://github.com/sirixdb/sirix.git`
2. `sudo docker-compose up keycloak`

### Start the SirixDB HTTP-Server and the Keycloak-Container using docker compose

_This section describes setting up the Keycloak using `docker compose`. If you are looking for configuring keycloak from scratch, instructions for that is described in the previous section_

For setting up the SirixDB HTTP-Server and a basic Keycloak-instance with a test `sirixdb` realm:

1. At first clone this repository with the following command (Or download .zip)
    
       git clone https://github.com/sirixdb/sirix.git

2. `cd` into the sirix folder that was just cloned.
    
       cd sirix/

3. Run the Keycloak container using `docker compose`

       sudo docker compose up keycloak

4. Visit `http://localhost:8080` and login to the admin console using username: `admin` and password: `admin`
5. From the navigation panel on the left, select `Realm Settings` and verify that the `Name` field is set to `sirixdb`
6. Select the client with Client ID `sirix`
   1. Verify direct access grant enabled on.
   2. Verify that `Access Type` is set to `confidential`
   3. In the `credentials` tab
      1. Verify that the Client Authenticatior is set to `Client Id and Secret`
      2. Click on Regenerate Secret to generate a new secret. Set the value of the field named `client.secret` of the [configuration file](https://raw.githubusercontent.com/sirixdb/sirix/master/bundles/sirix-rest-api/src/main/resources/sirix-conf.json) to this secret.
      
7. Finally run the SirixDB-HTTP Server and Keycloak container with docker compose

        docker compose up

### SirixDB HTTP-Server Setup Without Docker/docker-compose

To created a fat-JAR. Download our ZIP-file for instance, then

1. `cd bundles/sirix-rest-api`
2. `gradle build -x test`

And a fat-JAR with all required dependencies should have been created in your target folder.

Furthermore, a `key.pem` and a `cert.pem` file are needed. These two files have to be in your user home directory in a directory called "sirix-data", where Sirix stores the databases. For demo purposes they can be copied from our [resources directory](https://github.com/sirixdb/sirix/tree/master/bundles/sirix-rest-api/src/main/resources).

Once also Keycloak is set up we can start the server via:

`java -jar -Duser.home=/opt/sirix sirix-rest-api-*-SNAPSHOT-fat.jar -conf sirix-conf.json -cp /opt/sirix/*`

If you like to change your user home directory to `/opt/sirix` for instance.

The fat-JAR in the future will be downloadable from the [maven repository](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/sirix-rest-api/0.9.0-SNAPSHOT/).

### Run the Integration Tests
In order to run the integration tests under `bundles/sirix-rest-api/src/test/kotlin` make sure that you assign your admin user all the user-roles you have created in the Keycloak setup (last step). Make sure that Keycloak is running first and execute the tests in your favorite IDE for instance.

Note that the following VM-parameters currently are needed: `-ea --add-modules=jdk.incubator.foreign --enable-preview`

### Command-line tool
We ship a (very) simple command-line tool for the sirix-xquery bundle:

Get the [latest sirix-xquery JAR](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/sirix-xquery/) with dependencies.

### Documentation
We are currently working on the documentation. You may find first drafts and snippets in the [documentation](https://sirix.io/documentation.html) and in this README. Furthermore, you are kindly invited to ask any question you might have (and you likely have many questions) in the community forum (preferred) or in the Discord channel.
Please also have a look at and play with our sirix-example bundle which is available via maven or our new asynchronous RESTful API (shown next).

## Getting Help

### Community Forum
If you have any questions or are considering to contribute or use Sirix, please use the [Community Forum](https://sirix.discourse.group) to ask questions. Any kind of question, may it be an API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

### Join us on Discord
You may find us on [Discord](https://discord.gg/yC33wVpv7t) for quick questions.

## Contributors ✨

SirixDB is maintained by

* Johannes Lichtenberger

And the Open Source Community.

As the project was forked from a university project called Treetank, my deepest gratitude to Marc Kramis, who came up with the idea of building a versioned, secure and energy-efficient data store, which retains the history of resources of his Ph.D. Furthermore, Sebastian Graf came up with a lot of ideas and greatly improved the implementation for his Ph.D. Besides, a lot of students worked and improved the project considerably.

Thanks goes to these wonderful people, who greatly improved SirixDB lately. SirixDB couldn't exist without the help of the Open Source community:

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/yiss"><img src="https://avatars1.githubusercontent.com/u/12660796?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ilias YAHIA</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=yiss" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/BirokratskaZila"><img src="https://avatars1.githubusercontent.com/u/24469472?v=4?s=100" width="100px;" alt=""/><br /><sub><b>BirokratskaZila</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=BirokratskaZila" title="Documentation">📖</a></td>
    <td align="center"><a href="https://mrbuggysan.github.io/"><img src="https://avatars0.githubusercontent.com/u/9119360?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Andrei Buiza</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=MrBuggySan" title="Code">💻</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/dmytro-bondar-330804103/"><img src="https://avatars0.githubusercontent.com/u/11942950?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Bondar Dmytro</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Loniks" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/santoshkumarkannur"><img src="https://avatars3.githubusercontent.com/u/56201023?v=4?s=100" width="100px;" alt=""/><br /><sub><b>santoshkumarkannur</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=santoshkumarkannur" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/LarsEckart"><img src="https://avatars1.githubusercontent.com/u/4414802?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Lars Eckart</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=LarsEckart" title="Code">💻</a></td>
    <td align="center"><a href="http://www.hackingpalace.net"><img src="https://avatars1.githubusercontent.com/u/6793260?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jayadeep K M</b></sub></a><br /><a href="#projectManagement-kmjayadeep" title="Project Management">📆</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://keithkim.org"><img src="https://avatars0.githubusercontent.com/u/318225?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Keith Kim</b></sub></a><br /><a href="#design-karmakaze" title="Design">🎨</a></td>
    <td align="center"><a href="https://github.com/theodesp"><img src="https://avatars0.githubusercontent.com/u/328805?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Theofanis Despoudis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=theodesp" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/Mrexsp"><img src="https://avatars3.githubusercontent.com/u/23698645?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Mario Iglesias Alarcón</b></sub></a><br /><a href="#design-Mrexsp" title="Design">🎨</a></td>
    <td align="center"><a href="https://twitter.com/_anmonteiro"><img src="https://avatars2.githubusercontent.com/u/661909?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antonio Nuno Monteiro</b></sub></a><br /><a href="#projectManagement-anmonteiro" title="Project Management">📆</a></td>
    <td align="center"><a href="http://fultonbrowne.github.io"><img src="https://avatars1.githubusercontent.com/u/50185337?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Fulton Browne</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FultonBrowne" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/felixrabe"><img src="https://avatars3.githubusercontent.com/u/400795?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Felix Rabe</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=felixrabe" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/ELWillis10"><img src="https://avatars3.githubusercontent.com/u/182492?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ethan Willis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ethanwillis" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/bark"><img src="https://avatars1.githubusercontent.com/u/223964?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Erik Axelsson</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=bark" title="Code">💻</a></td>
    <td align="center"><a href="https://se.rg.io/"><img src="https://avatars1.githubusercontent.com/u/976915?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sérgio Batista</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=batista" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/chaensel"><img src="https://avatars2.githubusercontent.com/u/2786041?v=4?s=100" width="100px;" alt=""/><br /><sub><b>chaensel</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=chaensel" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/balajiv113"><img src="https://avatars1.githubusercontent.com/u/13016475?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Balaji Vijayakumar</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=balajiv113" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/FernandaCG"><img src="https://avatars3.githubusercontent.com/u/28972973?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Fernanda Campos</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FernandaCG" title="Code">💻</a></td>
    <td align="center"><a href="https://joellau.github.io/"><img src="https://avatars3.githubusercontent.com/u/29514264?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Joel Lau</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=JoelLau" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/add09"><img src="https://avatars3.githubusercontent.com/u/38160880?v=4?s=100" width="100px;" alt=""/><br /><sub><b>add09</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=add09" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/EmilGedda"><img src="https://avatars2.githubusercontent.com/u/4695818?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Emil Gedda</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=EmilGedda" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/arohlen"><img src="https://avatars1.githubusercontent.com/u/49123208?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Andreas Rohlén</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=arohlen" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/marcinbieleckiLLL"><img src="https://avatars3.githubusercontent.com/u/26444765?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Marcin Bielecki</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=marcinbieleckiLLL" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/ManfredNentwig"><img src="https://avatars1.githubusercontent.com/u/164948?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Manfred Nentwig</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ManfredNentwig" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/Raj-Datta-Manohar"><img src="https://avatars0.githubusercontent.com/u/25588557?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Raj</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Raj-Datta-Manohar" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/mosheduminer"><img src="https://avatars.githubusercontent.com/u/47164590?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Moshe Uminer</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=mosheduminer" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

Contributions of any kind are highly welcome!

## License

This work is released under the [BSD 3-clause license](LICENSE).
