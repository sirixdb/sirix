[![Build Status](https://travis-ci.org/sirixdb/sirix.png)](https://travis-ci.org/sirixdb/sirix)
[![All Contributors](https://img.shields.io/badge/all_contributors-20-orange.svg?style=flat-square)](#contributors)
[![CodeFactor](https://www.codefactor.io/repository/github/sirixdb/sirix/badge)](https://www.codefactor.io/repository/github/sirixdb/sirix)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
[![Maven Central](https://img.shields.io/maven-central/v/io.sirix/sirix-parent.svg)](https://search.maven.org/search?q=g:io.sirix)

<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/logo.png"/></p>

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=SirixDB+-+a+storage+system%2C+which+creates+%28very+small-sized%29+snapshots+of+your+data+on+every+transaction-commit+through+the+implementation+of+a+novel+sliding+snapshot+algorithm.&url=http://sirix.io&via=sirix&hashtags=versioning,diffing,xml,kotlin,coroutines,vertx)

[![Follow](https://img.shields.io/twitter/follow/sirixdb.svg?style=social)](https://twitter.com/sirixdb)

[Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) | [Join us on Slack](https://join.slack.com/t/sirixdb/shared_invite/enQtNjI1Mzg4NTY4ODUzLTE3NmRhMWRiNWEzMjQ0NjAxNTZlODBhMTQzMWM2Nzc5MThkMjlmMzI0ODRlNGE0ZDgxNDcyODhlZDRhYjM2N2U) | [Community Forum](https://sirix.discourse.group/)

**Working on your first Pull Request?** You can learn how from this *free* series [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github) and another tutorial: [How YOU can contribute to OSS, a beginners guide](https://dev.to/itnext/how-you-can-contribute-to-oss-36id)

<h1 align="center">SirixDB - An Evolutionary, Temporal NoSQL Document Store</h1>
<h2 align="center">Store and query revisions of your data efficiently</h2>

>"Remember that you're lucky, even if you don't think you are, because there's always something that you can be thankful for." - Esther Grace Earl (http://tswgo.org)

**We currently support the storage and (time travel) querying of both XML- and JSON-data in our binary encoding which is tailored to support versioning. Our index-structures and the whole storage engine has been written from scratch to support versioning natively. In the Future we might also support the storage and querying of other data formats.**

<p>&nbsp;</p>

<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/showcase/screencast-three-revisions-faster.gif"/></p>

<p>&nbsp;</p>

**Note: Work on a [Front-end](https://github.com/sirixdb/sirix-web-frontend) built with [Vue.js](https://vuejs.org), [D3.js](https://d3js) and Typescript has just begun**

**Discuss it in the [Community Forum](https://sirix.discourse.group)**

## Why should you even bother? Advantages of a native, temporal database system
We could write quite a bunch of stuff, why it's oftentimes of great value to keep a past state of your data in a storage system, but recently we stumbled across an excellent [blog post](https://www.hadoop360.datasciencecentral.com/blog/temporal-databases-why-you-should-care-and-how-to-get-started-par), which explains the advantages of keeping historical data very well. In a nutshell, it's all about looking at the evolution of your data, finding trends, doing audits, implementing efficient undo-/redo-operations... the [Wikipedia page](https://en.wikipedia.org/wiki/Temporal_database) has a bunch of examples. We recently also added use cases over [here](https://sirix.io/documentation.html).

Our strong belief is that a temporal storage system must address the issues, which arise from keeping past state way better than traditional approaches. Usually, storing time-varying, temporal data in database systems, which do not support the storage thereof natively results in a lot of unwanted hurdles. Storage space is wasted, query performance to retrieve past states of your data is not ideal and usually temporal operations are missing altogether.

Data must be stored in a way, that storage space is used as effectively as possible while supporting the reconstruction of each revision, as it was seen by the database during the commits, in linear time, no matter if it's the very first revision or the most recent revision. Ideally query time of old/past revisions as well as the most recent revision should be in the same runtime complexity (logarithmic when querying for specific records).

We not only support snapshot-based versioning on a record granular level through a novel versioning algorithm called sliding snapshot, but also time travel queries, efficient diffing between revisions and the storage of semi-structured data to name a few.

The following time-travel query to be executed on our binary JSON representation of [Twitter sample data](https://raw.githubusercontent.com/sirixdb/sirix/master/bundles/sirix-core/src/test/resources/json/twitter.json) gives an initial impression of what's possible:

```xquery
let $statuses := jn:open('mycol.jn','mydoc.jn', xs:dateTime('2019-04-13T16:24:27Z'))=>statuses
let $foundStatus := for $status in bit:array-values($statuses)
  let $dateTimeCreated := xs:dateTime($status=>created_at)
  where $dateTimeCreated > xs:dateTime("2018-02-01T00:00:00") and not(exists(jn:previous($status)))
  order by $dateTimeCreated
  return $status
return {"revision": sdb:revision($foundStatus), $foundStatus{text}}
```

The query basically opens a database/resource in a specific revision based on a timestamp (`2019–04–13T16:24:27Z`) and searches for all statuses, which have a `created_at` timestamp, which has to be greater than the 1st of February in 2018 and did not exist in the previous revision. `=>` is a dereferencing operator used to dereference keys in JSON objects, array values can be accessed as shown with the function bit:array-values or through specifying an index, starting with zero: array[[0]] for instance specifies the first value of the array.

## SirixDB Features
SirixDB is a log-structured, temporal NoSQL document store, which stores evolutionary data. It never overwrites any data on-disk. Thus, we're able to restore and query the full revision history of a resource in the database.

### Design Goals
Some of the most important core principles and design goals are:

<dl>
  <dt>Minimize Storage Overhead</dt>
  <dd>SirixDB shares unchanged data pages as well as records between revisions, depending on a chosen versioning algorithm during the initial bootstrapping of a resource. SirixDB aims to balance read and writer performance in its default configuration</dd>
  <dt>Concurrent</dt>
  <dd>SirixDB contains very few locks and aims to be as suitable for multithreaded systems as possible</dd>
  <dt>Asynchronous</dt>
  <dd>Operations can happen independently; each transaction is bound to a specific revision and only one read/write-transaction on a resource is permitted concurrently to N read-only-transactions</dd>
  <dt>Versioning/Revision history</dt>
  <dd>SirixDB stores a revision history of every resource in the database without imposing extra overhead. It uses a huge persistent, durable page-tree for indexing revisions and data</dd>
  <dt>Data integrity</dt>
  <dd>SirixDB, like ZFS, stores full checksums of the pages in the parent pages. That means that almost all data corruption can be detected upon reading in the future, we aim to partition and replicate databases in the future</dd>
  <dt>Copy-on-write semantics</dt>
  <dd>Similarly to the file systems Btrfs and ZFS, SirixDB uses CoW semantics, meaning that SirixDB never overwrites data. Instead, database-page fragments are copied/written to a new location</dd>
  <dt>Per revision and per page versioning</dt>
  <dd>SirixDB does not only version on a per revision, but also on a per page-base. Thus, whenever we change a potentially small fraction
of records in a data-page, it does not have to copy the whole page and write it to a new location on a disk or flash drive. Instead, we can specify one of several versioning strategies known from backup systems or a novel sliding snapshot algorithm during the creation of a database resource. The versioning-type we specify is used by SirixDB to version data-pages</dd>
  <dt>Guaranteed atomicity and consistency (without a WAL)</dt>
  <dd>The system will never enter an inconsistent state (unless there is hardware failure), meaning that unexpected power-off won't ever damage the system. This is accomplished without the overhead of a write-ahead-log (<a
href="https://en.wikipedia.org/wiki/Write-ahead_logging">WAL</a>)</dd>
  <dt>Log-structured and SSD friendly</dt>
  <dd>SirixDB batches writes and syncs everything sequentially to a flash drive
during commits. It never overwrites committed data</dd>
</dl>

## Revision Histories
**Keeping the revision history is one of the main features in
SirixDB.** We're able to revert any revision into an earlier
version or back up the system automatically without the overhead of
copying. SirixDB only ever copies changed database-pages and depending
on the versioning algorithm we chose during the creation of a
database/resource only page-fragments as well as ancestor index-pages
to create a new revision.

We can reconstruct every revision in <em>O(n)</em>, where <em>n</em>
denotes the number of nodes in the revision. Binary search is used on
an in-memory (linked) map to load the revision, thus finding the
revision root page has an asymptotic runtime complexity of <em>O(log
n)</em>, where <em>n</em>, in this case, is the number of stored
revisions.

Currently, SirixDB offers two built-in native data models, namely a
binary XML store as well as a JSON store.

<p>&nbsp;&nbsp;</p>

<p align="center"><img src="https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/sunburstview-cut.png"/></p>

<p>&nbsp;&nbsp;</p>

Articles published on Medium:
- [Asynchronous, Temporal  REST With Vert.x, Keycloak and Kotlin Coroutines](https://medium.com/hackernoon/asynchronous-temporal-rest-with-vert-x-keycloak-and-kotlin-coroutines-217b25756314)
- [Pushing Database Versioning to Its Limits by Means of a Novel Sliding Snapshot Algorithm and Efficient Time Travel Queries](https://medium.com/sirixdb-sirix-io-how-we-built-a-novel-temporal/why-and-how-we-built-a-temporal-database-system-called-sirixdb-open-source-from-scratch-a7446f56f201)
- [How we built an asynchronous, temporal RESTful API based on Vert.x, Keycloak and Kotlin/Coroutines for Sirix.io (Open Source)](https://medium.com/sirixdb-sirix-io-how-we-built-a-novel-temporal/how-we-built-an-asynchronous-temporal-restful-api-based-on-vert-x-4570f681a3)
- [Why Copy-on-Write Semantics and Node-Level-Versioning are Key to Efficient Snapshots](https://hackernoon.com/sirix-io-why-copy-on-write-semantics-and-node-level-versioning-are-key-to-efficient-snapshots-754ba834d3bb)

## Project Maintainer

SirixDB is maintained by

* Johannes Lichtenberger

And the Open Source Community.

## Contributors ✨

As the project was forked from a university project called Treetank, my deepest gratitute to Marc Kramis, who came up with the idea of building a versioned, secure and energy efficient data store, which retains the history of resources for his PhD. Furthermore Sebastian Graf came up with a lot of ideas and greatly improved the implementation for his PhD. Besides, a lot of students worked and improved the project considerably.

Thanks goes to these wonderful people, who greatly improved SirixDB lately. SirixDB couldn't exist without the help of the Open Source community:

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/yiss"><img src="https://avatars1.githubusercontent.com/u/12660796?v=4" width="100px;" alt=""/><br /><sub><b>Ilias YAHIA</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=yiss" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/BirokratskaZila"><img src="https://avatars1.githubusercontent.com/u/24469472?v=4" width="100px;" alt=""/><br /><sub><b>BirokratskaZila</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=BirokratskaZila" title="Documentation">📖</a></td>
    <td align="center"><a href="https://mrbuggysan.github.io/"><img src="https://avatars0.githubusercontent.com/u/9119360?v=4" width="100px;" alt=""/><br /><sub><b>Andrei Buiza</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=MrBuggySan" title="Code">💻</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/dmytro-bondar-330804103/"><img src="https://avatars0.githubusercontent.com/u/11942950?v=4" width="100px;" alt=""/><br /><sub><b>Bondar Dmytro</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Loniks" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/santoshkumarkannur"><img src="https://avatars3.githubusercontent.com/u/56201023?v=4" width="100px;" alt=""/><br /><sub><b>santoshkumarkannur</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=santoshkumarkannur" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/LarsEckart"><img src="https://avatars1.githubusercontent.com/u/4414802?v=4" width="100px;" alt=""/><br /><sub><b>Lars Eckart</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=LarsEckart" title="Code">💻</a></td>
    <td align="center"><a href="http://www.hackingpalace.net"><img src="https://avatars1.githubusercontent.com/u/6793260?v=4" width="100px;" alt=""/><br /><sub><b>Jayadeep K M</b></sub></a><br /><a href="#projectManagement-kmjayadeep" title="Project Management">📆</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://keithkim.org"><img src="https://avatars0.githubusercontent.com/u/318225?v=4" width="100px;" alt=""/><br /><sub><b>Keith Kim</b></sub></a><br /><a href="#design-karmakaze" title="Design">🎨</a></td>
    <td align="center"><a href="https://github.com/theodesp"><img src="https://avatars0.githubusercontent.com/u/328805?v=4" width="100px;" alt=""/><br /><sub><b>Theofanis Despoudis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=theodesp" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/Mrexsp"><img src="https://avatars3.githubusercontent.com/u/23698645?v=4" width="100px;" alt=""/><br /><sub><b>Mario Iglesias Alarcón</b></sub></a><br /><a href="#design-Mrexsp" title="Design">🎨</a></td>
    <td align="center"><a href="https://twitter.com/_anmonteiro"><img src="https://avatars2.githubusercontent.com/u/661909?v=4" width="100px;" alt=""/><br /><sub><b>Antonio Nuno Monteiro</b></sub></a><br /><a href="#projectManagement-anmonteiro" title="Project Management">📆</a></td>
    <td align="center"><a href="http://fultonbrowne.github.io"><img src="https://avatars1.githubusercontent.com/u/50185337?v=4" width="100px;" alt=""/><br /><sub><b>Fulton Browne</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FultonBrowne" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/felixrabe"><img src="https://avatars3.githubusercontent.com/u/400795?v=4" width="100px;" alt=""/><br /><sub><b>Felix Rabe</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=felixrabe" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/ELWillis10"><img src="https://avatars3.githubusercontent.com/u/182492?v=4" width="100px;" alt=""/><br /><sub><b>Ethan Willis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ethanwillis" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/bark"><img src="https://avatars1.githubusercontent.com/u/223964?v=4" width="100px;" alt=""/><br /><sub><b>Erik Axelsson</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=bark" title="Code">💻</a></td>
    <td align="center"><a href="https://se.rg.io/"><img src="https://avatars1.githubusercontent.com/u/976915?v=4" width="100px;" alt=""/><br /><sub><b>Sérgio Batista</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=batista" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/chaensel"><img src="https://avatars2.githubusercontent.com/u/2786041?v=4" width="100px;" alt=""/><br /><sub><b>chaensel</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=chaensel" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/balajiv113"><img src="https://avatars1.githubusercontent.com/u/13016475?v=4" width="100px;" alt=""/><br /><sub><b>Balaji Vijayakumar</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=balajiv113" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/FernandaCG"><img src="https://avatars3.githubusercontent.com/u/28972973?v=4" width="100px;" alt=""/><br /><sub><b>Fernanda Campos</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FernandaCG" title="Code">💻</a></td>
    <td align="center"><a href="https://joellau.github.io/"><img src="https://avatars3.githubusercontent.com/u/29514264?v=4" width="100px;" alt=""/><br /><sub><b>Joel Lau</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=JoelLau" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

Contributions of any kind are highly welcome!

## Table of contents

-   [Getting Started](#getting-started)
    -   [Download ZIP or Git Clone](#download-zip-or-git-clone)
    -   [Maven Artifacts](#maven-artifacts)
    -   [Docker Images](#docker-images)
    -   [Command line tool](#command-line-tool)
    -   [First steps](#first-steps)
    -   [Documentation](#documentation)
-   [RESTful-API](#restful-api)
-   [DOM alike API](#dom-alike-api) 💪
-   [Simple XQuery Examples](#simple-xquery-examples)
-   [Getting Help](#getting-help)
    -   [Community Forum](#community-forum)
    -   [Join us on Slack](#join-us-on-slack)
-   [Visualizations](#visualizations)
-   [Why should you even bother?](#why-should-you-even-bother)
-   [Features in a nutshell](#features-in-a-nutshell)
-   [Developers](#developers)
-   [Further information](#further-information)
-   [License](#license)
-   [Involved People](#involved-people)

## Getting started

### [Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) or Git Clone

```
git clone https://github.com/sirixdb/sirix.git
```

or use the following dependencies in your Maven (or Gradle?) project.

We just changed to Java13 (OpenJDK 13).

### Maven artifacts
At this stage of development, you could use the latest SNAPSHOT artifacts from [the OSS snapshot repository](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/) to get the most recent changes. However, we just released version 0.9.5 of Sirix :-)

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
repository{
    maven {
        url "https://oss.sonatype.org/content/repositories/snapshots/"
        mavenContent {
            snapshotsOnly()
        }
    }
}
```

<strong>Note that we changed the groupId from `com.github.sirixdb.sirix` to `io.sirix`. Most recent version is 0.9.6-SNAPSHOT.</strong>

Maven artifacts are deployed to the central maven repository (however please use the SNAPSHOT-variants as of now). Currently the following artifacts are available:

Core project:
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-core</artifactId>
  <version>0.9.6-SNAPSHOT</version>
</dependency>
```
```groovy
compile group:'io.sirix', name:'sirix-core', version:'0.9.6-SNAPSHOT'
```

Brackit binding:
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-xquery</artifactId>
  <version>0.9.6-SNAPSHOT</version>
</dependency>
```
```groovy
compile group:'io.sirix', name:'sirix-xquery', version:'0.9.6-SNAPSHOT'
```

Asynchronous, RESTful API with Vert.x, Kotlin and Keycloak (the latter for authentication via OAuth2/OpenID-Connect):
```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-rest-api</artifactId>
  <version>0.9.4-SNAPSHOT</version>
</dependency>
```

```groovy
compile group: 'io.sirix', name: 'sirix-rest-api', version: '0.9.6-SNAPSHOT'
```

Other modules are currently not available (namely the GUI, the distributed package as well as an outdated Saxon binding).

### Docker images for the Sirix HTTP(S)-Server / the REST-API
First, we need a running Keycloak server for now on port 8080.

As a Keycloak instance is needed for the RESTful-API we'll build a simple docker compose file maybe with a demo database user and some roles in the future.

For running a keycloak docker container you could for instance use the following docker command:
`docker run -d --name keycloak -p 8080:8080 -e KEYCLOAK_USER=admin -e KEYCLOAK_PASSWORD=admin -e KEYCLOAK_LOGLEVEL=DEBUG jboss/keycloak`. Afterwards it can be configured via a Web UI: http://localhost:8080. Keycloak is needed for our RESTful, asynchronous API. It is the authorization server instance.

Docker images of Sirix can be pulled from Docker Hub (sirixdb/sirix). However the easiest way for now is to download Sirix, then

1. Change into the sirix-rest-api bundle: `cd bundles/sirix-rest-api`
2. Change the configuration in `src/main/resources/sirix-conf.json` and add the secret from Keycloak (see for instance this great [tutorial](
https://piotrminkowski.wordpress.com/2017/09/15/building-secure-apis-with-vert-x-and-oauth2/) and change the HTTP(S)-Server port Sirix is listening on:

<img src="https://piotrminkowski.files.wordpress.com/2017/09/vertx-sec-3.png"/>

3. You can simply use the example `key.pem`/`cert.pem` files in `src/main/resources` for HTTPS (for example.org), but you have to change it, once we release the stable version for production. Then you for sure have to use a certificate/key for your domain. You could use [Let's Encrypt](https://letsencrypt.org/) for instance to get an SSL/TLS certificate for free.
4. Build the docker image: `docker build -t sirixdb/sirix`
5. Run the docker container: `docker run --network=host -t -i -p 9443:9443 sirixdb/sirix` (on Windows this does not seem to work)

Sirix should be up and running afterwards. Please let us know if you have any trouble setting it up.

### Command line tool
We ship a (very) simple command line tool for the sirix-xquery bundle:

Get the [latest sirix-xquery JAR](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/sirix-xquery/) with dependencies.

### First steps
Please have a look into our sirix-example project how to use Sirix from Java or have a look into the [documentation](https://sirix.io/documentation.html).

### Documentation
We are currently working on the documentation. You may find first drafts and snippets in the [documentation](https://sirix.io/documentation.html) and in this README. Furthermore you are kindly invited to ask any question you might have (and you likely have many questions) in the community forum (preferred) or in the Slack channel.
Please also have a look at and play with our sirix-example bundle which is available via maven or our new asynchronous RESTful API (shown next).

The following sections show different APIs to interact with Sirix.

## RESTful-API
We provide a simple, asynchronous RESTful-API. Authorization is done via OAuth2 (Password Credentials/Resource Owner Flow) using a Keycloak authorization server instance. Keycloak can be set up as described in this excellent [tutorial](
https://piotrminkowski.wordpress.com/2017/09/15/building-secure-apis-with-vert-x-and-oauth2/).

Have a look into our [REST-API documentation](https://sirix.io/rest-api.html) how to setup Keycloak and the non-blocking SirixDB HTTP-Server.

After Keycloak and our server are up and running, we can write a simple HTTP-Client. We first have to obtain a token from the `/login` endpoint with a given "username/password" JSON-Object. Using an asynchronous HTTP-Client (from Vert.x) in Kotlin, it looks like this:

```kotlin
val server = "https://localhost:9443"

val credentials = json {
  obj("username" to "testUser",
      "password" to "testPass")
}

val response = client.postAbs("$server/login").sendJsonAwait(credentials)

if (200 == response.statusCode()) {
  val user = response.bodyAsJsonObject()
  val accessToken = user.getString("access_token")
}
```

This access token must then be sent in the Authorization HTTP-Header for each subsequent request. Storing a first resource would look like (simple HTTP PUT-Request):

```kotlin
val xml = """
    <xml>
      foo
      <bar/>
    </xml>
""".trimIndent()

var httpResponse = client.putAbs("$server/database/resource1").putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml").putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

if (200 == response.statusCode()) {
  println("Stored document.")
} else {
  println("Something went wrong ${response.message}")
}
```
First, an empty database with the name `database` with some metadata is created, second the XML-fragment is stored with the name `resource1`. The PUT HTTP-Request is idempotent. Another PUT-Request with the same URL endpoint would just delete the former database and resource and create the database/resource again. Note that every request now has to contain an `HTTP-Header` which content type it sends and which resource-type it expects (`Content-Type: application/xml` and `Accept: application/xml`) for instance. This is needed as we now support the storage of and retrieval of XML or JSON-data. The following sections show the API for usage without binary and in-memory XML representation.

The HTTP-Response should be 200 and the HTTP-body yields:

```xml
<rest:sequence xmlns:rest="https://sirix.io/rest">
  <rest:item>
    <xml rest:id="1">
      foo
      <bar rest:id="3"/>
    </xml>
  </rest:item>
</rest:sequence>
```

We are serializing the generated IDs from our storage system for element-nodes.

Via a `GET HTTP-Request` to `https://localhost:9443/database/resource1` we are also able to retrieve the stored resource again.

However, this is not really interesting so far. We can update the resource via a `POST-Request`. Assuming we retrieved the access token as before, we can simply do a POST-Request and use the information we gathered before about the node-IDs:

```kotlin
val xml = """
    <test>
      yikes
      <bar/>
    </test>
""".trimIndent()

val url = "$server/database/resource1?nodeId=3&insert=asFirstChild"

val httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                         .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml").putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))
```

The interesting part is the URL, we are using as the endpoint. We simply say, select the node with the ID 3, then insert the given XML-fragment as the first child. This yields the following serialized XML-document:

```xml
<rest:sequence xmlns:rest="https://sirix.io/rest">
  <rest:item>
    <xml rest:id="1">
      foo
      <bar rest:id="3">
        <test rest:id="4">
          yikes
          <bar rest:id="6"/>
        </test>
      </bar>
    </xml>
  </rest:item>
</rest:sequence>
```
The interesting part is that every PUT- as well as POST-request does an implicit `commit` of the underlying transaction. Thus, we are now able send the first GET-request for retrieving the contents of the whole resource again for instance through specifying an simple XPath-query, to select the root-node in all revisions `GET https://localhost:9443/database/resource1?query=/xml/all-times::*` and get the following XPath-result:

```xml
<rest:sequence xmlns:rest="https://sirix.io/rest">
  <rest:item rest:revision="1" rest:revisionTimestamp="2018-12-20T18:44:39.464Z">
    <xml rest:id="1">
      foo
      <bar rest:id="3"/>
    </xml>
  </rest:item>
  <rest:item rest:revision="2" rest:revisionTimestamp="2018-12-20T18:44:39.518Z">
    <xml rest:id="1">
      foo
      <bar rest:id="3">
        <xml rest:id="4">
          foo
          <bar rest:id="6"/>
        </xml>
      </bar>
    </xml>
  </rest:item>
</rest:sequence>
```

In general we support several additional temporal XPath axis:

```xquery
future::
future-or-self::
past::
past-or-self::
previous::
previous-or-self::
next::
next-or-self::
first::
last::
all-time::
```

The same can be achieved through specifying a range of revisions to serialize (start- and end-revision parameters) in the GET-request:

```GET https://localhost:9443/database/resource1?start-revision=1&end-revision=2```

or via timestamps:

```GET https://localhost:9443/database/resource1?start-revision-timestamp=2018-12-20T18:00:00&end-revision-timestamp=2018-12-20T19:00:00```

We for sure are also able to delete the resource or any subtree thereof by an updating XQuery expression (which is not very RESTful) or with a simple `DELETE` HTTP-request:

```kotlin
val url = "$server/database/resource1?nodeId=3"

val httpResponse = client.deleteAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                         .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

if (200 == httpResponse.statusCode()) {
  ...
}
```

This deletes the node with ID 3 and in our case as it's an element node the whole subtree. For sure it's committed as revision 3 and as such all old revisions still can be queried for the whole subtree (or in the first revision it's only the element with the name "bar" without any subtree).

If we want to get a diff, currently in the form of an XQuery Update Statement (but we could serialize them in any format), simply call the XQuery function `sdb:diff`:

`sdb:diff($coll as xs:string, $res as xs:string, $rev1 as xs:int, $rev2 as xs:int) as xs:string`

For instance via a GET-request like this for the database/resource we created above, we could make this request:

`GET https://localhost:9443/?query=sdb%3Adiff%28%27database%27%2C%27resource1%27%2C1%2C2%29`

Note that the query-String has to be URL-encoded, thus it's decoded

`sdb:diff('database','resource1',1,2)`

The output for the diff in our example is this XQuery-Update statement wrapped in an enclosing sequence-element:

```xml
<rest:sequence xmlns:rest="https://sirix.io/rest">
  let $doc := sdb:doc('database','resource1', 1)
  return (
    insert nodes <xml>foo<bar/></xml> as first into sdb:select-node($doc, 3)
  )
</rest:sequence>
```

This means the `resource1` from `database` is opened in the first revision. Then the subtree `<xml>foo<bar/></xml>` is appended to the node with the stable node-ID 3 as a first child.

https://github.com/sirixdb/sirix/wiki/RESTful-API gives an overview about the API.

## DOM alike API
Think of this rather low level API as a persistent (in the sense of storing it to disk/a flash drive) DOM interface for Sirix, whereas nodes can be selected by a transactional cursor API by their unique identifier, which has been created during insertion with a sequence generator. Another DOM like API is available through our XQuery layer, which adds a simple Interface for in-memory node instances. However the low level API, which we are describing below doesn't have to have all nodes in-memory (and it usually doesn't). Nodes are fetched from variable length pages which have been either cached by a buffer manager in memory or reside on the flash drive or a spinning disk and have to be read from a file.

```java
// Path to the database.
var file = Paths.get("sirix-database");

// Create the database.
var config = new DatabaseConfiguration(file);
Databases.createXmlDatabase(config);

// Open the database.
try (var database = Databases.openXmlDatabase(file)) {
  /*
   * Create a resource in the database with the name "resource".
   * Store deweyIDs (hierarchical node labels), use text node compression,
   * build a summary of all paths in the resource and use the SLIDING_SNAPSHOT
   * versioning algorithm.
   */
  database.createResource(
            ResourceConfiguration.newBuilder("resource")
                                 .useDeweyIDs(true)
                                 .useTextCompression(true)
                                 .buildPathSummary(true)
                                 .versioningApproach(Versioning.SLIDING_SNAPSHOT)
                                 .build());
  try (
      // Start a resource manager on the given resource.
      var manager = database.openResourceManager("resource1");
      // Start the single read/write transaction.
      var wtx = manager.beginNodeTrx()) {
    // Import an XML-document.
    wtx.insertSubtreeAsFirstChild(XMLShredder.createFileReader(LOCATION.resolve("input.xml")));

    // Move to the node which automatically got the node-key 2 from Sirix during the import of the XML-document.
    wtx.moveTo(2);

    // Then move the subtree located at this node to the first child of node 4.
    wtx.moveSubtreeToFirstChild(4)

    // Get the name of the current node.
    final QName name = wtx.getName();

    // Get the value of the current node.
    final String value = wtx.getValue();

    // Commit revision 1.
    wtx.commit();

    // Reuse transaction handle and insert an element to the first child where the current transaction cursor resides.
    wtx.insertElementAsFirstChild(new QName("foo"));

    // Commit revision 2 with a commit message.
    wtx.commit("[MOD] Inserted another element.");

    // Serialize the revision back to XML.
    final OutputStream out = new ByteArrayOutputStream();
    new XmlSerializer.XmlSerializerBuilder(manager, out).prettyPrint().build().call();

    System.out.println(out);
  }
} catch (final SirixException | IOException | XMLStreamException e) {
  // LOG or do anything, the database is closed properly.
}
```

There are N reading transactions as well as one write-transaction permitted on a resource.

A read-only transaction can be opened through:

```java
var rtx = manager.beginNodeReadOnlyTrx()
```

The codè above starts a transaction on the most recent revision.

The following code starts a transaction at revision 1.

```java
var rtx = manager.beginNodeReadOnlyTrx(1)
```

The next read only transaction is going to be stared on the revision, which has been committed at the closest timestamp to the given point in time.

```java
var time = LocalDateTime.of(2018, Month.APRIL, 28, 23, 30);
var rtx = manager.beginNodeReadOnlyTrx(time.toInstant())
```

There are also several ways to start the single read/write-transaction:

```java
  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @return instance of class implementing the {@link XmNodeTrx} instance
   */
  XmlNodeTrx beginNodeTrx();

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @return instance of class implementing the {@link XmlNodeTrx} instance
   */
  XmlNodeTrx beginNodeTrx(@Nonnegative int maxNodes);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   * @return instance of class implementing the {@link XmlNodeTrx} instance
   */
  XmlNodeTrx beginNodeTrx(TimeUnit timeUnit, int maxTime);

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
   * @return instance of class implementing the {@link XdmNodeTrx} instance
   */
  XmlNodeTrx beginNodeTrx(@Nonnegative int maxNodes, TimeUnit timeUnit, int maxTime);
```

With <code>wtx.revertTo(int)</code> you're able to revert everything to an old revision (given by the integer). Followed by a commit the former version is commited as a new revision.

Use one of the provided axis to navigate through the DOM-like tree-structre (for instance in level order only through level 4):
```java
var axis = new LevelOrderAxis.Builder(rtx).includeSelf().filterLevel(4).build()
```
Post-order traversal:
```java
var axis = new PostOrderAxis(rtx)
```
And many more (for instance all XPath axis).

Or navigate to a specific node and then in time, for instance through all future revisions or all past revisions...:
```java
var axis = new FutureAxis<XmlNodeReadTrx>(rtx)
```
```java
var axis = new PastAxis<XmlNodeReadTrx>(rtx)
```

and many more as well.

Besides, we for instance provide diff-algorithms to import differences between several versions of (currently XML)-documents.

For instance after storing one revision in Sirix, we can import only the differences encountered by a sophisticated tree-to-tree diff-algorithm.

```java
var resOldRev = Paths.get("sirix-resource-to-update");
var resNewRev = Paths.get("new-revision-as-xml-file");

FMSEImport.xmlDataImport(resOldRev, resNewRev);
```

Furthermore we provide diff-algorithms to determine all differences between any two revisions once they are stored in Sirix. To enable a fast diff-algorithm we optionally store a merkle-tree (that is each node stores an additional hash-value).

In order to invoke a diff you either use with a resource-manager and an immutable set of observers (2 and 1 are the revision numbers to compare):

```java
DiffFactory.invokeFullDiff(
    new DiffFactory.Builder(resourceMgr, 2, 1, DiffOptimized.HASHED, ImmutableSet.of(observer)))
```

Or you invoke a structural diff, which does not check attributes or namespace-nodes:

```java
DiffFactory.invokeStructuralDiff(
    new DiffFactory.Builder(resourceMgr, 2, 1, DiffOptimized.HASHED, ImmutableSet.of(observer)))
```

An observer simply has to implement this interface:

```java
/**
 * Interface for observers, which are listening for diffs.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public interface DiffObserver {
  /**
   * Called for every node comparsion.
   *
   * @param diffType the {@link DiffType} type
   * @param newNodeKey node key of node in new revision
   * @param oldNodeKey node key of node in old revision
   * @param depth current {@link DiffDepth} instance
   */
  void diffListener(@Nonnull DiffType diffType, long newNodeKey, long oldNodeKey,
      @Nonnull DiffDepth depth);

  /** Signals that the diff calculation is done. */
  void diffDone();
}
```

On top of this API we built a brackit(.org) binding, which enables XQuery support as well as another DOM-alike API with DBNode-instances (in-memory) nodes (for instance `public DBNode getLastChild()`, `public DBNode getFirstChild()`, `public Stream<DBNode> getChildren()`...). You can also mix the APIs.

## Simple XQuery Examples
Test if fragments of the resource are not present in the past. In this example they are appended to a node in the most recent revision and stored in a subsequent revision)
```xquery
(* Loading document: *)
bit:load('mydoc.xml', '/tmp/sample8721713104854945959.xml')

(* Update loaded document: *)
let $doc := doc('mydoc.xml')
INSERT NODES <a><b/>test</a> INTO $doc/log

(* intermediate explicit commit *)
sdb:commit($doc)

(* Query loaded document: *)
doc('mydoc.xml')/log/all-time::*
(* First version: *)
<log tstamp="Fri Jun 14 07:59:08 CEST 2013" severity="low">
  <src>192.168.203.49</src>
  <msg>udic fyllwx abrjxa apvd</msg>
</log>
(* Second version: *)
<log tstamp="Fri Jun 14 07:59:08 CEST 2013" severity="low">
  <src>192.168.203.49</src>
  <msg>udic fyllwx abrjxa apvd</msg>
  <a>
    <b/>
    test
  </a>
</log>

(* Query loaded document (nodes, which are children of the log-element but did not exist in the past): *)
(* The second revision is initially loaded *)
doc('mydoc.xml', 2)/log/*[not(past::*)]
<a>
  <b/>
  test
</a>
```

Creation of a path index for all paths (note that we already can keep a path summary):

```java
// Create and commit path index on all elements.
try (final DBStore store = DBStore.newBuilder().build()) {
  final QueryContext ctx3 = new QueryContext(store);
  final XQuery q = new XQuery(new SirixCompileChain(store),
      "let $doc := sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
          + "let $stats := sdb:create-path-index($doc, '//*') "
          + "return <rev>{sdb:commit($doc)}</rev>");
  q.serialize(ctx3, System.out);
}
```

Temporal XPath axis extensions include:

```xquery
future::
future-or-self::
past::
past-or-self::
previous::
previous-or-self::
next::
next-or-self::
first::
last::
all-time::
```

Many more examples of creating name indexes, content and structure indexes and how to query them can be found in the examples module.

Have a look into the wiki for examples regarding a lower level (really powerful) cursor based API to navigate/and or modify  the tree structure or to navigate in time.

A lot of the ideas still stem from the Ph.D. thesis of Marc Kramis: Evolutionary Tree-Structured Storage: Concepts, Interfaces, and Applications

http://www.uni-konstanz.de/mmsp/pubsys/publishedFiles/Kramis2014.pdf

As well as from Sebastian Graft's work and thesis:

https://kops.uni-konstanz.de/handle/123456789/27250

## Getting Help

### Community Forum
Any questions or even consider to contribute or use Sirix? Use the [Community Forum](https://sirix.discourse.group) to ask questions. Any kind of question, may it be a API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

### Join us on Slack
You may find us on [Slack](https://sirixdb.slack.com) for quick questions.

## Visualizations (built on top of the cursor-based transaction API)
<p>The following diagram shows a screenshot of an interactive visualization, which depicts moves of single nodes or whole subtress through hierarchical edge bundling.</p>

<p align="center"><img src="https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/moves-cut.png"/></p>

A screencast is available depicting the SunburstView and the TextView side by side:
http://www.youtube.com/watch?v=l9CXXBkl5vI

<p>Currently, as we focused on various improvements in performance and features of the core storage system, the visualizations are a bit dated (and not working), but in the future we aim to bring them into the web (for instance using d3) instead of providing a standalone desktop GUI.</p>

## Why should you even bother?
Do you have to handle irregular data without knowing the schema before storing the data? You currently store this data in a relational DBMS? Maybe a tree-structured (XML or JSON) storage system much better suits your needs as it doesn't require a predefined schema before even knowing the structure of the data which has to be persisted.

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

Furthermore we aim to support an extended XDM in order to store JSON natively with additional node-types in Sirix. The implementation should be straight forward. Afterwards we'll explore how to efficiently distribute Sirix with Vert.x or directly via an Ignite or Hazelcast data grid.

Besides, the architecture for versioning data is not restricted to tree-structures by all means as demonstrated in the Ph.D. Thesis of Sebastian Graf (Sirix originated a few years ago as a fork of Treetank going back to its roots and focusing on the versioning of tree-structured data): http://nbn-resolving.de/urn:nbn:de:bsz:352-272505

Storing files natively is also on our agenda.

## Developers
Developers which are eager to put forth the idea of a versioned, secure database system especially suitable, but not restricted to rooted trees (serialized form as XML/JSON) are always welcome. The idea is not only to support (and extend querying) as for instance via XQuery efficiently, but also to support other data mining tasks such as the comparison of hierarchical tree-structures.

## More visualizations
![Wikipedia / SunburstView comparison mode / TextView comparison mode](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-scrolled.png "Wikipedia / SunburstView comparison mode / TextView comparison mode")
![Small Multiple Displays (incremental variant)](https://github.com/JohannesLichtenberger/sirix/raw/master/bundles/sirix-gui/src/main/resources/images/wikipedia-incremental.png "Small Multiple Displays (incremental variant)")

## Further information

Sirix was initially forked from Treetank (https://github.com/disy/treetank).
As such my deepest gratitude to all the other students who worked on the project.

First of all:

- Marc Kramis for his first drafts,
- Sebastian Graf for his almost complete rewrite of Treetank,
- Patrick Lang (RESTful API),
- Lukas Lewandowski (RESTful API),
- Tina Scherer (XPath engine)

and all the others who worked on the project.

## License

This work is released under the [BSD 3-clause license](LICENSE).
