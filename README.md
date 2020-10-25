<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/logo.png"/></p>

<h1 align="center">An Evolutionary, Accumulate-Only Database System</h1>
<p align="center">Stores small-sized, immutable snapshots of your data and facilitates querying the full history</p>

<p align="center">
    <a href="https://travis-ci.org/sirixdb/sirix" alt="Built Status"><img src="https://travis-ci.org/sirixdb/sirix.png"></img></a>
    <a href="#contributors-" alt="All Contributors"><img src="https://img.shields.io/badge/all_contributors-23-orange.svg?style=flat-square"></img></a>
    <a href="https://www.codefactor.io/repository/github/sirixdb/sirix" alt="Code Factor"><img src="ttps://www.codefactor.io/repository/github/sirixdb/sirix/badge"></img></a>
    <a href="http://makeapullrequest.com" alt="PRs Welcome"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square"></img></a>
    <a href="https://search.maven.org/search?q=g:io.sirix" alt="Maven Central"><img src="https://img.shields.io/maven-central/v/io.sirix/sirix-core.svg"></img></a>
    <a href="https://coveralls.io/github/sirixdb/sirix?branch=master" alt="Coverage Status"><img src="https://coveralls.io/repos/github/sirixdb/sirix/badge.svg?branch=master"></img></a>
</p>

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=SirixDB+-+a+storage+system%2C+which+creates+%28very+small-sized%29+snapshots+of+your+data+on+every+transaction-commit+through+the+implementation+of+a+novel+sliding+snapshot+algorithm.&url=http://sirix.io&via=sirix&hashtags=versioning,diffing,xml,kotlin,coroutines,vertx)

[![Follow](https://img.shields.io/twitter/follow/sirixdb.svg?style=social)](https://twitter.com/sirixdb)

[Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) | [Join us on Slack](https://join.slack.com/t/sirixdb/shared_invite/enQtNjI1Mzg4NTY4ODUzLTE3NmRhMWRiNWEzMjQ0NjAxNTZlODBhMTQzMWM2Nzc5MThkMjlmMzI0ODRlNGE0ZDgxNDcyODhlZDRhYjM2N2U) | [Community Forum](https://sirix.discourse.group/)

**Working on your first Pull Request?** You can learn how from this *free* series [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github) and another tutorial: [How YOU can contribute to OSS, a beginners guide](https://dev.to/itnext/how-you-can-contribute-to-oss-36id)

>"Remember that you're lucky, even if you don't think you are, because there's always something that you can be thankful for." - Esther Grace Earl (http://tswgo.org)

SirixDB uses a huge persistent (in the functional sense) tree of tries, wherein the committed snapshots share unchanged pages and even common records in changed pages. The system only stores page-fragments instead of full pages during a commit to reduce write-amplification. During read operations, the system reads the page-fragments in parallel to reconstruct an in-memory page.

SirixDB currently supports the storage and (time travel) querying of both XML - and JSON-data in our binary encoding, tailored to support versioning. The index-structures and the whole storage engine has been written from scratch to support versioning natively. We might also implement the storage and querying of other data formats as relational data.

<!--
<p>&nbsp;</p>

<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/showcase/screencast-three-revisions-faster.gif"/></p>

<p>&nbsp;</p>
-->
**Note: Work on a [Frontend](https://github.com/sirixdb/sirix-svelte-frontend) built with [Svelte](https://svelte.dev), [D3.js](https://d3js), and Typescript has just begun**

**Discuss it in the [Community Forum](https://sirix.discourse.group)**

## Table of contents
-   [Keeping All Versions of Your Data By Sharing Structure](#keeping-all-versions-of-your-data-by-sharing-structure)
-   [SirixDB Features](#sirixdb-features)
    -   [Design Goals](#design-goals)
    -   [Revision Histories](#revision-histories)
-   [Getting Started](#getting-started)
    -   [Download ZIP or Git Clone](#download-zip-or-git-clone)
    -   [Setup of the SirixDB HTTP-Server and Keycloak to use the REST-API](#setup-of-the-sirixdb-http-server-and-keycloak-to-use-the-rest-api)
    -   [Command line tool](#command-line-tool)
    -   [Documentation](#documentation)
-   [Getting Help](#getting-help)
    -   [Community Forum](#community-forum)
    -   [Join us on Slack](#join-us-on-slack)
-   [Contributors](#contributors-)
-   [License](#license)

## Keeping All Versions of Your Data By Sharing Structure
We could write quite a bunch of stuff, why it's often of great value to keep all states of your data in a storage system. Still, recently we stumbled across an excellent [blog post](https://www.hadoop360.datasciencecentral.com/blog/temporal-databases-why-you-should-care-and-how-to-get-started-par), which explains the advantages of keeping historical data very well. In a nutshell, it's all about looking at the evolution of your data, finding trends, doing audits, implementing efficient undo-/redo-operations. The [Wikipedia page](https://en.wikipedia.org/wiki/Temporal_database) has a bunch of examples. We recently also added use cases over [here](https://sirix.io/documentation.html).

Our firm belief is that a temporal storage system must address the issues, which arise from keeping past states way better than traditional approaches. Usually, storing time-varying, temporal data in database systems that do not support the storage thereof natively results in many unwanted hurdles. They waste storage space, query performance to retrieve past states of your data is not ideal, and usually, temporal operations are missing altogether.

The DBS must store data in a way that storage space is used as effectively as possible while supporting the reconstruction of each revision, as the database saw it during the commits. All this should be handled in linear time, whether it's the first revision or the most recent revision. Ideally, query time of old/past revisions and the most recent revision should be in the same runtime complexity (logarithmic when querying for specific records).

SirixDB not only supports snapshot-based versioning on a record granular level through a novel versioning algorithm called sliding snapshot, but also time travel queries, efficient diffing between revisions and the storage of semi-structured data to name a few.

Executing the following time-travel query to on our binary JSON representation of [Twitter sample data](https://raw.githubusercontent.com/sirixdb/sirix/master/bundles/sirix-core/src/test/resources/json/twitter.json) gives an initial impression of the possibilities:

```xquery
let $statuses := jn:open('mycol.jn','mydoc.jn', xs:dateTime('2019-04-13T16:24:27Z'))=>statuses
let $foundStatus := for $status in $statuses
  let $dateTimeCreated := xs:dateTime($status=>created_at)
  where $dateTimeCreated > xs:dateTime("2018-02-01T00:00:00") and not(exists(jn:previous($status)))
  order by $dateTimeCreated
  return $status
return {"revision": sdb:revision($foundStatus), $foundStatus{text}}
```

The query opens a database/resource in a specific revision based on a timestamp (`2019–04–13T16:24:27Z`) and searches for all statuses, which have a `created_at` timestamp, which has to be greater than the 1st of February in 2018 and did not exist in the previous revision. `=>` is a dereferencing operator used to dereference keys in JSON objects, array values can be accessed as shown with the function bit:array-values or through specifying an index, starting with zero: array[[0]] for instance specifies the first value of the array.

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
  <dd>Similarly to the file systems Btrfs and ZFS, SirixDB uses CoW semantics, meaning that SirixDB never overwrites data. Instead, database-page fragments are copied/written to a new location.</dd>
  <dt>Per revision and page versioning</dt>
  <dd>SirixDB does not only version on a per revision, but also on a per page-base. Thus, whenever we change a potentially small fraction
of records in a data-page, it does not have to copy the whole page and write it to a new location on a disk or flash drive. Instead, we can specify one of several versioning strategies known from backup systems or a novel sliding snapshot algorithm during the creation of a database resource. The versioning-type we specify is used by SirixDB to version data-pages.</dd>
  <dt>Guaranteed atomicity and consistency (without a WAL)</dt>
  <dd>The system will never enter an inconsistent state (unless there is hardware failure), meaning that unexpected power-off won't ever damage the system. This is accomplished without the overhead of a write-ahead-log. (<a
href="https://en.wikipedia.org/wiki/Write-ahead_logging">WAL</a>)</dd>
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

## Getting started

### [Download ZIP](https://github.com/sirixdb/sirix/archive/master.zip) or Git Clone

```
git clone https://github.com/sirixdb/sirix.git
```

or use the following dependencies in your Maven or Gradle project.

**SirixDB uses Java15, thus you need an up-to-date Gradle (if you want to work on SirixDB) and IntelliJ or Eclipse.**

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

<strong>Note that we changed the groupId from `com.github.sirixdb.sirix` to `io.sirix`. Most recent version is 0.9.6-SNAPSHOT.</strong>

Maven artifacts are deployed to the central maven repository (however please use the SNAPSHOT-variants as of now). Currently, the following artifacts are available:

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

### Setup of the SirixDB HTTP-Server and Keycloak to use the REST-API

The REST-API is asynchronous at its very core. We use Vert.x, which is a toolkit built on top of Netty. It is heavily inspired by Node.js but for the JVM. As such, it uses event loop(s), which is thread(s), which never should by blocked by long-running CPU tasks or disk-bound I/O. We are using Kotlin with coroutines to keep the code simple. SirixDB uses OAuth2 (Password Credentials/Resource Owner Flow) using a Keycloak authorization server instance.

### Start Docker Keycloak-Container using docker-compose
For setting up the SirixDB HTTP-Server and a basic Keycloak-instance with a test realm:

1. `git clone https://github.com/sirixdb/sirix.git`
2. `sudo docker-compose up keycloak`

### Keycloak setup

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
9. If "oAuthFlowType" is specified in the ame configuration file change the value to "PASSWORD" (if not default is "PASSWORD").
10. Regarding Keycloak the `direct access` grant on the settings tab must be `enabled`.
11. Our (user-/group-)roles are "create" to allow creating databases/resources, "view" to allow to query database resources, "modify" to modify a database resource and "delete" to allow deletion thereof. You can also assign `${databaseName}-` prefixed roles.
 
### Start the SirixDB HTTP-Server and the Keycloak-Container using docker-compose
The following command will start the docker container

1. `sudo docker-compose up`

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

Note that the following VM-parameters currently are needed: `-ea --enable-preview --add-modules=jdk.incubator.foreign`

### Command-line tool
We ship a (very) simple command-line tool for the sirix-xquery bundle:

Get the [latest sirix-xquery JAR](https://oss.sonatype.org/content/repositories/snapshots/io/sirix/sirix-xquery/) with dependencies.

### Documentation
We are currently working on the documentation. You may find first drafts and snippets in the [documentation](https://sirix.io/documentation.html) and in this README. Furthermore, you are kindly invited to ask any question you might have (and you likely have many questions) in the community forum (preferred) or in the Slack channel.
Please also have a look at and play with our sirix-example bundle which is available via maven or our new asynchronous RESTful API (shown next).

## Getting Help

### Community Forum
If you have any questions or are considering to contribute or use Sirix, please use the [Community Forum](https://sirix.discourse.group) to ask questions. Any kind of question, may it be an API-question or enhancement proposal, questions regarding use-cases are welcome... Don't hesitate to ask questions or make suggestions for improvements. At the moment also API-related suggestions and critics are of utmost importance.

### Join us on Slack
You may find us on [Slack](https://sirixdb.slack.com) for quick questions.

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
    <td align="center"><a href="https://github.com/add09"><img src="https://avatars3.githubusercontent.com/u/38160880?v=4" width="100px;" alt=""/><br /><sub><b>add09</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=add09" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/EmilGedda"><img src="https://avatars2.githubusercontent.com/u/4695818?v=4" width="100px;" alt=""/><br /><sub><b>Emil Gedda</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=EmilGedda" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/arohlen"><img src="https://avatars1.githubusercontent.com/u/49123208?v=4" width="100px;" alt=""/><br /><sub><b>Andreas Rohlén</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=arohlen" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/marcinbieleckiLLL"><img src="https://avatars3.githubusercontent.com/u/26444765?v=4" width="100px;" alt=""/><br /><sub><b>Marcin Bielecki</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=marcinbieleckiLLL" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/ManfredNentwig"><img src="https://avatars1.githubusercontent.com/u/164948?v=4" width="100px;" alt=""/><br /><sub><b>Manfred Nentwig</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ManfredNentwig" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/Raj-Datta-Manohar"><img src="https://avatars0.githubusercontent.com/u/25588557?v=4" width="100px;" alt=""/><br /><sub><b>Raj</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Raj-Datta-Manohar" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

Contributions of any kind are highly welcome!

## License

This work is released under the [BSD 3-clause license](LICENSE).
