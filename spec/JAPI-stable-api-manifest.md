# JAPI — Classified Java API manifest

Target: 1.0.0. **Draft declaration inventory with the decided three-tier boundary**,
governed by JAPI-101, JAPI-109 and JAPI-110. Source baseline:
`13f65148b117b9c8e1858a7f1510d2f65501bad9`. Classification below records the intended
1.0 compatibility surface; compiled signatures and transitive-type closure still need
verification before freezing the release artifact.

## Scope and classification (JAPI-101, JAPI-109)

| Tier | Contract |
|---|---|
| stable | Small user-facing core: database/resource opening, read/write transactions, revisions/time travel, diff, history, query entry points and configuration builders. The explicitly marked type/member signatures receive the 1.x compatibility guarantee. |
| expert | Explicit extension/callback APIs with weaker guarantees, outside the stable 1.x signature promise. Users must review release compatibility notes; exact expert deprecation/support windows remain to specify. |
| internal | Low-level page/storage/index/cursor implementation details and other unselected declarations. May change in any release, even when Java visibility is public. |

Each declaration below is labelled independently. A stable type name does **not**
promote all of its methods, nested types or public fields. Basic node navigation on
public read/write transactions is stable; page bindings, record objects, cache paths,
internal cursor implementations and storage/index-controller access are internal.
Visitor/axis extension APIs, transaction hooks and raw record-history callbacks are
labelled expert. Their weaker signature guarantees do not waive applicable behavioral
requirements such as TXN-801–804. Custom index hooks, when exposed as a supported
extension, belong in the labelled expert tier; this baseline has no separately
invented custom-index-hook API, and existing index controllers remain internal.

The explicit inventory includes current low-level declarations to make their exclusion
reviewable. It is not a package wildcard. Unlisted implementation declarations are
internal, not automatically stable because an object happens to expose them. Required
support types in stable signatures must receive an explicit limited support entry or
be removed from that signature before freezing; this transitive closure remains an
artifact-completion gap. External Brackit/JDK/JSON-library signatures must be pinned
where exposed without promising compatibility of those libraries' entire APIs.

The release requires a CI signature-compatibility check from 1.0 (japicmp-style),
including inherited/bridge signatures and exposed types. **No JPMS export lockdown is
required for 1.0**; module visibility is not the compatibility classification.

## Explicit type inventory (JAPI-101, JAPI-109)

Source SHA-256 pins each declaration source. The tier applies to the type identity;
individual members below may be weaker. This is not a compiled API artifact.

| Fully qualified type | Type tier | Source | Source SHA-256 |
|---|---|---|---|
| `io.sirix.api.Axis` | expert | [Axis.java](../bundles/sirix-core/src/main/java/io/sirix/api/Axis.java) | `8149e295380991fecb38863dc379cef97889c6f23dced1b5253ab51a6dbc8a45` |
| `io.sirix.api.Builder` | stable | [Builder.java](../bundles/sirix-core/src/main/java/io/sirix/api/Builder.java) | `1e8958688b8d6b5b584187161ba15b340af0c196143bd9e977f667883be5392a` |
| `io.sirix.api.Database` | stable | [Database.java](../bundles/sirix-core/src/main/java/io/sirix/api/Database.java) | `f05ff4c4d5f33f56a0328e96950bcc184e70590d3fd53f01fe4e552038975fa7` |
| `io.sirix.api.Expression` | internal | [Expression.java](../bundles/sirix-core/src/main/java/io/sirix/api/Expression.java) | `5a9a02afaa8819a9b44995d0bf535c2efcbaeacb6aedbaf65181df0357f8a042` |
| `io.sirix.api.Filter` | expert | [Filter.java](../bundles/sirix-core/src/main/java/io/sirix/api/Filter.java) | `8369107bd67a9987c1b85ff38ef4e611f9e4e21eb6221ea943828c5c4f5e7ead` |
| `io.sirix.api.ItemList` | internal | [ItemList.java](../bundles/sirix-core/src/main/java/io/sirix/api/ItemList.java) | `3970a10f05eba4504cd5cc29fd455d65f96e614b149882c72ea446802cb18b7b` |
| `io.sirix.api.JsonDiff` | stable | [JsonDiff.java](../bundles/sirix-core/src/main/java/io/sirix/api/JsonDiff.java) | `a792593b27516d93217f3dba5565de52e057fafbab604c0ec362d3cbd660fda2` |
| `io.sirix.api.Movement` | stable | [Movement.java](../bundles/sirix-core/src/main/java/io/sirix/api/Movement.java) | `390d33db6a09cb19c94c83563dcfc9eeef5eeafffe25e598cc847dd6375db92c` |
| `io.sirix.api.NodeCursor` | stable | [NodeCursor.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeCursor.java) | `e1936bfb304cbf6aa933959a11abfda8b508e00a2ac0b115626bb563dc7a1805` |
| `io.sirix.api.NodeReadOnlyTrx` | stable | [NodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeReadOnlyTrx.java) | `aad01e6a8cd4c80e1206369f9a9d5a66d0db4f93da469737eac6b21e092a2f53` |
| `io.sirix.api.NodeTrx` | stable | [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java) | `df74accc678b7d190c1c4ba7aa6ba1c0726b26cbbab83682a1a8e1c2bf2d99b4` |
| `io.sirix.api.PostCommitHook` | expert | [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java) | `56a830e10ceeaaff9466af87be1d404632b16653dcbd00bf2ce55cdc7f072e30` |
| `io.sirix.api.PreCommitHook` | expert | [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java) | `f439321ae821118266704dfa807bc293b5f15e0c3bb29cb8e5c64d3e61210a8f` |
| `io.sirix.api.RecordHistoryVisitor` | expert | [RecordHistoryVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/RecordHistoryVisitor.java) | `cb1bb9de1c97472f3871a99a2d24f9b6cfcfeec213aa4b83a88925a05f468d26` |
| `io.sirix.api.RecordRunVisitor` | expert | [RecordRunVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/RecordRunVisitor.java) | `df3e035a2bee35a8aac3a2dcc9bfda74bfa271fc059bcdc4b3f1b0c3c333d870` |
| `io.sirix.api.ResourceSession` | stable | [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java) | `abc6509338f967d0eef9e05bc513fabacf6252345bd9c9ac05b6e9932ff5dcc2` |
| `io.sirix.api.RevisionInfo` | stable | [RevisionInfo.java](../bundles/sirix-core/src/main/java/io/sirix/api/RevisionInfo.java) | `99730608dfa0c9f220695d1857aa314ca715d8bb66a1faca138a217bbe26beaa` |
| `io.sirix.api.SirixAxis` | expert | [SirixAxis.java](../bundles/sirix-core/src/main/java/io/sirix/api/SirixAxis.java) | `31c7981411ab28a693c301d86d0adef2ead62f6233d93aaf5e7e1a6c7801c38f` |
| `io.sirix.api.StorageEngineReader` | internal | [StorageEngineReader.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineReader.java) | `8ab08eb98512b7153f8e603aae7009729eccd75af302b7bedcdd3ddc1777c2d2` |
| `io.sirix.api.StorageEngineWriter` | internal | [StorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineWriter.java) | `59877fcd868ca33437d72e01e19a1e5b64c0fd9a395e053e1132a040251ebec3` |
| `io.sirix.api.Transaction` | internal | [Transaction.java](../bundles/sirix-core/src/main/java/io/sirix/api/Transaction.java) | `087b01babaddf7444d2cc38eb4e515362d47e884dd47e1e96b86934063496478` |
| `io.sirix.api.TransactionManager` | internal | [TransactionManager.java](../bundles/sirix-core/src/main/java/io/sirix/api/TransactionManager.java) | `f57a11b05cd671e7f1485db5f20015a5378cde37c127d57fbf0bdf099e65b429` |
| `io.sirix.api.json.JsonNodeReadOnlyTrx` | stable | [JsonNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeReadOnlyTrx.java) | `d4c8c67a6147f1a90611208c97ab9d6932bb730fac253c4f9b43779e0b155951` |
| `io.sirix.api.json.JsonNodeTrx` | stable | [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java) | `f8f7a449f11d93a466b5d909582cec09e2bc0432113b0107762a9c6e23f41879` |
| `io.sirix.api.json.JsonResourceSession` | stable | [JsonResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonResourceSession.java) | `0c596e4c622441b058924f1f73b09682aef8ec167ad1db992bfe72bf4ccf2fcf` |
| `io.sirix.api.visitor.JsonNodeVisitor` | expert | [JsonNodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/JsonNodeVisitor.java) | `d835da24ce9f4ce17f4b8fbeb02bacb84c6801c060a7f62b7c1d87c30b2c4856` |
| `io.sirix.api.visitor.NodeVisitor` | expert | [NodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/NodeVisitor.java) | `184a6b11382058530a3b951550f5d208daa1a11991e41e8554bd0e17a14a0910` |
| `io.sirix.api.visitor.VisitResult` | expert | [VisitResult.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/VisitResult.java) | `8f38a1174209b2bcfdf77555d5a1238bca42c1ed89ae42914dae1d4f12e70c8c` |
| `io.sirix.api.visitor.VisitResultType` | expert | [VisitResultType.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/VisitResultType.java) | `080cd417d13d2f00dbbfe0ba17a51443703bd13d81956e90026a2f0139b95d8e` |
| `io.sirix.api.visitor.XmlNodeVisitor` | expert | [XmlNodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/XmlNodeVisitor.java) | `bf2550de6bb5b838bde2cd0c93241f1f5593a568df501c9bc5a3b42181fc6f6f` |
| `io.sirix.api.xml.XmlNodeReadOnlyTrx` | stable | [XmlNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeReadOnlyTrx.java) | `cea62cc0a12636548bb44039bfd960caf11e23ddabc51115f3b966ab3017397d` |
| `io.sirix.api.xml.XmlNodeTrx` | stable | [XmlNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeTrx.java) | `a4db928a4209c9210d1e3c9dbac3d18a5d98a936bb6fd20faa228600f42388ac` |
| `io.sirix.api.xml.XmlResourceSession` | stable | [XmlResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlResourceSession.java) | `fa6715d6ca104bbfc1bd951386f07899f295ea20d728647fe8a613d60f5bbca3` |
| `io.sirix.access.Databases` | stable | [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java) | `9d9e542a788d79d99fe64894c010e51beb45b324e20ca3cf589c94f6695cab21` |
| `io.sirix.access.DatabaseConfiguration` | stable | [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java) | `2f8b937af3ed96f880979d831d786fc3d149d8054fc1b332d952b0763f1bd057` |
| `io.sirix.access.ResourceConfiguration` | stable | [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java) | `058321abf3bd4cffec458f48ee03c8107bedba893d4fd3e755dd7ddd3c3f863e` |
| `io.sirix.access.DatabaseType` | stable | [DatabaseType.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseType.java) | `18193378a90740232020c9d63a3fde32e9da3a457512ffbdeac6ecd3d4699290` |
| `io.sirix.access.User` | stable | [User.java](../bundles/sirix-core/src/main/java/io/sirix/access/User.java) | `641eff1314d825d395e9a5e8416fa4b5e1d404297179d6575f99460a75968857` |
| `io.sirix.access.trx.node.AfterCommitState` | stable | [AfterCommitState.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AfterCommitState.java) | `d47399134850df4088e1c3b80289ca858cb3211a6862ebb5e46c55a451c6e681` |
| `io.sirix.settings.VersioningType` | stable | [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java) | `922018c77ef96acd3ad36f365b33e935a99d53bc145ebb8c8fcc49e51af8aa84` |
| `io.sirix.io.StorageType` | stable | [StorageType.java](../bundles/sirix-core/src/main/java/io/sirix/io/StorageType.java) | `9f57b1590bafc518c3488cd0290b9be306a07b1ae8b25b024a50c261d5c0f2da` |
| `io.sirix.access.trx.node.HashType` | stable | [HashType.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/HashType.java) | `587105d629405c3bd35163e66da363f2d4b5bc38b92e857a61f77d892b6563df` |
| `io.sirix.exception.SirixException` | stable | [SirixException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixException.java) | `6d11a8d0b57076a6b3bdea704a17f8ad5f0d02cfa8024f9cb5237f5473a12062` |
| `io.sirix.exception.SirixUsageException` | stable | [SirixUsageException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixUsageException.java) | `b0eeff956e852e311e807a51aab8bbdf4c0dca1f52e493813a68a4f510a5fc15` |
| `io.sirix.exception.SirixIOException` | stable | [SirixIOException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixIOException.java) | `b189cb7a4f0e4351821972388f8da11d09c8e3b152e4b18834f28b26b736f9d7` |
| `io.sirix.exception.SirixRevisionPrunedException` | stable | [SirixRevisionPrunedException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixRevisionPrunedException.java) | `6c9334f9a82dde5bebc7659aa616465558342316594a0a6b66a6b941ff494265` |
| `io.sirix.exception.UnsupportedStoreFeatureException` | stable | [UnsupportedStoreFeatureException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/UnsupportedStoreFeatureException.java) | `9cf65f51ad6003e72283510449b27c46dc11f9ad54ae211a18a928c966a143ef` |
| `io.sirix.query.SirixQueryContext` | stable | [SirixQueryContext.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixQueryContext.java) | `00781b3879734a6ec48eccbba7703803343534b7f91a55b03a5ae7ed047d6c1c` |
| `io.sirix.query.SirixCompileChain` | stable | [SirixCompileChain.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixCompileChain.java) | `4dc1e47daced4b045793a897b8afa1866357e42d3c6771e41567261533667607` |
| `io.sirix.query.json.JsonDBStore` | stable | [JsonDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/JsonDBStore.java) | `8ff003ee1fc93a0823ccb4d39488eaf9a089c62e259a095962f13c2c2012de63` |
| `io.sirix.query.node.XmlDBStore` | stable | [XmlDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/XmlDBStore.java) | `b6b5486cd13c543bc32fd186418dec82ac080aebb992464b989a7db6fd137005` |
| `io.sirix.query.json.BasicJsonDBStore` | stable | [BasicJsonDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/BasicJsonDBStore.java) | `07e338104621251c568c9974361e5dbdbca9fe86b804b5028a77e904c511ce29` |
| `io.sirix.query.node.BasicXmlDBStore` | stable | [BasicXmlDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/BasicXmlDBStore.java) | `137675e3109b06e8c82483b0f9a8c2bc34559f8e5a59bdedd683649e2afd0307` |
| `io.sirix.query.json.JsonDBCollection` | stable | [JsonDBCollection.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/JsonDBCollection.java) | `2a9e90c550a2db649caa7dd7d2ab5e8c6381f5e93565872397d12a992d86cfbc` |
| `io.sirix.query.node.XmlDBCollection` | stable | [XmlDBCollection.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/XmlDBCollection.java) | `37d98e0c99a4c50ec0f9ca465e8973dd8094fe1762e14c84c49c02228e651dd4` |
| `io.sirix.service.json.BasicJsonDiff` | stable | [BasicJsonDiff.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/BasicJsonDiff.java) | `9a2a113efd981f43fcae5fca950650c7e13fb07651d92d455e069b40b3737b6a` |
| `io.sirix.diff.DiffFactory` | stable | [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java) | `632521d62386527ec2ca8ad993dae042cc1856f79726ed3d86b92c5e8c7edbcb` |
| `io.sirix.diff.DiffObserver` | stable | [DiffObserver.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffObserver.java) | `2bf91270d9b62a6d48bec6da12f8e3099ea1492e68fc2ef757a36cfe1f961375` |
| `io.sirix.diff.DiffDepth` | stable | [DiffDepth.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffDepth.java) | `8759ee1718d634721faabf4673c9547af3471f8979a818468936bf03e54279fe` |

## Declared API signatures (JAPI-101)

Parsed from declarations, grouped by owning type. `enum NAME(args)` records an enum
constant (arguments document the current declared value); it is signature notation,
not compilable Java. Bodies and comments are omitted. Nested public/protected types
are separate owners. Implicit-public interface members are included; private interface
helpers and anonymous enum-constant implementations are excluded. Record headers carry
their component list; compiler-generated accessors/constructors and inherited members
need the planned compiled signature artifact before freeze.

### io.sirix.api.Axis

Source: [Axis.java](../bundles/sirix-core/src/main/java/io/sirix/api/Axis.java).

```text
[expert] public interface Axis extends LongIterable, LongIterator, SirixAxis
[expert] NodeCursor getCursor();
[expert] XmlNodeReadOnlyTrx asXmlNodeReadTrx();
[expert] void foreach(XmlNodeVisitor visitor);
[expert] long nextNode();
[expert] void reset(long nodeKey);
[expert] IncludeSelf includeSelf();
[expert] long peek();
[expert] long getStartKey();
[expert] PathSummaryReader asPathSummary();
[expert] <T extends NodeReadOnlyTrx & NodeCursor> T getTrx();
[expert] JsonNodeReadOnlyTrx asJsonNodeReadTrx();
```

### io.sirix.api.Builder

Source: [Builder.java](../bundles/sirix-core/src/main/java/io/sirix/api/Builder.java).

```text
[stable] public interface Builder<T>
[stable] T build();
```

### io.sirix.api.Database

Source: [Database.java](../bundles/sirix-core/src/main/java/io/sirix/api/Database.java).

```text
[stable] public interface Database<T extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> extends AutoCloseable
[stable] boolean isOpen();
[stable] boolean createResource(ResourceConfiguration config);
[stable] boolean existsResource(String resourceName);
[stable] List<Path> listResources();
[stable] T beginResourceSession(String resourceName);
[stable] Database<T> removeResource(String resourceName);
[stable] @Override void close();
[stable] DatabaseConfiguration getDatabaseConfig();
[internal] Transaction beginTransaction();
[stable] String getResourceName(long id);
[stable] long getResourceID(String name);
[stable] String getName();
```

### io.sirix.api.Expression

Source: [Expression.java](../bundles/sirix-core/src/main/java/io/sirix/api/Expression.java).

```text
[internal] @FunctionalInterface public interface Expression
[internal] void evaluate() throws SirixXPathException;
```

### io.sirix.api.Filter

Source: [Filter.java](../bundles/sirix-core/src/main/java/io/sirix/api/Filter.java).

```text
[expert] public interface Filter<R>
[expert] boolean filter();
[expert] R getTrx();
[expert] void setTrx(R rtx);
```

### io.sirix.api.ItemList

Source: [ItemList.java](../bundles/sirix-core/src/main/java/io/sirix/api/ItemList.java).

```text
[internal] public interface ItemList<T extends Node>
[internal] int addItem(final T pNode);
[internal] T getItem(final long pKey);
[internal] int size();
```

### io.sirix.api.JsonDiff

Source: [JsonDiff.java](../bundles/sirix-core/src/main/java/io/sirix/api/JsonDiff.java).

```text
[stable] public interface JsonDiff
[stable] String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber);
[stable] String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber, long startNodeKey, long maxDepth);
```

### io.sirix.api.Movement

Source: [Movement.java](../bundles/sirix-core/src/main/java/io/sirix/api/Movement.java).

```text
[stable] public enum Movement
[stable] enum TOPARENT
[stable] enum NONE
```

### io.sirix.api.NodeCursor

Source: [NodeCursor.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeCursor.java).

```text
[stable] public interface NodeCursor extends AutoCloseable
[stable] boolean moveTo(long key);
[stable] boolean moveToNextFollowing();
[stable] boolean moveToDocumentRoot();
[stable] boolean moveToParent();
[stable] boolean moveToFirstChild();
[stable] boolean moveToLastChild();
[stable] boolean moveToLeftSibling();
[stable] boolean moveToRightSibling();
[stable] boolean moveToPrevious();
[stable] boolean moveToNext();
[stable] boolean hasNode(long key);
[stable] boolean hasParent();
[stable] boolean hasFirstChild();
[stable] boolean hasLastChild();
[stable] boolean hasLeftSibling();
[stable] boolean hasRightSibling();
[stable] long getLeftSiblingKey();
[stable] long getRightSiblingKey();
[stable] long getFirstChildKey();
[stable] long getLastChildKey();
[stable] long getParentKey();
[internal] ImmutableNode getNode();
[stable] long getNodeKey();
[stable] NodeKind getRightSiblingKind();
[stable] NodeKind getLeftSiblingKind();
[stable] NodeKind getFirstChildKind();
[stable] NodeKind getLastChildKind();
[stable] NodeKind getParentKind();
[stable] NodeKind getKind();
```

### io.sirix.api.NodeReadOnlyTrx

Source: [NodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeReadOnlyTrx.java).

```text
[stable] public interface NodeReadOnlyTrx extends AutoCloseable
[stable] int getId();
[stable] int getRevisionNumber();
[stable] Instant getRevisionTimestamp();
[stable] long getMaxNodeKey();
[stable] @Override void close();
[stable] long getNodeKey();
[stable] ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession();
[stable] CommitCredentials getCommitCredentials();
[stable] boolean moveTo(long key);
[internal] StorageEngineReader getStorageEngineReader();
[internal] long getPathNodeKey();
[internal] int keyForName(String name);
[internal] String nameForKey(int key);
[stable] long getDescendantCount();
[stable] long getChildCount();
[internal] NodeKind getPathKind();
[stable] boolean isDocumentRoot();
[stable] boolean isClosed();
[stable] QNm getName();
[stable] boolean hasChildren();
[stable] long getHash();
[stable] String getValue();
[stable] default byte[] getValueBytes();
[stable] default boolean valueEquals(final byte[] expected);
[stable] Optional<User> getUser();
[internal] boolean storeDeweyIDs();
[internal] SirixDeweyID getDeweyID();
[stable] int getPreviousRevisionNumber();
```

### io.sirix.api.NodeTrx

Source: [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java).

```text
[stable] public interface NodeTrx extends NodeReadOnlyTrx
[stable] default NodeTrx commit();
[stable] default NodeTrx commit(@Nullable String commitMessage);
[stable] NodeTrx commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp);
[stable] NodeTrx rollback();
[internal] void markRollbackOnly(Throwable cause);
[stable] NodeTrx revertTo(int revision);
[expert] NodeTrx addPreCommitHook(PreCommitHook hook);
[expert] NodeTrx addPostCommitHook(PostCommitHook hook);
[expert] default void runLocked(Runnable work);
[expert] void awaitPendingAsyncCommit();
[internal] long getMutationSequence();
[internal] default long getStructuralMutationSequence();
[stable] NodeTrx truncateTo(int revision);
[internal] PathSummaryReader getPathSummary();
[stable] @Override void close();
[internal] StorageEngineWriter getStorageEngineWriter();
[stable] Optional<User> getUserOfRevisionToRepresent();
[stable] NodeTrx remove();
```

### io.sirix.api.PostCommitHook

Source: [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java).

```text
[expert] @FunctionalInterface public interface PostCommitHook
[expert] void postCommit(NodeReadOnlyTrx rtx);
```

### io.sirix.api.PreCommitHook

Source: [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java).

```text
[expert] @FunctionalInterface public interface PreCommitHook
[expert] void preCommit(NodeReadOnlyTrx rtx);
```

### io.sirix.api.RecordHistoryVisitor

Source: [RecordHistoryVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/RecordHistoryVisitor.java).

```text
[expert] @FunctionalInterface public interface RecordHistoryVisitor
[expert] void visit(int revision, DataRecord record);
```

### io.sirix.api.RecordRunVisitor

Source: [RecordRunVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/RecordRunVisitor.java).

```text
[expert] @FunctionalInterface public interface RecordRunVisitor
[expert] void visit(int fromRevision, int toRevision, DataRecord record);
```

### io.sirix.api.ResourceSession

Source: [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java).

```text
[stable] public interface ResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> extends AutoCloseable
[internal] default @Nullable StorageEngineReader openDerivedIndexReader(final StorageEngineReader source);
[internal] default @Nullable ValidTimeReadCache getValidTimeReadCache();
[stable] Path getResourcePath();
[stable] List<RevisionInfo> getHistory();
[stable] List<RevisionInfo> getHistory(int revisions);
[stable] List<RevisionInfo> getHistory(int fromRevision, int toRevision);
[stable] long[] getHistoryTimestamps();
[stable] long[] getHistoryTimestamps(int fromRevision, int toRevision);
[stable] int[] getRecordChangeRevisions(long nodeKey);
[expert] void scanRecordHistory(long nodeKey, RecordHistoryVisitor visitor);
[expert] void scanValueRuns(long nodeKey, RecordRunVisitor visitor);
[stable] Optional<W> getNodeTrx();
[internal] int activeTrxCount();
[internal] default StorageEngineReader createStorageEngineReader();
[internal] StorageEngineReader createStorageEngineReader(int revision);
[internal] default StorageEngineWriter createStorageEngineWriter();
[stable] default R beginNodeReadOnlyTrx();
[internal] StorageEngineWriter createStorageEngineWriter(int revision);
[stable] R beginNodeReadOnlyTrx(int revision);
[stable] R beginNodeReadOnlyTrx(Instant pointInTime);
[stable] W beginNodeTrx(AfterCommitState afterCommitState);
[stable] W beginNodeTrx(int maxNodes, AfterCommitState afterCommitState);
[stable] W beginNodeTrx(int maxTime, TimeUnit timeUnit, AfterCommitState afterCommitState);
[stable] W beginNodeTrx(int maxNodes, int maxTime, TimeUnit timeUnit, AfterCommitState afterCommitState);
[stable] W beginNodeTrx();
[stable] W beginNodeTrx(int maxNodes);
[stable] W beginNodeTrx(int maxTime, TimeUnit timeUnit);
[stable] W beginNodeTrx(int maxNodeCount, int maxTime, TimeUnit timeUnit);
[internal] PathSummaryReader openPathSummary(int revision);
[internal] default PathSummaryReader openPathSummary();
[stable] int getRevisionNumber(Instant pointInTime);
[stable] int getRevisionNumber(Instant pointInTime, int revisionCeiling);
[stable] @Override void close();
[stable] boolean isClosed();
[stable] int getMostRecentRevisionNumber();
[stable] ResourceConfiguration getResourceConfig();
[internal] <C extends IndexController<R, W>> C getRtxIndexController(int revision);
[internal] <C extends IndexController<R, W>> C getWtxIndexController(int revision);
[internal] Optional<R> getNodeReadTrxByTrxId(Integer ID);
[internal] boolean hasRunningNodeWriteTrx();
[stable] Optional<User> getUser();
[internal] R getOrCreateSharedReadOnlyTrx(int revision);
[internal] void closeSharedReadOnlyTrxs(int revision);
```

### io.sirix.api.RevisionInfo

Source: [RevisionInfo.java](../bundles/sirix-core/src/main/java/io/sirix/api/RevisionInfo.java).

```text
[stable] public final class RevisionInfo
[stable] public RevisionInfo(final User user, final int revision, final Instant revisionTimestamp, final String commitMessage);
[stable] public User getUser();
[stable] public int getRevision();
[stable] public Instant getRevisionTimestamp();
[stable] public Optional<String> getCommitMessage();
[stable] @Override public int hashCode();
[stable] @Override public boolean equals(final Object other);
[stable] @Override public String toString();
```

### io.sirix.api.SirixAxis

Source: [SirixAxis.java](../bundles/sirix-core/src/main/java/io/sirix/api/SirixAxis.java).

```text
[expert] public interface SirixAxis

```

### io.sirix.api.StorageEngineReader

Source: [StorageEngineReader.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineReader.java).

```text
[internal] public interface StorageEngineReader extends AutoCloseable
[internal] default @Nullable DataRecord cachedProjectionDictionaryRecord(final long key);
[internal] default void cacheProjectionDictionaryRecord(final long key, final DataRecord record);
[internal] default void evictProjectionDictionaryRecord(final long key);
[internal] BufferManager getBufferManager();
[internal] UberPage getUberPage();
[internal] ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession();
[internal] boolean hasTrxIntentLog();
[internal] StorageEngineReader getTransactionView();
[internal] default @Nullable StorageEngineReader getDerivedIndexReader();
[internal] RecordPageGuard preserveRecordPageGuard();
[internal] int getTrxId();
[internal] long getDatabaseId();
[internal] long getResourceId();
[internal] CommitCredentials getCommitCredentials();
[internal] <V extends DataRecord> V getRecord(long key, IndexType indexType, int index);
[internal] default void ensureFsstSymbolTable(KeyValueLeafPage page);
[internal] RevisionRootPage getActualRevisionRootPage();
[internal] String getName(int nameKey, NodeKind recordKind);
[internal] int getNameCount(int nameKey, NodeKind recordKind);
[internal] byte[] getRawName(int nameKey, NodeKind recordKind);
[internal] @Override void close();
[internal] NodeStorageEngineReader.PageReferenceToPage getRecordPage(IndexLogKey indexLogKey);
[internal] default @Nullable RegionsOnlyPage getRecordPageRegionsOnly(IndexLogKey indexLogKey, int regionKindMask, int regionDeferMask);
[internal] default RegionsOnlyPage @Nullable [] getRecordPageFragmentRegions(IndexLogKey indexLogKey, int regionKindMask);
[internal] default byte @Nullable [] fsstSymbolTable(long id);
[internal] boolean isClosed();
[internal] int getRevisionNumber();
[internal] static int recordPageOffset(long key);
[internal] long pageKey(long recordKey, IndexType indexType);
[internal] NamePage getNamePage(RevisionRootPage revisionRoot);
[internal] PathPage getPathPage(RevisionRootPage revisionRoot);
[internal] CASPage getCASPage(RevisionRootPage revisionRoot);
[internal] PathSummaryPage getPathSummaryPage(RevisionRootPage revisionRoot);
[internal] DeweyIDPage getDeweyIDPage(RevisionRootPage revisionRoot);
[internal] VectorPage getVectorPage(RevisionRootPage revisionRoot);
[internal] ProjectionIndexPage getProjectionIndexPage(RevisionRootPage revisionRoot);
[internal] ValidTimeIndexPage getValidTimeIndexPage(RevisionRootPage revisionRoot);
[internal] TemporalAggregateIndexPage getTemporalAggregateIndexPage(RevisionRootPage revisionRoot);
[internal] ChangeLogIndexPage getChangeLogIndexPage(RevisionRootPage revisionRoot);
[internal] @Nullable PageReference getReferenceToLeafOfSubtree(PageReference startReference, long pageKey, int indexNumber, IndexType indexType, RevisionRootPage revisionRootPage);
[internal] Reader getReader();
[internal] IndirectPage dereferenceIndirectPageReference(PageReference indirectPageReference);
[internal] RevisionRootPage loadRevRoot(int lastCommitedRev);
[internal] int getCurrentMaxIndirectPageTreeLevel(IndexType indexType, int index, RevisionRootPage revisionRootPage);
[internal] DataRecord getValue(KeyValuePage<? extends DataRecord> page, long nodeKey);
[internal] HOTLeafPage getHOTLeafPage(IndexType indexType, int indexNumber);
[internal] @Nullable Page loadHOTPage(PageReference reference);
[internal] @Nullable Page loadHOTPageAndGuard(PageReference reference);
[internal] default @Nullable HOTLeafEntry readHOTProjectionEntry(final PageReference reference, final byte[] key, final long sideReferenceKey);
[internal] default @Nullable HOTLeafEntry readHOTProjectionEntry(final PageReference reference, final byte[] key, final long sideReferenceKey, final HOTReadIntent intent);
[internal] default @Nullable HOTLeafEntry readHOTProjectionEntry(final PageReference reference, final byte[] key, final long sideReferenceKey, final HOTReadIntent intent, final @Nullable ReadScope scope);
[internal] List<HOTLeafPage> loadHOTLeafFragments(PageReference chainRef);
[internal] void releaseHOTLeafFragments(List<HOTLeafPage> fragments, @Nullable HOTLeafPage keepOpen);
[internal] default @Nullable OverflowPage readSideOverflowPage(PageReference reference);
[internal] default OverflowPage @Nullable [] readSideOverflowPageBatch(long[] offsets);
[internal] @Nullable PageReference getLeafPageReference(long recordPageKey, int indexNumber, IndexType indexType);
[internal] default void prefetchRecordPages(final long[] recordPageKeys, final int count, final IndexType indexType);
[internal] default int recordPagePrefetchBatch();
[internal] default void prefetchPageSpans(final PageReference[] references, final int count);
```

### io.sirix.api.StorageEngineReader.RecordPageGuard

Source: [StorageEngineReader.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineReader.java).

```text
[internal] interface RecordPageGuard extends AutoCloseable
[internal] @Override void close();
```

### io.sirix.api.StorageEngineWriter

Source: [StorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineWriter.java).

```text
[internal] public interface StorageEngineWriter extends StorageEngineReader
[internal] BytesOut<?> newBufferedBytesInstance();
[internal] StorageEngineWriter truncateTo(int revision);
[internal] StorageEngineWriter appendLogRecord(PageReference reference, PageContainer page);
[internal] <V extends DataRecord> V createRecord(V record, IndexType indexType, int index);
[internal] <V extends DataRecord> V prepareRecordForModification(long key, IndexType indexType, int index);
[internal] @SuppressWarnings("unchecked") default <V extends DataRecord> V prepareRecordForModificationDocument(final long key);
[internal] void persistRecord(DataRecord record, IndexType indexType, int index);
[internal] default byte[] encodeStringValueForInsert(KeyValueLeafPage page, byte[] value, int off, int len);
[internal] default void addNameCount(int key, int delta, NodeKind nodeKind);
[internal] default void adoptDocumentLeafPage(KeyValueLeafPage page);
[internal] default void installDocumentStringDictionaries(@Nullable GlobalStringDictionaries dictionaries);
[internal] default void installDocumentStringDictionaryFactory(@Nullable LongFunction<GlobalStringDictionaries> factory);
[internal] default void installDocumentPageEncodedListener(@Nullable LongConsumer listener);
[internal] default void installEncodePassCompleteListener(@Nullable Runnable listener);
[internal] default void installLiveDocumentStringReadView(@Nullable GlobalStringDictionaries live);
[internal] default void stageOverflowCarriersOfLiveLeaf(KeyValueLeafPage page);
[internal] default KeyValueLeafPage prepareDocumentLeafForBlit(long recordPageKey);
[internal] void removeRecord(long key, IndexType indexType, int index);
[internal] int createNameKey(String name, NodeKind kind);
[internal] int keyForName(String name, NodeKind kind);
[internal] default UberPage commit();
[internal] default UberPage commit(@Nullable String commitMessage);
[internal] default UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp);
[internal] default UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isAutoCommitting);
[internal] UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isAutoCommitting, boolean isIntermediateCommit);
[internal] void commit(PageReference reference);
[internal] default boolean stageUncommittedOverflowPage(final PageReference reference);
[internal] default boolean stageUncommittedOverflowPage(final PageReference reference, final long localityGroup);
[internal] PageContainer dereferenceRecordPageForModification(PageReference reference);
[internal] default void setWriteSingletonBinder(final WriteSingletonBinder binder);
[internal] void markTransactionRollbackOnly(Throwable cause);
[internal] void assertTransactionWritable();
[internal] <P extends Page> P prepareSecondaryIndexPage(IndexType indexType);
[internal] default void allocateForDocumentCreation();
[internal] default KeyValueLeafPage getAllocKvl();
[internal] default int getAllocSlotOffset();
[internal] default long getAllocNodeKey();
[internal] default void asyncFlush();
[internal] default void setStorageEpochSpilling(final boolean enabled);
[internal] default void asyncFlushStorageEpoch();
[internal] default void captureRecordForDeferredRead(final long recordRootKey);
[internal] default boolean isAsyncFlushLogBoundaryReached();
[internal] default boolean isStorageEpochLogBoundaryReached();
[internal] default void recordAsyncFlushForegroundNanos(final long elapsedNanos);
[internal] default UberPage commitWritePages(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isIntermediateCommit);
[internal] default void hardenCommit(UberPage uberPage, boolean isIntermediateCommit);
[internal] default void awaitPendingAsyncFlush();
[internal] StorageEngineReader getStorageEngineReader();
[internal] UberPage rollback();
[internal] PageContainer getLogRecord(PageReference reference);
[internal] TransactionIntentLog getLog();
[internal] int getRevisionToRepresent();
[internal] PageGuard acquireGuardForNode(long nodeKey);
[internal] default @Nullable KeyValueLeafPage getModifiedPageForRead(long recordPageKey, IndexType indexType, int index);
[internal] default boolean isReadOnlyPageForRead(KeyValueLeafPage page);
[internal] default @Nullable DataRecord getDetachedRecordForRead(KeyValueLeafPage page, long recordKey);
[internal] default void releasePageForRead(@Nullable KeyValueLeafPage page);
```

### io.sirix.api.StorageEngineWriter.WriteSingletonBinder

Source: [StorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineWriter.java).

```text
[internal] @FunctionalInterface interface WriteSingletonBinder
[internal] DataRecord bind(KeyValueLeafPage page, int offset, long nodeKey);
```

### io.sirix.api.Transaction

Source: [Transaction.java](../bundles/sirix-core/src/main/java/io/sirix/api/Transaction.java).

```text
[internal] public interface Transaction extends AutoCloseable
[internal] Transaction commit();
[internal] Transaction rollback();
[internal] long getId();
[internal] @Override void close();
[internal] Transaction add(NodeTrx nodeTrx);
```

### io.sirix.api.TransactionManager

Source: [TransactionManager.java](../bundles/sirix-core/src/main/java/io/sirix/api/TransactionManager.java).

```text
[internal] public interface TransactionManager extends AutoCloseable
[internal] Transaction beginTransaction();
[internal] TransactionManager closeTransaction(Transaction trx);
[internal] @Override void close();
```

### io.sirix.api.json.JsonNodeReadOnlyTrx

Source: [JsonNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeReadOnlyTrx.java).

```text
[stable] public interface JsonNodeReadOnlyTrx extends NodeCursor, NodeReadOnlyTrx
[stable] @Override String getValue();
[stable] boolean isObject();
[stable] boolean isObjectKey();
[stable] boolean isArray();
[stable] boolean isStringValue();
[stable] boolean isNumberValue();
[stable] boolean isNullValue();
[stable] boolean isBooleanValue();
[stable] @Override boolean moveTo(long nodeKey);
[stable] @Override boolean moveToDocumentRoot();
[stable] @Override boolean moveToFirstChild();
[stable] @Override boolean moveToLastChild();
[stable] @Override boolean moveToLeftSibling();
[stable] @Override boolean moveToParent();
[stable] @Override boolean moveToRightSibling();
[stable] @Override boolean moveToPrevious();
[stable] @Override boolean moveToNext();
[stable] @Override boolean moveToNextFollowing();
[stable] VisitResult acceptVisitor(JsonNodeVisitor visitor);
[stable] @Override JsonResourceSession getResourceSession();
[stable] boolean getBooleanValue();
[stable] Number getNumberValue();
[internal] int getNameKey();
[stable] default byte[] getNameBytes();
[internal] boolean isFusedSyntheticChild();
[stable] List<JsonObject> getUpdateOperations();
[stable] List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth);
```

### io.sirix.api.json.JsonNodeTrx

Source: [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java).

```text
[stable] public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx
[stable] JsonNodeTrx copySubtreeAsFirstChild(JsonNodeReadOnlyTrx rtx);
[stable] JsonNodeTrx copySubtreeAsLeftSibling(JsonNodeReadOnlyTrx rtx);
[stable] JsonNodeTrx copySubtreeAsRightSibling(JsonNodeReadOnlyTrx rtx);
[stable] JsonNodeTrx copyNodeWithKey(JsonNodeReadOnlyTrx rtx, InsertPosition position);
[stable] JsonNodeTrx insertObjectAsFirstChild();
[stable] JsonNodeTrx insertObjectAsLastChild();
[stable] JsonNodeTrx insertObjectAsLeftSibling();
[stable] JsonNodeTrx insertObjectAsRightSibling();
[stable] JsonNodeTrx insertObjectRecordAsFirstChild(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertObjectRecordAsLastChild(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertObjectRecordAsLeftSibling(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertObjectRecordAsRightSibling(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertObjectRecordWithPrimitiveAsFirstChild(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertObjectRecordWithPrimitiveAsRightSibling(String key, ObjectRecordValue<?> value);
[stable] JsonNodeTrx insertArrayAsFirstChild();
[stable] JsonNodeTrx insertArrayAsLastChild();
[stable] JsonNodeTrx insertArrayAsLeftSibling();
[stable] JsonNodeTrx insertArrayAsRightSibling();
[stable] JsonNodeTrx replaceObjectRecordValue(ObjectRecordValue<?> value);
[stable] JsonNodeTrx setObjectKeyName(String key);
[stable] JsonNodeTrx setStringValue(String value);
[stable] JsonNodeTrx setBooleanValue(boolean value);
[stable] JsonNodeTrx setNumberValue(Number value);
[stable] JsonNodeTrx remove();
[stable] JsonNodeTrx moveSubtreeToFirstChild(long fromKey);
[stable] JsonNodeTrx moveSubtreeToRightSibling(long fromKey);
[stable] JsonNodeTrx moveSubtreeToLeftSibling(long fromKey);
[stable] JsonNodeTrx insertStringValueAsFirstChild(String value);
[stable] JsonNodeTrx insertStringValueAsLastChild(String value);
[stable] JsonNodeTrx insertStringValueAsLeftSibling(String value);
[stable] JsonNodeTrx insertStringValueAsRightSibling(String value);
[stable] JsonNodeTrx insertBooleanValueAsFirstChild(boolean value);
[stable] JsonNodeTrx insertBooleanValueAsLastChild(boolean value);
[stable] JsonNodeTrx insertBooleanValueAsLeftSibling(boolean value);
[stable] JsonNodeTrx insertBooleanValueAsRightSibling(boolean value);
[stable] JsonNodeTrx insertNumberValueAsFirstChild(Number value);
[stable] JsonNodeTrx insertNumberValueAsLastChild(Number value);
[stable] JsonNodeTrx insertNumberValueAsLeftSibling(Number value);
[stable] JsonNodeTrx insertNumberValueAsRightSibling(Number value);
[stable] JsonNodeTrx insertNullValueAsFirstChild();
[stable] JsonNodeTrx insertNullValueAsLastChild();
[stable] JsonNodeTrx insertNullValueAsLeftSibling();
[stable] JsonNodeTrx insertNullValueAsRightSibling();
[stable] JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(Item item);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(Item item);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(Item item);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(Item item);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit, CheckParentNode checkParentNode);
[stable] default LdjsonResult insertLdjsonAsFirstChild(JsonParser parser, Commit commit);
[stable] default LdjsonResult insertLdjsonAsLastChild(JsonParser parser, Commit commit);
[stable] JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode);
[stable] JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode, SkipRootToken skipRootToken);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit);
[stable] default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode);
```

### io.sirix.api.json.JsonNodeTrx.Commit

Source: [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java).

```text
[internal] enum Commit
[internal] enum IMPLICIT
[internal] enum NO
```

### io.sirix.api.json.JsonNodeTrx.CheckParentNode

Source: [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java).

```text
[internal] enum CheckParentNode
[internal] enum YES
[internal] enum NO
```

### io.sirix.api.json.JsonNodeTrx.SkipRootToken

Source: [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java).

```text
[internal] enum SkipRootToken
[internal] enum YES
[internal] enum NO
```

### io.sirix.api.json.JsonNodeTrx.LdjsonResult

Source: [JsonNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeTrx.java).

```text
[internal] record LdjsonResult(long documentCount, long arrayNodeKey)

```

### io.sirix.api.json.JsonResourceSession

Source: [JsonResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonResourceSession.java).

```text
[stable] public interface JsonResourceSession extends ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx>

```

### io.sirix.api.visitor.JsonNodeVisitor

Source: [JsonNodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/JsonNodeVisitor.java).

```text
[expert] public interface JsonNodeVisitor extends NodeVisitor
[expert] default VisitResult visit(ImmutableArrayNode node);
[expert] default VisitResult visit(ImmutableObjectNode node);
[expert] default VisitResult visit(ImmutableBooleanNode node);
[expert] default VisitResult visit(ImmutableStringNode node);
[expert] default VisitResult visit(ImmutableNumberNode node);
[expert] default VisitResult visit(ImmutableNullNode node);
[expert] default VisitResult visit(ImmutableJsonDocumentRootNode node);
[expert] default VisitResult visit(ObjectNamedBooleanNode node);
[expert] default VisitResult visit(ObjectNamedNumberNode node);
[expert] default VisitResult visit(ObjectNamedStringNode node);
[expert] default VisitResult visit(ObjectNamedNullNode node);
[expert] default VisitResult visit(ObjectNamedObjectNode node);
[expert] default VisitResult visit(ObjectNamedArrayNode node);
```

### io.sirix.api.visitor.NodeVisitor

Source: [NodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/NodeVisitor.java).

```text
[expert] public interface NodeVisitor

```

### io.sirix.api.visitor.VisitResult

Source: [VisitResult.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/VisitResult.java).

```text
[expert] public interface VisitResult

```

### io.sirix.api.visitor.VisitResultType

Source: [VisitResultType.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/VisitResultType.java).

```text
[expert] public enum VisitResultType implements VisitResult
[expert] enum SKIPSIBLINGS
[expert] enum SKIPSUBTREE
[expert] enum CONTINUE
[expert] enum TERMINATE
```

### io.sirix.api.visitor.XmlNodeVisitor

Source: [XmlNodeVisitor.java](../bundles/sirix-core/src/main/java/io/sirix/api/visitor/XmlNodeVisitor.java).

```text
[expert] public interface XmlNodeVisitor extends NodeVisitor
[expert] default VisitResult visit(ImmutablePI node);
[expert] default VisitResult visit(ImmutableComment node);
[expert] default VisitResult visit(ImmutableElement node);
[expert] default VisitResult visit(ImmutableAttributeNode node);
[expert] default VisitResult visit(ImmutableNamespace node);
[expert] default VisitResult visit(ImmutableText node);
[expert] default VisitResult visit(ImmutableXmlDocumentRootNode node);
```

### io.sirix.api.xml.XmlNodeReadOnlyTrx

Source: [XmlNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeReadOnlyTrx.java).

```text
[stable] public interface XmlNodeReadOnlyTrx extends NodeCursor, NodeReadOnlyTrx
[internal] @Override StorageEngineReader getStorageEngineReader();
[stable] VisitResult acceptVisitor(XmlNodeVisitor visitor);
[internal] @Override ImmutableXmlNode getNode();
[stable] boolean moveToAttribute(int index);
[stable] boolean moveToAttributeByName(QNm name);
[stable] boolean moveToNamespace(int index);
[stable] @Override String getValue();
[stable] @Override QNm getName();
[stable] String getType();
[internal] byte[] rawNameForKey(int key);
[internal] ItemList<AtomicValue> getItemList();
[stable] @Override boolean isClosed();
[stable] @Override boolean moveToNextFollowing();
[internal] SirixDeweyID getLeftSiblingDeweyID();
[internal] SirixDeweyID getRightSiblingDeweyID();
[internal] SirixDeweyID getParentDeweyID();
[internal] SirixDeweyID getFirstChildDeweyID();
[internal] SirixDeweyID getDeweyID();
[stable] int getAttributeCount();
[stable] int getNamespaceCount();
[stable] boolean isNameNode();
[internal] int getURIKey();
[internal] int getPrefixKey();
[internal] int getLocalNameKey();
[internal] int getNameCount(String name, NodeKind kind);
[internal] int getTypeKey();
[stable] long getAttributeKey(int index);
[stable] @Override boolean hasChildren();
[stable] boolean hasAttributes();
[stable] boolean hasNamespaces();
[internal] @Override long getPathNodeKey();
[internal] @Override NodeKind getPathKind();
[stable] boolean isStructuralNode();
[stable] boolean isValueNode();
[stable] @Override long getHash();
[stable] List<Long> getAttributeKeys();
[stable] List<Long> getNamespaceKeys();
[stable] byte[] getRawValue();
[stable] @Override long getChildCount();
[stable] @Override long getDescendantCount();
[stable] String getNamespaceURI();
[stable] @Override boolean moveTo(long nodeKey);
[stable] @Override boolean moveToDocumentRoot();
[stable] @Override boolean moveToFirstChild();
[stable] @Override boolean moveToLastChild();
[stable] @Override boolean moveToLeftSibling();
[stable] @Override boolean moveToParent();
[stable] @Override boolean moveToRightSibling();
[stable] @Override boolean moveToPrevious();
[stable] @Override boolean moveToNext();
[stable] boolean isElement();
[stable] boolean isText();
[stable] @Override boolean isDocumentRoot();
[stable] boolean isComment();
[stable] boolean isAttribute();
[stable] boolean isNamespace();
[stable] boolean isPI();
[internal] ImmutableNameNode getNameNode();
[internal] ImmutableValueNode getValueNode();
[stable] @Override XmlResourceSession getResourceSession();
```

### io.sirix.api.xml.XmlNodeTrx

Source: [XmlNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeTrx.java).

```text
[stable] public interface XmlNodeTrx extends XmlNodeReadOnlyTrx, NodeTrx
[internal] void beginAtomicOperation();
[internal] void endAtomicOperation();
[stable] XmlNodeTrx copySubtreeAsFirstChild(XmlNodeReadOnlyTrx rtx);
[stable] XmlNodeTrx copySubtreeAsLeftSibling(XmlNodeReadOnlyTrx rtx);
[stable] XmlNodeTrx copySubtreeAsRightSibling(XmlNodeReadOnlyTrx rtx);
[stable] XmlNodeTrx replaceNode(XMLEventReader reader);
[stable] XmlNodeTrx replaceNode(XmlNodeReadOnlyTrx rtx);
[stable] XmlNodeTrx moveSubtreeToFirstChild(long fromKey);
[stable] XmlNodeTrx moveSubtreeToRightSibling(long fromKey);
[stable] XmlNodeTrx moveSubtreeToLeftSibling(long fromKey);
[stable] XmlNodeTrx insertCommentAsLeftSibling(String value);
[stable] XmlNodeTrx insertCommentAsRightSibling(String value);
[stable] XmlNodeTrx insertCommentAsFirstChild(String value);
[stable] XmlNodeTrx insertPIAsLeftSibling(String target, String content);
[stable] XmlNodeTrx insertPIAsRightSibling(String target, String content);
[stable] XmlNodeTrx insertPIAsFirstChild(String target, String content);
[stable] XmlNodeTrx insertElementAsFirstChild(QNm name);
[stable] XmlNodeTrx insertElementAsLeftSibling(QNm name);
[stable] XmlNodeTrx insertElementAsRightSibling(QNm name);
[stable] XmlNodeTrx insertTextAsFirstChild(String value);
[stable] XmlNodeTrx insertTextAsLeftSibling(String value);
[stable] XmlNodeTrx insertTextAsRightSibling(String value);
[stable] XmlNodeTrx insertAttribute(QNm name, String value);
[stable] XmlNodeTrx insertAttribute(QNm name, String value, Movement move);
[stable] XmlNodeTrx insertNamespace(QNm name);
[stable] XmlNodeTrx insertNamespace(QNm name, Movement move);
[stable] XmlNodeTrx insertSubtreeAsFirstChild(XMLEventReader reader);
[stable] XmlNodeTrx insertSubtreeAsFirstChild(XMLEventReader reader, Commit commit);
[stable] XmlNodeTrx insertSubtreeAsRightSibling(XMLEventReader reader);
[stable] XmlNodeTrx insertSubtreeAsRightSibling(XMLEventReader reader, Commit commit);
[stable] XmlNodeTrx insertSubtreeAsLeftSibling(XMLEventReader reader);
[stable] XmlNodeTrx insertSubtreeAsLeftSibling(XMLEventReader reader, Commit commit);
[stable] XmlNodeTrx remove();
[stable] XmlNodeTrx setName(QNm name);
[stable] XmlNodeTrx setValue(String value);
[stable] @Override default XmlNodeTrx commit();
[stable] @Override default XmlNodeTrx commit(@Nullable String commitMessage);
[stable] @Override XmlNodeTrx commit(@Nullable String commitMessage, @Nullable Instant commitTimestamp);
[stable] @Override XmlNodeTrx rollback();
[stable] @Override XmlNodeTrx revertTo(int revision);
[stable] @Override void close();
[expert] @Override XmlNodeTrx addPreCommitHook(PreCommitHook hook);
[expert] @Override XmlNodeTrx addPostCommitHook(PostCommitHook hook);
[stable] @Override XmlNodeTrx truncateTo(int revision);
```

### io.sirix.api.xml.XmlNodeTrx.Commit

Source: [XmlNodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeTrx.java).

```text
[internal] enum Commit
[internal] enum Implicit
[internal] enum No
```

### io.sirix.api.xml.XmlResourceSession

Source: [XmlResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlResourceSession.java).

```text
[stable] public interface XmlResourceSession extends ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx>

```

### io.sirix.access.Databases

Source: [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java).

```text
[stable] public final class Databases
[stable] public static DatabaseType getDatabaseType(final Path file);
[stable] public static boolean createXmlDatabase(final DatabaseConfiguration dbConfig);
[stable] public static boolean createJsonDatabase(final DatabaseConfiguration dbConfig);
[stable] @SuppressWarnings("ReferenceEquality") public static void removeDatabase(final Path dbFile);
[internal] public static void freeAllocatedMemory();
[internal] public static void clearGlobalCaches();
[internal] public static void clearCachesForDatabase(final long databaseId);
[internal] public static void clearCachesForResource(final long databaseId, final long resourceId);
[stable] public static Database<XmlResourceSession> openXmlDatabase(final Path file, final User user);
[stable] public static Database<JsonResourceSession> openJsonDatabase(final Path file, final User user);
[stable] public static Database<JsonResourceSession> openJsonDatabase(final Path file);
[stable] public static Database<XmlResourceSession> openXmlDatabase(final Path file);
[stable] public static synchronized boolean existsDatabase(final Path dbPath);
[internal] public static final String PROP_RECORD_PAGE_CACHE = "sirix.cache.recordPage";
[internal] public static final String PROP_RECORD_PAGE_FRAGMENT_CACHE = "sirix.cache.recordPageFragment";
[internal] public static final String PROP_PAGE_CACHE = PageCache.LEGACY_BYTES_PROPERTY;
[internal] public static final String PROP_REVISION_ROOT_CACHE_ENTRIES = "sirix.cache.revisionRoot.max.entries";
[internal] public static BufferManager getGlobalBufferManager();
[internal] public static @Nullable BufferManager peekGlobalBufferManager();
[internal] public static RevisionEpochTracker getGlobalEpochTracker();
[internal] public static synchronized void reinitializeBufferManagerForTesting(long recordPageCacheBytes, long recordPageFragmentCacheBytes);
[internal] public static synchronized void reinitializeBufferManagerForTesting(long recordPageCacheBytes, long recordPageFragmentCacheBytes, long pageCacheBytes);
```

### io.sirix.access.DatabaseConfiguration

Source: [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java).

```text
[stable] public final class DatabaseConfiguration
[internal] public static final String SEGMENT_ALLOCATION_SIZE_PROPERTY = "sirix.allocator.maxSize";
[stable] public DatabaseConfiguration(final Path file);
[stable] public DatabaseConfiguration setDatabaseType(final DatabaseType type);
[stable] public DatabaseType getDatabaseType();
[stable] public DatabaseConfiguration setMaxSegmentAllocationSize(final long size);
[stable] public long getMaxSegmentAllocationSize();
[internal] public DatabaseConfiguration setMaximumResourceID(final long id);
[internal] public long getMaxResourceID();
[internal] public DatabaseConfiguration setDatabaseId(final long id);
[internal] public long getDatabaseId();
[stable] public Path getDatabaseFile();
[stable] @Override public String toString();
[stable] @Override public boolean equals(final @Nullable Object obj);
[stable] @Override public int hashCode();
[internal] public Path getConfigFile();
[stable] public String getDatabaseName();
[internal] public static void serialize(final DatabaseConfiguration config) throws SirixIOException;
[internal] public static DatabaseConfiguration deserialize(final Path file);
```

### io.sirix.access.DatabaseConfiguration.DatabasePaths

Source: [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java).

```text
[internal] public enum DatabasePaths
[internal] enum CONFIG_BINARY(Paths.get("dbsetting.obj"), false)
[internal] enum KEY_SELECTOR(Paths.get("keyselector"), true)
[internal] enum DATA(Paths.get("resources"), true)
[internal] enum LOCK(Paths.get(".lock"), false)
[internal] public Path getFile();
[internal] public boolean isFolder();
[internal] public static int compareStructure(final Path file);
```

### io.sirix.access.ResourceConfiguration

Source: [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java).

```text
[stable] public final class ResourceConfiguration
[internal] public static final BinaryEncodingVersion BINARY_ENCODING_VERSION = BinaryEncodingVersion.V0;
[internal] public static final String NODE_HASH_FUNCTION_ID = "XX3";
[internal] public final StorageType storageType;
[internal] public final VersioningType versioningType;
[internal] public final HashType hashType;
[internal] public final int maxNumberOfRevisionsToRestore;
[internal] public final ByteHandlerPipeline byteHandlePipeline;
[internal] public Path resourcePath;
[internal] public final boolean useTextCompression;
[internal] public final boolean withPathSummary;
[internal] public final boolean withPathStatistics;
[internal] public final boolean repairBulkInsertHashes;
[internal] public final RecordSerializer recordPersister;
[internal] public final boolean areDeweyIDsStored;
[internal] public final int deweyIdSiblingDistance;
[internal] public final LongHashFunction nodeHashFunction;
[internal] public final StringCompressionType stringCompressionType;
[internal] public final RegionCompressionType regionCompressionType;
[internal] public final boolean verifyChecksumsOnRead;
[internal] public final boolean saltRecordPages;
[internal] public final HashAlgorithm hashAlgorithm;
[internal] public final ValidTimeConfig validTimeConfig;
[internal] public final UUID resourceUuid;
[stable] public static Builder newBuilder(final String resource);
[internal] public BinaryEncodingVersion getBinaryEncodingVersion();
[stable] public boolean customCommitTimestamps();
[internal] public ResourceConfiguration setID(final long id);
[stable] public StorageType getStorageType();
[stable] public long getID();
[stable] public long getDatabaseId();
[stable] @Override public int hashCode();
[stable] @Override public boolean equals(final Object obj);
[stable] @Override public String toString();
[stable] public Path getResource();
[stable] public String getName();
[stable] public boolean storeDiffs();
[internal] public Path getConfigFile();
[stable] public boolean storeChildCount();
[stable] public boolean aggregateKernel();
[stable] public boolean storeNodeHistory();
[stable] public ValidTimeConfig getValidTimeConfig();
[stable] public boolean hasValidTimeSupport();
[internal] public static void serialize(final ResourceConfiguration config) throws SirixIOException;
[internal] public static ResourceConfiguration deserialize(final Path file) throws SirixIOException;
```

### io.sirix.access.ResourceConfiguration.ResourcePaths

Source: [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java).

```text
[internal] public enum ResourcePaths
[internal] enum DATA(Paths.get("data"), true)
[internal] enum TRANSACTION_INTENT_LOG(Paths.get("log"), true)
[internal] enum CONFIG_BINARY(Paths.get("ressetting.obj"), false)
[internal] enum INDEXES(Paths.get("indexes"), true)
[internal] enum ENCRYPTION_KEY(Paths.get("encryption"), true)
[internal] enum UPDATE_OPERATIONS(Paths.get("update-operations"), true)
[internal] public Path getPath();
[internal] public boolean isFolder();
[internal] public static int compareStructure(final Path file);
```

### io.sirix.access.ResourceConfiguration.Builder

Source: [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java).

```text
[stable] public static final class Builder
[internal] public boolean storeDiffs = true;
[stable] public Builder(final String resource);
[stable] public Builder storageType(final StorageType type);
[internal] public Builder persistenter(final RecordSerializer persistenter);
[stable] public Builder storeDiffs(final boolean storeDiffs);
[stable] public Builder versioningApproach(final VersioningType versioning);
[stable] public Builder hashKind(final HashType hashKind);
[internal] public Builder byteHandlerPipeline(final ByteHandlerPipeline byteHandler);
[stable] public Builder maxNumberOfRevisionsToRestore(final int revisionsToRestore);
[stable] public Builder useDeweyIDs(final boolean useDeweyIDs);
[stable] public Builder deweyIdSiblingDistance(final int distance);
[stable] public Builder useTextCompression(final boolean useTextCompression);
[stable] public Builder buildPathSummary(final boolean buildPathSummary);
[stable] public Builder buildPathStatistics(final boolean buildPathStatistics);
[internal] public Builder repairBulkInsertHashes(final boolean repair);
[stable] public Builder storeChildCount(final boolean storeChildCount);
[stable] public Builder customCommitTimestamps(final boolean customCommitTimestamps);
[stable] public Builder storeNodeHistory(boolean storeNodeHistory);
[internal] public Builder binaryEncodingVersion(BinaryEncodingVersion binaryEncodingVersion);
[stable] public Builder regionCompressionType(RegionCompressionType regionCompressionType);
[stable] public Builder stringCompressionType(StringCompressionType stringCompressionType);
[stable] public Builder verifyChecksumsOnRead(boolean verify);
[stable] public Builder saltRecordPages(final boolean enabled);
[stable] public Builder hashAlgorithm(HashAlgorithm algorithm);
[stable] public Builder validTimeConfig(ValidTimeConfig validTimeConfig);
[internal] public Builder resourceUuid(final UUID resourceUuid);
[stable] public Builder validTimePaths(String validFromPath, String validToPath);
[stable] public Builder useConventionalValidTimePaths();
[stable] @Override public String toString();
[stable] public ResourceConfiguration build();
```

### io.sirix.access.DatabaseType

Source: [DatabaseType.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseType.java).

```text
[stable] public enum DatabaseType
[stable] enum XML("xml")
[stable] enum JSON("json")
[internal] public String getStringType();
[internal] public static Optional<DatabaseType> fromString(String symbol);
[internal] public abstract <R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> Database<R> createDatabase( DatabaseConfiguration dbConfig, User user);
[internal] public abstract Node getDocumentNode(SirixDeweyID id);
```

### io.sirix.access.User

Source: [User.java](../bundles/sirix-core/src/main/java/io/sirix/access/User.java).

```text
[stable] public final class User
[stable] public User(final String userName, final UUID userId);
[stable] public UUID getId();
[stable] public String getName();
[stable] @Override public int hashCode();
[stable] @Override public boolean equals(final Object other);
[stable] @Override public String toString();
```

### io.sirix.access.trx.node.AfterCommitState

Source: [AfterCommitState.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AfterCommitState.java).

```text
[stable] public enum AfterCommitState
[stable] enum KEEP_OPEN
[stable] enum KEEP_OPEN_ASYNC_FLUSH
[stable] enum KEEP_OPEN_ASYNC_COMMIT
[stable] enum CLOSE
[stable] public static final int MAX_ASYNC_FLUSH_NODE_COUNT = Integer.getInteger("sirix.asyncFlush.maxNodeCount", 1 << 14);
```

### io.sirix.settings.VersioningType

Source: [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java).

```text
[stable] public enum VersioningType
[stable] enum FULL
[stable] enum DIFFERENTIAL
[stable] enum INCREMENTAL
[stable] enum SLIDING_SNAPSHOT
[internal] public static VersioningType fromString(String versioningType);
[internal] public static long combineSlotCopyNanos();
[internal] public static long combineRegionRebuildNanos();
[internal] public static long combineSlotsCopied();
[internal] public static void resetCombineDiag();
[internal] public abstract <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages, final int revsToRestore, final StorageEngineReader storageEngineReader);
[internal] public abstract <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification( final List<T> pages, final int revsToRestore, final StorageEngineReader storageEngineReader, final PageReference reference, final TransactionIntentLog log);
[internal] public abstract int[] getRevisionRoots(final int previousRevision, final int revsToRestore);
[internal] protected static <V extends DataRecord, T extends KeyValuePage<V>> void propagateFsstSymbolTable(final T sourcePage, final T targetPage);
[internal] protected static void copySlotPreservingMetadata(final KeyValueLeafPage src, final KeyValueLeafPage dst, final int offset, final int nodeKindId, final @Nullable FsstAwareSlotCopier copier);
[internal] public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineOwnedRecordPagesForModification( final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader, final PageReference reference, final TransactionIntentLog log);
[internal] public HOTLeafPage combineHOTLeafPages(final List<HOTLeafPage> pages, final int revToRestore, final StorageEngineReader storageEngineReader);
[internal] public static long ownedReplaysKeepingFsstBinding();
[internal] public static boolean hotMergeDiagEnabled();
[internal] public static long carryForwardRotations();
[internal] public static long carryForwardEntriesReemitted();
[internal] public static long singleFragmentReads();
[internal] public static long multiFragmentMerges();
[internal] public static long fragmentsWalked();
[internal] public static long pointLeafReads();
[internal] public static long pointFragmentsWalked();
[internal] public static void recordHOTPointRead(final int fragments);
[internal] public static long completeDumpShortCircuits();
[internal] public static long completeDumpsWalkedPast();
[internal] public static void resetFragmentMergeCounters();
[internal] public HOTLeafPage combineHOTLeafPagesForModification(final HOTLeafPage hotLeaf, final int revsToRestore, final StorageEngineReader storageEngineReader, final PageReference reference);
[internal] public boolean bumpHOTPageFragmentChain(final PageReference reference, final int priorFragmentRevision, final int currentRevision, final int revToRestore, final long databaseId, final long resourceId);
[internal] public static boolean hotSlidingSnapshotEvicts(final PageReference reference, final int revToRestore);
[internal] public static void carryForwardAgingHOTEntries(final List<HOTLeafPage> fragmentsNewestFirst, final HOTLeafPage modifiedLeaf);
[internal] public static void carryForwardDifferentialDelta(final HOTLeafPage priorDelta, final HOTLeafPage modifiedLeaf);
```

### io.sirix.io.StorageType

Source: [StorageType.java](../bundles/sirix-core/src/main/java/io/sirix/io/StorageType.java).

```text
[stable] public enum StorageType
[stable] enum IN_MEMORY
[stable] enum FILE
[stable] enum FILE_CHANNEL
[stable] enum MEMORY_MAPPED
[stable] enum IO_URING
[stable] enum S3
[internal] public static final String REVISION_FILE_DATA_CACHE_SIZE_PROPERTY = "sirix.revision.file.data.cache.size";
[internal] public static final long DEFAULT_REVISION_FILE_DATA_CACHE_MAX_SIZE = 1_000_000L;
[internal] public static final ConcurrentMap<Path, AsyncCache<Integer, RevisionFileData>> CACHE_REPOSITORY = new ConcurrentHashMap<>();
[internal] public static final ConcurrentMap<Path, RevisionIndexHolder> REVISION_INDEX_REPOSITORY = new ConcurrentHashMap<>();
[internal] public static void clearRevisionMetadataCaches();
[internal] public static StorageType fromString(String storageType);
[internal] public static boolean isExternalProvider(String storageType);
[internal] public abstract IOStorage getInstance(final ResourceConfiguration resourceConf);
[internal] public IOStorage getInstance(final ResourceConfiguration resourceConf, final @Nullable UberPage openingUberPage);
[internal] public static IOStorage getStorage(final ResourceConfiguration resourceConf);
[internal] public static IOStorage getStorage(final ResourceConfiguration resourceConf, final @Nullable UberPage openingUberPage);
[internal] public static RevisionIndexHolder getRevisionIndexHolder(ResourceConfiguration resourceConf);
```

### io.sirix.access.trx.node.HashType

Source: [HashType.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/HashType.java).

```text
[stable] public enum HashType
[stable] enum ROLLING
[stable] enum POSTORDER
[stable] enum NONE
[stable] public static HashType fromString(String string);
```

### io.sirix.exception.SirixException

Source: [SirixException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixException.java).

```text
[stable] public class SirixException extends RuntimeException
[stable] public SirixException(final Throwable throwable);
[stable] public SirixException(String message, Object... args);
[stable] public SirixException(String message);
[stable] public SirixException(final String... message);
[stable] public SirixException(final String message, final Throwable throwable);
[stable] public SirixException(Throwable cause, String message, Object[] args);
[stable] public static StringBuilder concat(final String... pMessage);
```

### io.sirix.exception.SirixUsageException

Source: [SirixUsageException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixUsageException.java).

```text
[stable] public class SirixUsageException extends SirixRuntimeException
[stable] public SirixUsageException(final String... message);
```

### io.sirix.exception.SirixIOException

Source: [SirixIOException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixIOException.java).

```text
[stable] public final class SirixIOException extends SirixRuntimeException
[stable] public SirixIOException(final String message);
[stable] public SirixIOException(final String message, final Throwable throwable);
[stable] public SirixIOException(final Throwable throwable);
```

### io.sirix.exception.SirixRevisionPrunedException

Source: [SirixRevisionPrunedException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixRevisionPrunedException.java).

```text
[stable] public final class SirixRevisionPrunedException extends SirixRuntimeException
[stable] public SirixRevisionPrunedException(final int revision, final Instant timestamp);
[stable] public int getRevision();
[stable] public Instant getTimestamp();
```

### io.sirix.exception.UnsupportedStoreFeatureException

Source: [UnsupportedStoreFeatureException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/UnsupportedStoreFeatureException.java).

```text
[stable] public final class UnsupportedStoreFeatureException extends UnsupportedOperationException
[stable] public UnsupportedStoreFeatureException(final String message);
```

### io.sirix.query.SirixQueryContext

Source: [SirixQueryContext.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixQueryContext.java).

```text
[stable] public final class SirixQueryContext implements QueryContext, AutoCloseable
[internal] public @Nullable StorageEngineReader borrowDerivedAggregateReader(final StorageEngineReader source);
[internal] public @Nullable DerivedAggregateCatchUp borrowDerivedAggregateCatchUp(final StorageEngineReader source);
[internal] public synchronized int resolveRevision(final JsonResourceSession session, final Instant instant);
[stable] public static SirixQueryContext createWithNodeStore(final XmlDBStore nodeStore);
[stable] public static SirixQueryContext createWithNodeStoreAndCommitStrategy(final XmlDBStore nodeStore, final CommitStrategy commitStrategy);
[stable] public static SirixQueryContext createWithJsonStore(final JsonDBStore jsonItemStore);
[stable] public static SirixQueryContext createWithJsonStoreAndCommitStrategy(final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy);
[stable] public static SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy);
[stable] public static SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy, final String commitMessage, final Instant commitTimestamp);
[stable] public static SirixQueryContext create();
[stable] @Override public void setDefaultJsonCollection(JsonCollection<?> defaultJsonCollection);
[stable] @Override public void setDefaultNodeCollection(NodeCollection<?> defaultNodeCollection);
[stable] @Override public void applyUpdates();
[internal] @Override public void addPendingUpdate(UpdateOp op);
[internal] @Override public UpdateList getUpdateList();
[internal] @Override public void setUpdateList(UpdateList updates);
[stable] @Override public void bind(QNm name, Sequence sequence);
[stable] @Override public Sequence resolve(QNm name) throws QueryException;
[stable] @Override public boolean isBound(QNm name);
[stable] @Override public void setContextItem(Item item);
[stable] @Override public Item getContextItem();
[stable] @Override public ItemType getItemType();
[stable] @Override public Node<?> getDefaultDocument();
[stable] @Override public void setDefaultDocument(Node<?> defaultDocument);
[stable] @Override public NodeCollection<?> getDefaultNodeCollection();
[stable] @Override public JsonCollection<?> getDefaultJsonCollection();
[stable] @Override public DateTime getDateTime();
[stable] @Override public Date getDate();
[stable] @Override public Time getTime();
[stable] @Override public DTD getImplicitTimezone();
[stable] @Override public AnyURI getBaseUri();
[internal] @Override public NodeFactory<?> getNodeFactory();
[stable] @Override public XmlDBStore getNodeStore();
[stable] @Override public JsonDBStore getJsonItemStore();
[stable] public String getCommitMessage();
[stable] @Override public void close();
```

### io.sirix.query.SirixQueryContext.CommitStrategy

Source: [SirixQueryContext.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixQueryContext.java).

```text
[stable] public enum CommitStrategy
[stable] enum AUTO
[stable] enum EXPLICIT
```

### io.sirix.query.SirixCompileChain

Source: [SirixCompileChain.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixCompileChain.java).

```text
[stable] public final class SirixCompileChain extends CompileChain implements AutoCloseable
[internal] public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.indexrewrite", true);
[stable] public static SirixCompileChain create();
[stable] public static SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore);
[stable] public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore);
[internal] public static SirixCompileChain createWithJsonStoreWithoutAutoWiring(final JsonDBStore jsonStore);
[stable] public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore, final JsonResourceSession session);
[stable] public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore);
[stable] public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore, final JsonDBStore jsonStore, final JsonResourceSession session, final int revision);
[internal] public static SirixCompileChain createParallel(final XmlDBStore nodeStore, final JsonDBStore jsonStore);
[internal] public static SirixCompileChain createParallel(final XmlDBStore nodeStore, final JsonDBStore jsonStore, final boolean ordered);
[internal] public static SirixCompileChain createParallelWithMorsel(final XmlDBStore nodeStore, final JsonDBStore jsonStore);
[internal] public static SirixCompileChain createWithMorsel(final XmlDBStore nodeStore, final JsonDBStore jsonStore);
[internal] public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore);
[internal] public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel, final boolean ordered);
[internal] public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel, final boolean ordered, final JsonResourceSession autoExecutorSession);
[internal] public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore, final boolean parallel, final boolean ordered, final JsonResourceSession autoExecutorSession, final int autoExecutorRevision);
[stable] @Override public Module compile(final String query) throws QueryException;
[internal] @Override protected AST parse(final String query) throws QueryException;
[internal] @Override protected Translator getTranslator(Map<QNm, Str> options);
[internal] @Override protected Optimizer getOptimizer(Map<QNm, Str> options);
[internal] public Mesh getMesh();
[internal] public void collectPendingHistograms();
[stable] @Override public void close();
```

### io.sirix.query.json.JsonDBStore

Source: [JsonDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/JsonDBStore.java).

```text
[stable] public interface JsonDBStore extends JsonStore, AutoCloseable
[internal] default Path databaseDirectory(final String name);
[internal] JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceSession> database);
[internal] JsonDBStore removeDatabase(Database<JsonResourceSession> database);
[internal] void registerWriteSession(JsonResourceSession session);
[stable] @Override JsonDBCollection lookup(String name);
[stable] @Override JsonDBCollection create(String name);
[stable] @Override JsonDBCollection create(String collName, Path path);
[stable] JsonDBCollection create(String collName, Path path, Object options);
[stable] @Override JsonDBCollection createFromPaths(String collName, Stream<Path> paths);
[stable] JsonDBCollection create(String collName, String optResName, Path path);
[stable] JsonDBCollection create(String collName, String optResName, Path path, Object options);
[stable] @Override JsonDBCollection create(String collName, String path);
[stable] JsonDBCollection create(String collName, String path, Object options);
[stable] JsonDBCollection create(String collName, String optResName, String json);
[stable] JsonDBCollection create(String collName, String optResName, String json, Object options);
[stable] JsonDBCollection create(String collName, String optResName, JsonReader json);
[stable] JsonDBCollection create(String collName, String optResName, JsonReader json, Object options);
[stable] JsonDBCollection create(String collName, Set<JsonReader> json);
[stable] JsonDBCollection create(String collName, Set<JsonReader> json, Object options);
[stable] JsonDBCollection createFromUris(String collName, List<String> uris, Object options);
[stable] @Override JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons);
[stable] default JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons, Object options);
[stable] @Override void drop(String name);
[stable] void drop(String name, Path databasePath);
[stable] @Override void makeDir(String path);
[stable] @Override void close();
[internal] Options options();
```

### io.sirix.query.node.XmlDBStore

Source: [XmlDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/XmlDBStore.java).

```text
[stable] public interface XmlDBStore extends NodeStore, AutoCloseable
[internal] default Path databaseDirectory(final String name);
[stable] @Override XmlDBCollection lookup(String name);
[stable] @Override XmlDBCollection create(String name);
[stable] @Override XmlDBCollection create(String collName, NodeSubtreeParser parser);
[stable] XmlDBCollection create(String collName, NodeSubtreeParser parser, String commitMessage, Instant commitTimestamp);
[stable] XmlDBCollection create(String collName, String optionalResourceName, NodeSubtreeParser parser);
[stable] XmlDBCollection create(String collName, String optionalResourceName, NodeSubtreeParser parser, String commitMessage, Instant commitTimestamp);
[stable] @Override XmlDBCollection create(String collName, Stream<NodeSubtreeParser> parsers);
[stable] @Override void drop(String name);
[stable] @Override void makeDir(String path);
[stable] @Override void close();
```

### io.sirix.query.json.BasicJsonDBStore

Source: [BasicJsonDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/BasicJsonDBStore.java).

```text
[stable] public final class BasicJsonDBStore implements JsonDBStore
[stable] public static Builder newBuilder();
[internal] public Path getLocation();
[internal] public boolean hasOnlyStockCollections();
[internal] @SuppressWarnings("NullAway") public long getCollectionClassificationCount();
[internal] @Override public Options options();
[stable] @Override public JsonDBCollection lookup(final String name);
[stable] @Override public JsonDBCollection create(final String name);
[stable] @Override public JsonDBCollection create(final String collName, final Path path);
[stable] @Override public JsonDBCollection create(String collName, Path path, Object options);
[stable] @Override public JsonDBCollection create(String collName, String json);
[stable] @Override public JsonDBCollection create(String collName, String json, Object options);
[stable] @Override public JsonDBCollection create(String collName, String optResName, String json);
[stable] @Override public JsonDBCollection create(String collName, String optResName, String json, Object options);
[stable] @Override public JsonDBCollection create(final String collName, final String optResName, final Path path);
[stable] @Override public JsonDBCollection create(final String collName, final String optResName, final Path path, Object options);
[stable] @Override public JsonDBCollection create(String collName, String resourceName, JsonReader jsonReader);
[stable] @Override public JsonDBCollection create(String collName, String resourceName, JsonReader reader, Object options);
[stable] public JsonDBCollection create(final String collName, final String resourceName, final JsonReader reader, final ProjectionSpec projection);
[stable] public JsonDBCollection create(final String collName, final String resourceName, final JsonParser parser, final ProjectionSpec projection);
[stable] public JsonDBCollection create(final String collName, final String resourceName, final JsonParser parser, final ProjectionSpec projection, final boolean ldjson);
[stable] public JsonDBCollection create(final String collName, final String resourceName, final JsonParser parser);
[stable] public JsonDBCollection create(final String collName, final String resourceName, final JsonParser parser, final boolean ldjson);
[internal] public JsonDBCollection createParallel(final String collName, final String resourceName, final InputStream input, final ProjectionSpec projection);
[internal] public JsonDBCollection createParallel(final String collName, final String resourceName, final InputStream input);
[internal] public JsonDBCollection createParallel(final String collName, final String resourceName, final Reader input, final ProjectionSpec projection);
[internal] public JsonDBCollection createParallel(final String collName, final String resourceName, final Reader input);
[stable] @Override public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders, Object options);
[stable] @Override public JsonDBCollection createFromUris(final String collName, final List<String> uris, final Object options);
[stable] @Override public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders);
[internal] @Override public JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceSession> database);
[internal] @Override public JsonDBStore removeDatabase(final Database<JsonResourceSession> database);
[stable] @Override public JsonDBCollection createFromJsonStrings(String collName, final @Nullable Stream<Str> jsonStrings);
[stable] @Override public JsonDBCollection createFromJsonStrings(String collName, final @Nullable Stream<Str> jsonStrings, final Object options);
[stable] @Override public JsonDBCollection createFromPaths(final String collName, final @Nullable Stream<Path> paths);
[internal] @Override public Path databaseDirectory(final String name);
[stable] @Override public void drop(final String name);
[stable] @Override public void drop(final String name, final Path databasePath);
[stable] @Override public void makeDir(final String path);
[internal] @Override public void registerWriteSession(final JsonResourceSession session);
[stable] @Override public void close();
```

### io.sirix.query.json.BasicJsonDBStore.Builder

Source: [BasicJsonDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/BasicJsonDBStore.java).

```text
[stable] @SuppressWarnings("unused") public static class Builder
[stable] public Builder storeNodeHistory(final boolean storeNodeHistory);
[stable] public Builder storageType(final StorageType storageType);
[stable] public Builder buildPathSummary(final boolean buildPathSummary);
[stable] public Builder buildPathStatistics(final boolean buildPathStatistics);
[stable] public Builder storeDeweyIds(final boolean storeDeweyIDs);
[stable] public Builder location(final Path location);
[stable] public Builder hashType(final HashType hashType);
[stable] public Builder versioningType(final VersioningType versioningType);
[stable] public Builder numberOfNodesBeforeAutoCommit(final int numberOfNodesBeforeAutoCommit);
[internal] public Builder useAsyncFlushForImports(final boolean useAsyncFlushForImports);
[stable] public BasicJsonDBStore build();
```

### io.sirix.query.node.BasicXmlDBStore

Source: [BasicXmlDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/BasicXmlDBStore.java).

```text
[stable] public final class BasicXmlDBStore implements XmlDBStore
[stable] public static Builder newBuilder();
[internal] public Path getLocation();
[stable] @Override public XmlDBCollection lookup(final String name);
[stable] @Override public XmlDBCollection create(final String name);
[stable] @Override public XmlDBCollection create(final String collName, final NodeSubtreeParser parser);
[stable] @Override public XmlDBCollection create(final String collName, final NodeSubtreeParser parser, final String commitMessage, final Instant commitTimestamp);
[stable] @Override public XmlDBCollection create(final String collName, final String optResName, final NodeSubtreeParser parser);
[stable] @Override public XmlDBCollection create(final String collName, final @Nullable String optResName, final NodeSubtreeParser parser, final @Nullable String commitMessage, final @Nullable Instant commitTimestamp);
[stable] @Override public XmlDBCollection create(final String collName, final @Nullable Stream<NodeSubtreeParser> parsers);
[internal] @Override public Path databaseDirectory(final String name);
[stable] @Override public void drop(final String name);
[stable] @Override public void makeDir(final String path);
[stable] @Override public void close();
```

### io.sirix.query.node.BasicXmlDBStore.Builder

Source: [BasicXmlDBStore.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/BasicXmlDBStore.java).

```text
[stable] @SuppressWarnings("unused") public static class Builder
[stable] public Builder storeNodeHistory(final boolean storeNodeHistory);
[stable] public Builder storeDeweyIds(boolean storeDeweyIds);
[stable] public Builder numberOfNodesBeforeAutoCommit(final int numberOfNodesBeforeAutoCommit);
[stable] public Builder hashType(final HashType hashType);
[stable] public Builder storageType(final StorageType storageType);
[stable] public Builder buildPathSummary(final boolean buildPathSummary);
[stable] public Builder buildPathStatistics(final boolean buildPathStatistics);
[stable] public Builder versioningType(final VersioningType versioningType);
[stable] public Builder location(final Path location);
[stable] public BasicXmlDBStore build();
```

### io.sirix.query.json.JsonDBCollection

Source: [JsonDBCollection.java](../bundles/sirix-query/src/main/java/io/sirix/query/json/JsonDBCollection.java).

```text
[stable] public interface JsonDBCollection extends TemporalJsonCollection<JsonDBItem>, AutoCloseable
[internal] Database<JsonResourceSession> getDatabase();
[internal] int getID();
[internal] JsonDBStore getJsonDBStore();
[internal] JsonDBCollection setJsonDBStore(JsonDBStore jsonDBStore);
[stable] JsonDBItem add(String resourceName, JsonReader reader, Object options);
[stable] JsonDBItem add(String resourceName, JsonReader reader);
[internal] void addAll(List<JsonReader> readers, Object options);
[internal] void addAllFromUris(List<String> uris, Object options);
[stable] @Override void close();
```

### io.sirix.query.node.XmlDBCollection

Source: [XmlDBCollection.java](../bundles/sirix-query/src/main/java/io/sirix/query/node/XmlDBCollection.java).

```text
[stable] public interface XmlDBCollection extends TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>>, AutoCloseable
[internal] Database<XmlResourceSession> getDatabase();
[internal] int getID();
[stable] XmlDBNode add(String resourceName, NodeSubtreeParser parser);
[stable] XmlDBNode add(String resourceName, NodeSubtreeParser parser, String commitMessage, Instant commitTimestamp);
[stable] @Override XmlDBNode getDocument();
[stable] @Override XmlDBNode getDocument(int revision);
[stable] @Override XmlDBNode getDocument(Instant pointInTime);
[stable] @Override XmlDBNode getDocument(String name);
[stable] @Override XmlDBNode getDocument(String name, int revision);
[stable] @Override XmlDBNode getDocument(String name, Instant pointInTime);
[stable] @Override Stream<XmlDBNode> getDocuments();
[stable] @Override XmlDBNode add(NodeSubtreeParser parser);
[internal] void addAll(List<NodeSubtreeParser> parsers);
[stable] @Override void close();
```

### io.sirix.service.json.BasicJsonDiff

Source: [BasicJsonDiff.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/BasicJsonDiff.java).

```text
[stable] public final class BasicJsonDiff implements DiffObserver, JsonDiff
[stable] public BasicJsonDiff(final String databaseName);
[stable] @Override public String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber);
[stable] @Override public String generateDiff(JsonResourceSession session, int oldRevisionNumber, int newRevisionNumber, long startNodeKey, long maxDepth);
[stable] public String generateDiff(JsonResourceSession session, int oldRevisionNumber, int newRevisionNumber, long startNodeKey, long maxDepth, boolean includeData);
[internal] public String generateDiffForReplay(final JsonResourceSession session, final int oldRevisionNumber, final int newRevisionNumber);
[internal] @Override public void diffListener(final DiffFactory.DiffType diffType, final long newNodeKey, final long oldNodeKey, final DiffDepth depth);
[internal] @Override public void diffDone();
```

### io.sirix.diff.DiffFactory

Source: [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java).

```text
[stable] public final class DiffFactory
[stable] public static Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder(final XmlResourceSession resourceSession, final int newRev, final int oldRev, final DiffOptimized diffKind, final Set<DiffObserver> observers);
[stable] public static synchronized void invokeJsonDiff(final Builder<JsonNodeReadOnlyTrx, JsonNodeTrx> builder);
[stable] public static synchronized void invokeFullXmlDiff(final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder);
[stable] public static synchronized void invokeStructuralXmlDiff(final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder);
```

### io.sirix.diff.DiffFactory.DiffType

Source: [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java).

```text
[stable] public enum DiffType
[stable] enum SAME
[stable] enum SAMEHASH
[stable] enum INSERTED
[stable] enum DELETED
[stable] enum UPDATED
[stable] enum REPLACED
[stable] enum REPLACEDNEW
[stable] enum REPLACEDOLD
[stable] enum MOVEDFROM
[stable] enum MOVEDTO
```

### io.sirix.diff.DiffFactory.DiffOptimized

Source: [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java).

```text
[stable] public enum DiffOptimized
[stable] enum NO
[stable] enum HASHED
```

### io.sirix.diff.DiffFactory.Builder

Source: [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java).

```text
[stable] public static final class Builder<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
[stable] public Builder(final ResourceSession<R, W> resMgr, final int newRev, final int oldRev, final DiffOptimized diffKind, final Set<DiffObserver> observers);
[internal] public Builder<R, W> isGUI(final boolean isGUI);
[stable] public Builder<R, W> oldStartKey(final long oldKey);
[stable] public Builder<R, W> oldMaxDepth(final long oldMaxDepth);
[stable] public Builder<R, W> newStartKey(final long newKey);
[stable] public Builder<R, W> newDepth(final int newDepth);
[stable] public Builder<R, W> oldDepth(final int oldDepth);
[internal] public Builder<R, W> diffAlgorithm(final DiffAlgorithm diffAlgorithm);
[stable] public Builder<R, W> hashKind(final HashType kind);
[stable] public Builder<R, W> skipSubtrees(final boolean skipSubtrees);
```

### io.sirix.diff.DiffObserver

Source: [DiffObserver.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffObserver.java).

```text
[stable] public interface DiffObserver
[stable] void diffListener(DiffFactory.DiffType diffType, long newNodeKey, long oldNodeKey, DiffDepth depth);
[stable] void diffDone();
```

### io.sirix.diff.DiffDepth

Source: [DiffDepth.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffDepth.java).

```text
[stable] public final class DiffDepth implements Serializable
[stable] public DiffDepth(final int newDepth, final int oldDepth);
[stable] public int getNewDepth();
[stable] public int getOldDepth();
```

## Required API additions not present at this baseline

JAPI-106 requires a normal revision-N success receipt, CommitOutcomeUnknownException
for uncertain durability, and the recovered N-present/N-absent result. TXN-805 and
JAPI-201 require resume-after-revision registration and HistoryLostException. These
are decided behavior/name requirements, not existing compiled declarations. The
current writer-returning commit signatures below must be reconciled with the receipt
contract before freezing. Exact fields/method descriptors remain implementation work.
FMT-402/VER-205 also require queryable original timestamp metadata for adjusted imported
revisions; the exact metadata field/accessor is pending and belongs to stable history APIs.
JAPI-111/TXN-311 require one conditional-write check on the write transaction, a precondition
value type carrying the resource history epoch and the token revision together with the subtree,
insertion-point or resource target kind, and a distinct
precondition-failure exception carrying the token's epoch and revision, the current epoch and head
revision, the target and the failing kind. The check belongs to the stable transaction core; neither the method nor the
types exist at this baseline, so no `io.sirix.api.NodeTrx` entry above covers them yet.
The new commit-outcome API belongs to the stable transaction core; listener registration
and callback signatures belong to the labelled expert extension tier.

## Freeze completion and verification (JAPI-101, JAPI-110)

- Produce the exact compiled stable signature artifact, including inherited methods,
  generic/bridge signatures, enum constants, constructors and exception types. Apply
  member-level exclusions even when a declaration is inherited by a stable subtype.
- Complete support-type closure for the marked core signatures (for example node-kind,
  commit credentials, JSON insertion values, query results and configuration enums).
  Do not silently promote their storage/serialization methods or entire packages.
- Pin external exposed dependency signatures. Reconcile the final hook signatures with
  the decided dispatcher/read-only-view design before freezing.
- Install the default CI comparison against the frozen 1.0 stable manifest and subsequent
  supported baselines. Deliberate breaking fixtures must fail; changes confined to
  expert/internal members must not be mistaken for stable compatibility failures.
- Compile and run an external client covering JSON/XML create/read/mutate/history,
  time travel, diff, query entry points and configuration against release candidates.
- Specify the expert tier's exact support/deprecation window; the three-tier policy,
  internal freedom to change, CI requirement and no-JPMS-lockdown scope are decided.

Acceptance mappings: JAPI-101 → JavaApiCompatibilityConformance, JAPI-109 →
JavaApiTierConformance, JAPI-110 → JavaApiSignatureGateConformance (all planned).
No compatibility-check plugin, module export change or runtime test is delivered by
this documentation-only task. Source extraction is not binary compatibility verification.
