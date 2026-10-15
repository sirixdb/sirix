package io.sirix.index.aggregate;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.changelog.ChangeLogSlab;

import java.util.function.ObjIntConsumer;

/** A log-fed derived structure; transaction, publication and recovery belong to the maintainer. */
interface DerivedIndex {
  void build(StorageEngineWriter destination, JsonNodeReadOnlyTrx source, IndexDef definition, Indexes sourceIndexes);

  void applySlab(StorageEngineWriter destination, IndexDef definition, Indexes sourceIndexes,
      ResourceConfiguration sourceConfiguration, ChangeLogSlab slab, ObjIntConsumer<byte[]> foldCandidate);

  void fold(StorageEngineWriter destination, IndexDef definition, Iterable<byte[]> groups);
}
