package org.sirix.access;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;

public enum DatabaseType {
  XDM {
    @Override
    Database<? extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>> createDatabase(
        DatabaseConfiguration dbConfig, XdmResourceStore store) {
      return new LocalXdmDatabase(dbConfig, store);
    }
  },

  JSON {
    @Override
    Database<? extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>> createDatabase(
        DatabaseConfiguration dbConfig, XdmResourceStore store) {
      return null;
    }
  };

  abstract Database<? extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>> createDatabase(
      DatabaseConfiguration dbConfig, XdmResourceStore store);
}
