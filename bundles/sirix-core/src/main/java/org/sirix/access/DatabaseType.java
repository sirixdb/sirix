package org.sirix.access;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;

public enum DatabaseType {
  XDM {
    @SuppressWarnings("unchecked")
    @Override
    <R extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>, S extends ResourceStore<R>> Database<R> createDatabase(
        DatabaseConfiguration dbConfig, S store) {
      return (Database<R>) new LocalXdmDatabase(dbConfig, (XdmResourceStore) store);
    }
  },

  JSON {
    @SuppressWarnings("unchecked")
    @Override
    <R extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>, S extends ResourceStore<R>> Database<R> createDatabase(
        DatabaseConfiguration dbConfig, S store) {
      return (Database<R>) new LocalJsonDatabase(dbConfig, (JsonResourceStore) store);
    }
  };

  abstract <R extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>, S extends ResourceStore<R>> Database<R> createDatabase(
      DatabaseConfiguration dbConfig, S store);
}
