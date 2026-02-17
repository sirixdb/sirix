package io.sirix.access.xml;

import dagger.Binds;
import dagger.Module;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.dagger.ResourceSessionScope;

/**
 * The module for {@link XmlResourceManagerComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface XmlResourceManagerModule {

  @Binds
  @ResourceSessionScope
  XmlResourceSession resourceSession(XmlResourceSessionImpl resourceSession);
}
