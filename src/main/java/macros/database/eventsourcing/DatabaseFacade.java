package macros.database.eventsourcing;

import java.util.List;
import java.util.Properties;

public interface DatabaseFacade {

  void close() throws Exception;

  void updateEntity(Object tx, int entityId, Properties nameChange) throws Exception;

  Object begin() throws Exception;

  void commit(Object tx) throws Exception;

  List<Properties> getRecentUpdates(Object tx, int entityId, int count) throws Exception;

  Properties getEntity(Object tx, int entityId) throws Exception;

  void rollback(Object tx) throws Exception;
  
}