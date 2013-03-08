package macros.database.stormpot;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;
import macros.database.DatabaseFacade;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotDatabaseFacade implements DatabaseFacade {
  private final LifecycledPool<Dao> pool;
  private final Timeout timeout;

  public StormpotDatabaseFacade(Database database, int poolSize) {
    final DataSource dataSource = database.createDataSource();
    Config<Dao> config = new Config<Dao>();
    Allocator<Dao> allocator = new Allocator<Dao>() {
      @Override
      public Dao allocate(Slot slot) throws Exception {
        return new Dao(slot, dataSource.getConnection());
      }
      @Override
      public void deallocate(Dao dao) throws Exception {
        dao.close();
      }
    };
    config.setAllocator(allocator);
    pool = new BlazePool<Dao>(config);
    timeout = new Timeout(10, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws Exception {
    pool.shutdown().await(timeout);
  }

  /*
   * Direct usage APIs
   */
  
  @Override
  public void insertLogRow(String txt, int x) throws Exception {
    Dao dao = pool.claim(timeout);
    try {
      dao.insertRow(txt, x);
    } finally {
      dao.release();
    }
  }
  
  /*
   * Transactional APIs
   */
  
  private static final int SNAPSHOT_INTERVAL = 10;

  @Override
  public Object begin() throws Exception {
    Dao dao = pool.claim(timeout);
    dao.begin();
    return dao;
  }

  @Override
  public void commit(Object tx) throws Exception {
    Dao dao = (Dao) tx;
    try {
      dao.commit();
    } finally {
      dao.release();
    }
  }

  @Override
  public void rollback(Object tx) throws Exception {
    Dao dao = (Dao) tx;
    try {
      dao.rollback();
    } finally {
      dao.release();
    }
  }

  @Override
  public void updateEntity(
      Object tx, int entityId, Properties change) throws Exception {
    Dao dao = (Dao) tx;
    List<Event> events = dao.getRecentEvents(entityId, SNAPSHOT_INTERVAL);
    dao.insertEventUpdate(entityId, change);
    if (needsSnapshot(events)) {
      dao.insertSnapshot(entityId, events, change);
    }
  }

  private boolean needsSnapshot(List<Event> events) {
    return events.size() == SNAPSHOT_INTERVAL
        && events.get(SNAPSHOT_INTERVAL - 1).type == Event.TYPE_SNAPSHOT;
  }

  @Override
  public List<Properties> getRecentUpdates(
      Object tx, int entityId, int count) throws Exception {
    Dao dao = (Dao) tx;
    List<Properties> updates = new ArrayList<Properties>(count);
    for (Event event : dao.getRecentEvents(entityId, count)) {
      Properties update = new Properties();
      update.load(new StringReader(event.payload));
      updates.add(update);
    }
    return updates;
  }

  @Override
  public Properties getEntity(Object tx, int entityId) throws Exception {
    Dao dao = (Dao) tx;
    List<Event> events = dao.getRecentEvents(entityId, SNAPSHOT_INTERVAL);
    return dao.buildSnapshot(events);
  }
}
