package macros.database.eventsourcing;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotDatabaseFacade implements DatabaseFacade {
  public static class Dao implements Poolable {
    private final Slot slot;
    private final Connection connection;
    private final PreparedStatement recentEvents;
    private final PreparedStatement insertEvent;
    private final StringWriter writer;
    
    public Dao(Slot slot, Connection connection) throws Exception {
      this.slot = slot;
      this.connection = connection;
      this.writer = new StringWriter();
      connection.setAutoCommit(false);
      recentEvents = connection.prepareStatement(
          "select * from event where entity_id = ? order by id desc limit ?");
      insertEvent = connection.prepareStatement(
          "insert into event (entity_id, type, payload) values (?, ?, ?)");
    }

    @Override
    public void release() {
      slot.release(this);
    }

    public void close() throws Exception {
      connection.close();
    }

    public void commit() throws Exception {
      connection.commit();
    }

    public void rollback() throws Exception {
      connection.rollback();
    }

    public List<Event> getRecentEvents(int entityId, int count) throws Exception {
      List<Event> events = new ArrayList<Event>(count);
      recentEvents.setInt(1, entityId);
      recentEvents.setInt(2, count);
      ResultSet result = recentEvents.executeQuery();
      while (result.next()) {
        events.add(new Event(
            result.getInt("id"),
            result.getInt("entity_id"),
            result.getInt("type"),
            result.getString("payload")));
      }
      Collections.reverse(events);
      return events;
    }

    public void insertEventUpdate(
        int entityId, Properties change) throws Exception {
      String payload = serialize(change);
      insertEvent(entityId, Event.TYPE_UPDATE, payload);
    }

    private String serialize(Properties change) throws IOException {
      change.store(writer, null);
      StringBuffer buffer = writer.getBuffer();
      String payload = buffer.toString();
      buffer.setLength(0);
      return payload;
    }

    private void insertEvent(int entityId, int type, String payload)
        throws SQLException {
      insertEvent.setInt(1, entityId);
      insertEvent.setInt(2, type);
      insertEvent.setString(3, payload);
      insertEvent.execute();
    }

    public void insertSnapshot(
        int entityId, List<Event> events, Properties change) throws Exception {
      Properties snapshot = buildSnapshot(events);
      snapshot.putAll(change);
      insertEvent(entityId, Event.TYPE_SNAPSHOT, serialize(snapshot));
    }

    public Properties buildSnapshot(List<Event> events) throws IOException {
      Properties snapshot = new Properties();
      for (Event event : events) {
          snapshot.load(new StringReader(event.payload));
      }
      return snapshot;
    }
  }
  
  public static class Event {
    public static final int TYPE_SNAPSHOT = 1;
    public static final int TYPE_UPDATE = 2;
    public static final int TYPE_DELETE = 3;
    
    public final int id;
    public final int entityId;
    public final int type;
    public final String payload;
    
    public Event(int id, int entityId, int type, String payload) {
      this.id = id;
      this.entityId = entityId;
      this.type = type;
      this.payload = payload;
    }
  }
  
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

  private static final int SNAPSHOT_INTERVAL = 10;

  @Override
  public Object begin() throws Exception {
    return pool.claim(timeout);
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
