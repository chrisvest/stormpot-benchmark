package macros.database.stormpot;

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

import stormpot.Poolable;
import stormpot.Slot;

class Dao implements Poolable {
  private final Slot slot;
  private final Connection connection;
  private final PreparedStatement insertLog;
  private final PreparedStatement recentEvents;
  private final PreparedStatement insertEvent;
  private final StringWriter writer;
  
  public Dao(Slot slot, Connection connection) throws Exception {
    this.slot = slot;
    this.connection = connection;
    this.writer = new StringWriter();
    connection.setAutoCommit(true);
    insertLog = connection.prepareStatement(
        "insert into log (txt, x) values (?, ?)");
    recentEvents = connection.prepareStatement(
        "select * from event where entity_id = ? order by id asc limit ?");
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

  public void insertRow(String txt, int x) throws Exception {
    insertLog.setString(1, txt);
    insertLog.setInt(2, x);
    insertLog.execute();
  }

  public void begin() throws Exception {
    connection.setAutoCommit(false);
  }

  public void commit() throws Exception {
    try {
      connection.commit();
    } finally {
      connection.setAutoCommit(true);
    }
  }

  public void rollback() throws Exception {
    try {
      connection.rollback();
    } finally {
      connection.setAutoCommit(true);
    }
  }

  public List<Event> getRecentEvents(int entityId, int count) throws Exception {
    List<Event> events = new ArrayList<Event>(10);
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
    // create defensive copy because Collections.reverse is destructive:
    events = new ArrayList<Event>(events);
    Collections.reverse(events);
    // there's no need to load up events prior to the most recent snapshot,
    // so we skip them
//    boolean snapshotReached = false;
    for (Event event : events) {
//      if (!snapshotReached && event.type == Event.TYPE_SNAPSHOT) {
//        snapshotReached = true;
//      }
//      if (snapshotReached) {
        snapshot.load(new StringReader(event.payload));
//      }
    }
    return snapshot;
  }
}
