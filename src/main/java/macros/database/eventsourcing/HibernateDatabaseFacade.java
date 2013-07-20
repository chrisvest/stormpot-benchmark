package macros.database.eventsourcing;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import macros.database.Database;

import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

public class HibernateDatabaseFacade implements DatabaseFacade {
  @Entity
  @Table(name = "event")
  @NamedQueries({
    @NamedQuery(
        name = "getRecent",
        query = "select e from HibernateDatabaseFacade$EventEntity e where e.entityId = :entityId " +
                "order by e.id desc")
  })
  public static class EventEntity {
    public static final int TYPE_SNAPSHOT = 1;
    public static final int TYPE_UPDATE = 2;
    public static final int TYPE_DELETE = 3;
    
    private Long id;
    
    private int entityId;
    
    private int type;
    
    private String payload;

    @Id @GeneratedValue
    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    @Column(name = "entity_id", nullable = false)
    public int getEntityId() {
      return entityId;
    }

    public void setEntityId(int entityId) {
      this.entityId = entityId;
    }
    
    @Column(name = "type", nullable = false)
    public int getType() {
      return type;
    }

    public void setType(int type) {
      this.type = type;
    }

    @Column(name = "payload", nullable = false, length = 4000)
    public String getPayload() {
      return payload;
    }

    public void setPayload(String payload) {
      this.payload = payload;
    }
  }
  
  public static class SessionAndTransaction {
    public Session session;
    public Transaction transaction;
  }
  
  private final SessionFactory sessionFactory;

  @SuppressWarnings("deprecation")
  public HibernateDatabaseFacade(Database database, int poolSize) {
    Configuration configuration = new Configuration()
      .addAnnotatedClass(EventEntity.class)
      .setProperty("hibernate.dialect", database.getDialect())
      .setProperty("hibernate.connection.driver_class", database.getDriver())
      .setProperty("hibernate.connection.url", database.getConnectionString())
      .setProperty("hibernate.connection.username", database.getUser())
      .setProperty("hibernate.connection.password", database.getPass())
      .setProperty("hibernate.c3p0.min_size", "" + poolSize)
      .setProperty("hibernate.c3p0.max_size", "" + poolSize)
      .setProperty("hibernate.c3p0.timeout", "1800")
      .setProperty("hibernate.c3p0.max_statements", "50")
      .setProperty("hibernate.jdbc.batch_size", "30")
      .setProperty("hibernate.show_sql", "false");
    sessionFactory = configuration.buildSessionFactory();
  }

  @Override
  public void close() {
    sessionFactory.close();
  }

  private static final int SNAPSHOT_INTERVAL = 10;

  @Override
  public Object begin() throws Exception {
    SessionAndTransaction stx = new SessionAndTransaction();
    stx.session = sessionFactory.openSession();
    stx.transaction = stx.session.beginTransaction();
    return stx;
  }

  @Override
  public void commit(Object tx) throws Exception {
    SessionAndTransaction stx = (SessionAndTransaction) tx;
    stx.transaction.commit();
    stx.session.close();
  }

  @Override
  public void rollback(Object tx) throws Exception {
    SessionAndTransaction stx = (SessionAndTransaction) tx;
    stx.transaction.rollback();
    stx.session.close();
  }

  @Override
  public void updateEntity(Object tx, int entityId, Properties change) throws Exception {
    Session session = ((SessionAndTransaction) tx).session;
    List<EventEntity> updates =
        getRecentEvents(session, entityId, SNAPSHOT_INTERVAL);
    EventEntity event = buildEntity(entityId, change, EventEntity.TYPE_UPDATE);
    session.persist(event);
    if (needsSnapshot(updates)) {
      insertSnapshot(session, entityId, updates, change);
    }
  }

  protected EventEntity buildEntity(int entityId, Properties change, int type)
      throws IOException {
    EventEntity event = new EventEntity();
    event.setEntityId(entityId);
    event.setType(type);
    event.setPayload(serialize(change));
    return event;
  }

  private void insertSnapshot(
      Session session, int entityId,
      List<EventEntity> updates, Properties change) throws Exception {
    Properties snapshot = buildSnapshot(updates);
    snapshot.putAll(change);
    EventEntity snapshotEntity =
        buildEntity(entityId, change, EventEntity.TYPE_SNAPSHOT);
    session.persist(snapshotEntity);
  }

  protected Properties buildSnapshot(List<EventEntity> updates)
      throws Exception {
    Properties snapshot = new Properties();
    for (EventEntity event : updates) {
      snapshot.load(new StringReader(event.getPayload()));
    }
    return snapshot;
  }

  private boolean needsSnapshot(List<EventEntity> events) {
    return events.size() == SNAPSHOT_INTERVAL
        && events.get(SNAPSHOT_INTERVAL - 1).getType() == EventEntity.TYPE_SNAPSHOT;
  }

  private String serialize(Properties change) throws IOException {
    StringWriter writer = new StringWriter();
    change.store(writer, null);
    StringBuffer buffer = writer.getBuffer();
    String payload = buffer.toString();
    buffer.setLength(0);
    return payload;
  }

  @Override
  public List<Properties> getRecentUpdates(
      Object tx, int entityId, int count) throws Exception {
    Session session = ((SessionAndTransaction) tx).session;
    List<EventEntity> events = getRecentEvents(session, entityId, count);
    List<Properties> list = new ArrayList<Properties>(count);
    for (EventEntity event : events) {
      Properties update = new Properties();
      update.load(new StringReader(event.getPayload()));
      list.add(update);
    }
    return list;
  }
  
  private List<EventEntity> getRecentEvents(Session session, int entityId, int count) {
    Query select = session.getNamedQuery("getRecent");
    select.setInteger("entityId", entityId);
    ScrollableResults scroll = select.scroll(ScrollMode.FORWARD_ONLY);
    List<EventEntity> list = new ArrayList<EventEntity>(count);
    int counter = 0;
    while (scroll.next() && counter < count) {
      EventEntity entity = (EventEntity) scroll.get(0);
      list.add(entity);
      counter++;
    }
    Collections.reverse(list);
    return list;
  }

  @Override
  public Properties getEntity(Object tx, int entityId) throws Exception {
    Session session = ((SessionAndTransaction) tx).session;
    List<EventEntity> events = getRecentEvents(session, entityId, SNAPSHOT_INTERVAL);
    return buildSnapshot(events);
  }
}
