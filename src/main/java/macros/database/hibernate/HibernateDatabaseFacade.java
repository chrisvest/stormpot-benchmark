package macros.database.hibernate;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import macros.database.Database;
import macros.database.DatabaseFacade;

import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateDatabaseFacade implements DatabaseFacade {
  
  private final SessionFactory sessionFactory;

  @SuppressWarnings("deprecation")
  public HibernateDatabaseFacade(Database database, int poolSize) {
    Configuration configuration = new Configuration()
      .addAnnotatedClass(LogEntity.class)
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
      .setProperty("hibernate.jdbc.batch_size", "30");
    sessionFactory = configuration.buildSessionFactory();
  }

  @Override
  public void close() {
    sessionFactory.close();
  }

  @Override
  public void insertLogRow(String txt, int x) {
    LogEntity event = new LogEntity();
    event.setTxt(txt);
    event.setX(x);
    Session session = sessionFactory.openSession();
    try {
      session.save(event);
    } finally {
      session.close();
    }
  }

  private static final int SNAPSHOT_INTERVAL = 10;

  @Override
  public Object begin() throws Exception {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    return session;
  }

  @Override
  public void commit(Object tx) throws Exception {
    Session session = (Session) tx;
    session.getTransaction().commit();
    session.close();
  }

  @Override
  public void rollback(Object tx) throws Exception {
    Session session = (Session) tx;
    session.getTransaction().rollback();
    session.close();
  }

  @Override
  public void updateEntity(Object tx, int entityId, Properties change) throws Exception {
    Session session = (Session) tx;
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
    Session session = (Session) tx;
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
    Session session = (Session) tx;
    List<EventEntity> events = getRecentEvents(session, entityId, SNAPSHOT_INTERVAL);
    return buildSnapshot(events);
  }
}
