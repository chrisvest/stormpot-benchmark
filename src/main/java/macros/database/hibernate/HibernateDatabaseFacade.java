package macros.database.hibernate;

import macros.database.Database;
import macros.database.DatabaseFacade;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateDatabaseFacade implements DatabaseFacade {
  
  private final SessionFactory sessionFactory;

  @SuppressWarnings("deprecation")
  public HibernateDatabaseFacade(Database database, int poolSize) {
    Configuration configuration = new Configuration()
      .addAnnotatedClass(LogEntity.class)
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
}
