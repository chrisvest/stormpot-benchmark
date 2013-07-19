package macros.database.simpleinsert;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import macros.database.Database;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateInserter implements Inserter {
  @Entity
  @Table(name = "log")
  public class LogEntity {
    private Long id;
    private String txt;
    private int x;
  
    @Id @GeneratedValue
    public Long getId() {
      return id;
    }
  
    public void setId(Long id) {
      this.id = id;
    }
  
    @Column(name ="txt", nullable = false, length = 255)
    public String getTxt() {
      return txt;
    }
  
    public void setTxt(String txt) {
      this.txt = txt;
    }
  
    @Column(name = "x", nullable = false)
    public int getX() {
      return x;
    }
  
    public void setX(int x) {
      this.x = x;
    }
  }

  private final SessionFactory sessionFactory;

  @SuppressWarnings("deprecation")
  public HibernateInserter(Database database, int poolSize) {
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
      .setProperty("hibernate.jdbc.batch_size", "30")
      .setProperty("hibernate.show_sql", "false");
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
