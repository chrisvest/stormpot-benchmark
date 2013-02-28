package macros.database;

import javax.persistence.*;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateDatabaseFacade implements DatabaseFacade {
  
  @Entity
  @Table(name = "event")
  public class Event {
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
  public HibernateDatabaseFacade(Database database, int poolSize) {
    Configuration configuration = new Configuration()
      .addAnnotatedClass(Event.class)
      .setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect")
      .setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver")
      .setProperty("hibernate.connection.url", database.getConnectionString())
      .setProperty("hibernate.connection.username", "root")
      .setProperty("hibernate.connection.password", "")
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
  public void insertRow(String txt, int x) {
    Event event = new Event();
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
