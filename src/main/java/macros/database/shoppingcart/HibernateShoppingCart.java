package macros.database.shoppingcart;

import java.util.Random;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import macros.database.Database;
import macros.database.XorShiftRandom;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

public class HibernateShoppingCart implements ShoppingCartWork {

  @Entity
  @Table(name = "`order`")
  private static class Order {
    @Id @GeneratedValue
    private int id;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
  }

  @Entity
  @Table(name = "product")
  public static class Product {
    @Id @GeneratedValue
    private int id;
    
    @Column(name = "name", nullable = false, length = 4000)
    private String name;
    
    @Column(name = "quantity", nullable = false)
    private int quantity;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
    
    public String getName() {
      return name;
    }
    
    public void setName(String name) {
      this.name = name;
    }
    
    public int getQuantity() {
      return quantity;
    }
    
    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
  }

  @Entity
  @Table(name = "orderline")
  public static class OrderLine {
    @Id @GeneratedValue
    private int id;
    
    @ManyToOne
    @JoinColumn(name = "orderId", nullable = false)
    private Order order;
    
    @ManyToOne
    @JoinColumn(name = "productId", nullable = false)
    private Product product;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
    
    public Order getOrder() {
      return order;
    }
    
    public void setOrder(Order order) {
      this.order = order;
    }
    
    public Product getProduct() {
      return product;
    }
    
    public void setProduct(Product product) {
      this.product = product;
    }
  }

  private SessionFactory sessionFactory;
  private final Random seeder = new Random(1234);

  public HibernateShoppingCart(Database database, int poolSize) {
    Configuration configuration = new Configuration()
      .addAnnotatedClass(Order.class)
      .addAnnotatedClass(Product.class)
      .addAnnotatedClass(OrderLine.class)
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
  public void doWork() {
    Session session = sessionFactory.openSession();
    try {
      Transaction tx = session.beginTransaction();
      try {
        doWork(session);
        tx.commit();
      } catch (Exception e) {
//        System.err.println(e.getMessage());
        e.printStackTrace();
        tx.rollback();
      }
    } finally {
      session.close();
    }
  }

  private void doWork(Session session) {
    XorShiftRandom rng = new XorShiftRandom(seeder.nextInt());
    Order order = new Order();
    session.persist(order);
    for (int i = 0; i < 10; i++) {
      int productId = rng.nextInt() & 1023;
      Product product = (Product) session.get(Product.class, productId);
      OrderLine orderLine = new OrderLine();
      orderLine.setOrder(order);
      orderLine.setProduct(product);
      product.setQuantity(product.getQuantity() - 1);
      session.persist(orderLine);
    }
  }

  @Override
  public void close() {
    sessionFactory.close();
  }

}
