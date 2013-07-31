package macros.database.shoppingcart;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import macros.database.Database;
import macros.database.XorShiftRandom;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;

public class HibernateShoppingCart implements ShoppingCartWork {

  @Entity
  @Table(name = "`order`")
  private static class Order {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue
    private int id;
    
    @SuppressWarnings("unused")
    @Column(name = "state", nullable = false)
    private int state = 1;
    
    @SuppressWarnings("unused")
    @Column(name = "charge", nullable = false)
    private int charge = 0;
    
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "order")
    private List<OrderLine> orderLines = new ArrayList<>();

    public void setState(int state) {
      this.state = state;
    }

    public void setCharge(int charge) {
      this.charge = charge;
    }

    public List<OrderLine> getOrderLines() {
      return orderLines;
    }
  }

  @Entity
  @Table(name = "product")
  public static class Product {
    @Id
    @GeneratedValue
    private int id;
    
    @Column(name = "name", nullable = false, length = 4000)
    private String name;
    
    @Column(name = "quantity", nullable = false)
    private int quantity;
    
    @Column(name = "price", nullable = false)
    private int price;

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

    public int getPrice() {
      return price;
    }

    public void setPrice(int price) {
      this.price = price;
    }
  }

  @Entity
  @Table(name = "orderline")
  public static class OrderLine {
    @Id
    @GeneratedValue
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

  @SuppressWarnings("deprecation")
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
  
  private static interface Work {
    void doWork(Session session) throws Exception;
  }
  
  private final Work placeOrder = new Work() {
    public void doWork(Session session) throws Exception {
      XorShiftRandom rng = new XorShiftRandom(seeder.nextInt());
      Order order = new Order();
      session.persist(order);
      for (int i = 0; i < 10; i++) {
        int productId = rng.nextInt() & 1023;
        Product product = (Product) session.get(Product.class, productId);
        OrderLine orderLine = new OrderLine();
        orderLine.setOrder(order);
        orderLine.setProduct(product);
        session.persist(orderLine);
        order.getOrderLines().add(orderLine);
      }
    }
  };
  
  private final Work chargeOrders = new Work() {
    public void doWork(Session session) throws Exception {
      @SuppressWarnings("unchecked")
      List<Order> pendingOrders =
          session.createCriteria(Order.class)
          .add(Restrictions.eq("state", 1))
          .list();
      
      for (Order order : pendingOrders) {
        order.setState(2);
        int charge = 0;
        for (OrderLine orderLine : order.getOrderLines()) {
          charge += orderLine.getProduct().getPrice();
        }
        order.setCharge(charge);
        session.update(order);
      }
    }
  };
  
  private final Work shipOrders = new Work() {
    public void doWork(Session session) throws Exception {
      @SuppressWarnings("unchecked")
      List<Order> chargedOrders =
          session.createCriteria(Order.class)
          .add(Restrictions.eq("state", 2))
          .list();
      
      for (Order order : chargedOrders) {
        order.setState(3);
        for (OrderLine orderLine : order.getOrderLines()) {
          Product product = orderLine.getProduct();
          product.setQuantity(product.getQuantity() - 1);
          session.update(product);
        }
        session.update(order);
      }
    }
  };

  @Override
  public void doWork() {
    Session session = sessionFactory.openSession();
    try {
      doTransaction(session, placeOrder);
      if (seeder.nextInt(100) < 20) {
        doTransaction(session, chargeOrders);
      }
      if (seeder.nextInt(100) < 10) {
        doTransaction(session, shipOrders);
      }
    } finally {
      session.close();
    }
  }

  private void doTransaction(Session session, Work work) {
    Transaction tx = session.beginTransaction();
    try {
      work.doWork(session);
      tx.commit();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      tx.rollback();
    }
  }

  @Override
  public void close() {
    sessionFactory.close();
  }
}
