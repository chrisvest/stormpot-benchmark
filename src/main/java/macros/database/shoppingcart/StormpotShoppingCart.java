package macros.database.shoppingcart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import macros.database.XorShiftRandom;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotShoppingCart implements ShoppingCartWork {
  
  public static class Entity {
    private int id;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
  }
  
  public static class Order extends Entity {
  }
  
  public static class Product extends Entity {
  }
  
  public static class OrderLine extends Entity {
    private Order order;
    private Product product;
    
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
  
  public static class Dao implements Poolable {
    private final Connection con;
    private final Slot slot;
    private final PreparedStatement insertOrder;
    private final PreparedStatement selectProduct;
    private final PreparedStatement insertOrderLine;
    private final PreparedStatement reduceProductQuantities;
    private final PreparedStatement chargePendingOrders;
    private final PreparedStatement shipChargedOrders;
    
    public Dao(Connection con, Slot slot) throws SQLException {
      this.con = con;
      this.slot = slot;
      con.setAutoCommit(false);
      String insertOrderSql = "insert into \"order\" default values";
      String chargePendingOrdersSql =
          "update \"order\" as o " +
          "set state = 2, " +
          "charge = (" +
          "  select sum(p.price) " +
          "  from orderline ol " +
          "  inner join product p on (p.id = ol.productId) " +
          "  where ol.orderId = o.id) " +
          "where o.state = 1";
      String reduceProductQuantitiesSql =
          "update product p " +
          "set quantity = quantity - (" +
          "  select count(ol.id) " +
          "  from orderline ol, \"order\" o " +
          "  where ol.orderId = o.id " +
          "    and o.state = 2 " +
          "    and ol.productId = p.id) " +
          "where p.id in (" +
          "  select distinct ol.productId " +
          "  from orderline ol, \"order\" o" +
          "  where ol.orderId = o.id " +
          "    and o.state = 2)";
      String shipChargedOrdersSql =
          "update \"order\" set state = 3 where state = 2";
      
      if (ShoppingCartBenchmark.isMySQL(con)) {
        insertOrderSql = "insert into `order` () values ()";
        chargePendingOrdersSql = chargePendingOrdersSql.replace('"', '`');
        reduceProductQuantitiesSql = reduceProductQuantitiesSql.replace('"', '`');
        shipChargedOrdersSql = shipChargedOrdersSql.replace('"', '`');
      }
      
      insertOrder = con.prepareStatement(
          insertOrderSql, Statement.RETURN_GENERATED_KEYS);
      selectProduct = con.prepareStatement(
          "select id, name, quantity, price from product where id = ?");
      insertOrderLine = con.prepareStatement(
          "insert into orderline (orderId, productId) values (?, ?)",
          Statement.RETURN_GENERATED_KEYS);
      reduceProductQuantities = con.prepareStatement(reduceProductQuantitiesSql);
      chargePendingOrders = con.prepareStatement(chargePendingOrdersSql);
      shipChargedOrders = con.prepareStatement(shipChargedOrdersSql);
    }

    @Override
    public void release() {
      slot.release(this);
    }

    public void close() throws SQLException {
      con.close();
    }

    public void rollback() throws SQLException {
      con.rollback();
    }

    public void commit() throws SQLException {
      con.commit();
    }

    public void createOrder(Order order) throws SQLException {
      insertOrder.execute();
      setKey(order, insertOrder);
    }

    private void setKey(Entity entity, Statement stmt) throws SQLException {
      ResultSet keys = stmt.getGeneratedKeys();
      if (keys.next()) {
        entity.setId(keys.getInt(1));
      } else {
        System.err.println("No key generated for " + typeOf(entity) + "!");
      }
      if (keys.next()) {
        System.err.println("More than one key generated for " + typeOf(entity) + "!");
      }
    }

    private String typeOf(Entity entity) {
      return entity.getClass().getSimpleName();
    }

    public Product getProduct(int productId) throws SQLException {
      selectProduct.setInt(1, productId);
      ResultSet result = selectProduct.executeQuery();
      if (result.next()) {
        Product product = new Product();
        product.setId(result.getInt("id"));
        return product;
      }
      System.err.println("No such product: " + productId);
      return null;
    }

    public void createOrderLine(OrderLine orderLine) throws SQLException {
      insertOrderLine.setInt(1, orderLine.getOrder().getId());
      insertOrderLine.setInt(2, orderLine.getProduct().getId());
      insertOrderLine.execute();
      setKey(orderLine, insertOrderLine);
    }

    public void chargePendingOrders() throws SQLException {
      chargePendingOrders.execute();
    }

    public void reduceProductQuantities() throws SQLException {
      reduceProductQuantities.execute();
    }

    public void shipChargedOrders() throws SQLException {
      shipChargedOrders.execute();
    }
  }

  private final LifecycledPool<Dao> pool;
  private final Random seeder = new Random(1234);
  private final Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
  
  public StormpotShoppingCart(final DataSource ds, int poolSize) {
    Config<Dao> config = new Config<Dao>();
    config.setSize(poolSize);
    config.setAllocator(new Allocator<Dao>() {
      @Override
      public Dao allocate(Slot slot) throws Exception {
        return new Dao(ds.getConnection(), slot);
      }

      @Override
      public void deallocate(Dao poolable) throws Exception {
        poolable.close();
      }
    });
    pool = new BlazePool<Dao>(config);
  }
  
  private static interface Work {
    void doWork(Dao dao) throws Exception;
  }

  private final Work placeOrder = new Work() {
    public void doWork(Dao dao) throws Exception {
      XorShiftRandom rng = new XorShiftRandom(seeder.nextInt());
      Order order = new Order();
      dao.createOrder(order);
      for (int i = 0; i < 10; i++) {
        Product product = dao.getProduct(rng.nextInt() & 1023);
        OrderLine orderLine = new OrderLine();
        orderLine.setOrder(order);
        orderLine.setProduct(product);
        dao.createOrderLine(orderLine);
      }
    }
  };
  
  private final Work chargeOrders = new Work() {
    public void doWork(Dao dao) throws Exception {
      dao.chargePendingOrders();
    }
  };
  
  private final Work shipOrders = new Work() {
    public void doWork(Dao dao) throws Exception {
      dao.reduceProductQuantities();
      dao.shipChargedOrders();
    }
  };
  
  private AtomicInteger errorCounter = new AtomicInteger();
  
  @Override
  public void doWork() {
    int r = seeder.nextInt(100);
    if (r < 10) {
      doTransaction(placeOrder, chargeOrders, shipOrders);
    } else if (r < 20) {
      doTransaction(placeOrder, chargeOrders);
    } else {
      doTransaction(placeOrder);
    }
  }

  private void doTransaction(Work... works) {
    Dao dao = null;
    try {
      dao = pool.claim(timeout);
      for (Work work : works) {
        work.doWork(dao);
      }
      dao.commit();
    } catch (Exception e) {
      errorCounter.incrementAndGet();
      System.err.println(e.getMessage());
      if (dao != null) {
        try {
          dao.rollback();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
    } finally {
      if (dao != null) {
        dao.release();
      }
    }
  }

  @Override
  public void close() {
    System.out.println("Error Count [Stormpot] = " + errorCounter.get());
    try {
      pool.shutdown().await(timeout);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
