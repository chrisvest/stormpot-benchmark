package macros.database.shoppingcart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
    private String name;
    private int quantity;
    
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
    private final PreparedStatement reduceProductQuantity;
    
    public Dao(Connection con, Slot slot) throws SQLException {
      this.con = con;
      this.slot = slot;
      con.setAutoCommit(false);
      String insertOrderSql = "insert into \"order\" default values";
      if (ShoppingCartBenchmark.isMySQL(con)) {
        insertOrderSql = "insert into `order` () values ()";
      }
      insertOrder = con.prepareStatement(
          insertOrderSql, Statement.RETURN_GENERATED_KEYS);
      selectProduct = con.prepareStatement(
          "select id, name, quantity from product where id = ?");
      insertOrderLine = con.prepareStatement(
          "insert into orderline (orderId, productId) values (?, ?)",
          Statement.RETURN_GENERATED_KEYS);
      reduceProductQuantity = con.prepareStatement(
          "update product set quantity = quantity - 1 where id = ?");
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

    public void create(Order order) throws SQLException {
      insertOrder.execute();
      setKey(order, insertOrder);
    }

    private void setKey(Entity entity, Statement stmt) throws SQLException {
      ResultSet keys = stmt.getGeneratedKeys();
      String entityType = entity.getClass().getSimpleName();
      if (keys.next()) {
        entity.setId(keys.getInt(1));
      } else {
        System.err.println("No key generated for " + entityType + "!");
      }
      if (keys.next()) {
        System.err.println("More than one key generated for " + entityType + "!");
      }
    }

    public Product getProduct(int productId) throws SQLException {
      selectProduct.setInt(1, productId);
      ResultSet result = selectProduct.executeQuery();
      if (result.next()) {
        Product product = new Product();
        product.setId(result.getInt("id"));
        product.setName(result.getString("name"));
        product.setQuantity(result.getInt("quantity"));
        return product;
      }
      System.err.println("No such product: " + productId);
      return null;
    }

    public void create(OrderLine orderLine) throws SQLException {
      insertOrderLine.setInt(1, orderLine.getOrder().getId());
      insertOrderLine.setInt(2, orderLine.getProduct().getId());
      insertOrderLine.execute();
      setKey(orderLine, insertOrderLine);
    }

    public void reduceQuantity(Product product) throws SQLException {
      reduceProductQuantity.setInt(1, product.getId());
      reduceProductQuantity.execute();
      product.setQuantity(product.getQuantity() - 1);
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

  @Override
  public void doWork() {
    Dao dao = null;
    try {
      dao = pool.claim(timeout);
      doWork(dao);
      dao.commit();
    } catch (Exception e) {
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

  private void doWork(Dao dao) throws Exception {
    XorShiftRandom rng = new XorShiftRandom(seeder.nextInt());
    Order order = new Order();
    dao.create(order);
    for (int i = 0; i < 10; i++) {
      Product product = dao.getProduct(rng.nextInt() & 1023);
      OrderLine orderLine = new OrderLine();
      orderLine.setOrder(order);
      orderLine.setProduct(product);
      dao.create(orderLine);
      dao.reduceQuantity(product);
    }
  }

  @Override
  public void close() {
    try {
      pool.shutdown().await(timeout);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
