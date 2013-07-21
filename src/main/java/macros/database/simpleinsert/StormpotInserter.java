package macros.database.simpleinsert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotInserter implements Inserter {
  public static class Dao implements Poolable {
    private final Slot slot;
    private final Connection connection;
    private final PreparedStatement insertLog;
    
    public Dao(Slot slot, Connection connection) throws Exception {
      this.slot = slot;
      this.connection = connection;
      insertLog = connection.prepareStatement(
          "insert into log (txt, x) values (?, ?)");
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
  }
  
  private final LifecycledPool<Dao> pool;
  private final Timeout timeout;

  public StormpotInserter(Database database, int poolSize) {
    final DataSource dataSource = database.createDataSource();
    Config<Dao> config = new Config<Dao>();
    Allocator<Dao> allocator = new Allocator<Dao>() {
      @Override
      public Dao allocate(Slot slot) throws Exception {
        return new Dao(slot, dataSource.getConnection());
      }
      @Override
      public void deallocate(Dao dao) throws Exception {
        dao.close();
      }
    };
    config.setAllocator(allocator);
    pool = new BlazePool<Dao>(config);
    timeout = new Timeout(10, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws Exception {
    pool.shutdown().await(timeout);
  }
  
  @Override
  public void insertLogRow(String txt, int x) throws Exception {
    Dao dao = pool.claim(timeout);
    try {
      dao.insertRow(txt, x);
    } finally {
      dao.release();
    }
  }
}
