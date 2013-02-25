package macros.database;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotDatabaseFacade implements DatabaseFacade {
  private static class Dao implements Poolable {
    private final Slot slot;
    private final Connection connection;
    
    public Dao(Slot slot, Connection connection) {
      this.slot = slot;
      this.connection = connection;
    }

    @Override
    public void release() {
      slot.release(this);
    }

    public void close() throws Exception {
      connection.close();
    }

    public void clearDatabase() {
      // TODO Auto-generated method stub
      
    }

    public void insertRow(String txt, int x) {
      // TODO Auto-generated method stub
      
    }
  }
  
  private final LifecycledPool<Dao> pool;
  private final Timeout timeout;

  public StormpotDatabaseFacade(final DataSource dataSource, int poolSize) {
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
  public void clearDatabase() throws Exception {
    pool.claim(timeout).clearDatabase();
  }

  @Override
  public void insertRow(String txt, int x) throws Exception {
    pool.claim(timeout).insertRow(txt, x);
  }
}
