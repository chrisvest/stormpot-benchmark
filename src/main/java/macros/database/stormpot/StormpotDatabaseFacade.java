package macros.database.stormpot;

import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;
import macros.database.DatabaseFacade;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotDatabaseFacade implements DatabaseFacade {
  private final LifecycledPool<Dao> pool;
  private final Timeout timeout;

  public StormpotDatabaseFacade(Database database, int poolSize) {
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
