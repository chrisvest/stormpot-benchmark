package stormpot.benchmark;

import java.util.concurrent.TimeUnit;

import stormpot.Config;
import stormpot.TimeExpiration;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class BlazePoolBench extends Bench {
  private static final Timeout timeout =
      new Timeout(1000, TimeUnit.MILLISECONDS);
  
  private BlazePool<MyPoolable> pool;

  @Override
  public void primeWithSize(int size, long objTtlMillis) {
    Config<MyPoolable> config = new Config<MyPoolable>();
    config.setAllocator(new StormpotPoolableAllocator());
    config.setSize(size);
    config.setExpiration(
        new TimeExpiration(objTtlMillis, TimeUnit.MILLISECONDS));
    pool = new BlazePool<MyPoolable>(config);
  }

  @Override
  public Object claim() throws Exception {
    return pool.claim(timeout);
  }

  @Override
  public void release(Object object) throws Exception {
    ((MyPoolable) object).release();
  }

  @Override
  public String getName() {
    return pool.getClass().getSimpleName();
  }
}
