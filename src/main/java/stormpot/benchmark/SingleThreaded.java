package stormpot.benchmark;

import java.util.concurrent.TimeUnit;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Slot;
import stormpot.Timeout;
import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Recorder;
import stormpot.bpool.BlazePool;
import stormpot.qpool.QueuePool;

public class SingleThreaded implements Benchmark {
  private static final Timeout TIMEOUT = new Timeout(1, TimeUnit.SECONDS);
  
  private LifecycledPool<MyPoolable> pool;

  @Override
  public void setUp() {
    Config<MyPoolable> config = new Config<MyPoolable>();
    config.setAllocator(new Allocator<MyPoolable>() {
      @Override
      public MyPoolable allocate(Slot slot) throws Exception {
        return new MyPoolable(slot);
      }

      @Override
      public void deallocate(MyPoolable obj) throws Exception {
      }
    });
    pool = new QueuePool<MyPoolable>(config);
//    pool = new BlazePool<MyPoolable>(config);
  }
  
  @Override
  public void setUpSession() {
  }

  @Override
  public void runSession(Recorder mainRecorder) throws Exception {
    Timeout timeout = TIMEOUT;
    
    for (int i = 0; i < 1000 * 1000; i++) {
      long begin = mainRecorder.begin();
      MyPoolable obj = pool.claim(timeout);
      obj.release();
      mainRecorder.record(begin);
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    pool.shutdown().await(TIMEOUT);
  }

  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(new SingleThreaded());
  }
}
