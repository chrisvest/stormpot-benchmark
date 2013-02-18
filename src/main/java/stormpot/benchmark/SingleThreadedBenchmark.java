package stormpot.benchmark;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

public class SingleThreadedBenchmark implements Benchmark {
  private final PoolFactory factory;
  private PoolFacade pool;

  public SingleThreadedBenchmark(
      @Param(value = "pools", defaults = "blaze") PoolFactory factory) {
    this.factory = factory;
  }
  
  @Override
  public void setUp() {
    pool = factory.create(10, 10000);
  }
  
  @Override
  public void runSession(Recorder recorder) throws Exception {
    long start = recorder.begin();
    for (int i = 0; i < 1000 * 1000; i++) {
      Object obj = pool.claim();
      pool.release(obj);
      start = recorder.record(start);
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    factory.shutdown(pool);
  }
  
  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(SingleThreadedBenchmark.class);
  }
}
