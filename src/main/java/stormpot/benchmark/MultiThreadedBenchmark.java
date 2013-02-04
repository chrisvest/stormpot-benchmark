package stormpot.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Recorder;

public class MultiThreadedBenchmark implements Benchmark {
  private static final int ITERATIONS = 2 * 1000 * 1000;
  private static final int THREADS = 4;

  private final PoolFactory factory;
  private ExecutorService executor;
  private PoolFacade pool;
  
  public MultiThreadedBenchmark(PoolFactory factory) {
    this.factory = factory;
  }

  @Override
  public void setUp() {
    executor = Executors.newFixedThreadPool(THREADS);
    pool = factory.create(10, 1000);
  }

  @Override
  public void runSession(Recorder recorder) throws Exception {
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(THREADS);
    List<Recorder> subRecorders = new ArrayList<Recorder>();
    
    for (int i = 0; i < THREADS; i++) {
      Recorder subRecorder = recorder.createBlankCopy();
      subRecorders.add(subRecorder);
      executor.execute(new Worker(pool, startLatch, endLatch, subRecorder));
    }
    
    startLatch.countDown();
    endLatch.await();
    
    for (Recorder subRecorder : subRecorders) {
      recorder.add(subRecorder);
    }
  }

  @Override
  public void tearDown() throws Exception {
    factory.shutdown(pool);
    executor.shutdown();
  }
  
  public static class Worker implements Runnable {
    private final PoolFacade pool;
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final Recorder recorder;

    public Worker(PoolFacade pool, CountDownLatch startLatch,
        CountDownLatch endLatch, Recorder recorder) {
      this.pool = pool;
      this.startLatch = startLatch;
      this.endLatch = endLatch;
      this.recorder = recorder;
    }

    @Override
    public void run() {
      try {
        startLatch.await();
        for (int i = 0; i < ITERATIONS; i++) {
          long begin = recorder.begin();
          pool.release(pool.claim());
          recorder.record(begin);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      endLatch.countDown();
    }
  }
  
  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(new MultiThreadedBenchmark(PoolFactory.blaze));
  }
}
