package stormpot.benchmark;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

import java.util.Queue;

import uk.co.real_logic.queues.OneToOneConcurrentArrayQueue3;

public class MessagePassingBenchmark implements Benchmark {

  private static final int REPITITIONS = 2 * 1000 * 1000;
  private static final int QUEUE_SIZE = 1024;
  
  private final PoolFactory factory;
  private Thread releaser;
  private PoolFacade pool;
  
  public MessagePassingBenchmark(
      @Param(value = "pools", defaults = "furious,queue") PoolFactory factory) {
    this.factory = factory;
  }

  @Override
  public void setUp() {
    this.pool = factory.create(QUEUE_SIZE, 1000);
  }

  @Override
  public void runSession(Recorder recorder) throws Exception {
    Queue<Object> queue = new OneToOneConcurrentArrayQueue3<Object>(QUEUE_SIZE);
    releaser = new Thread(new Releaser(queue, REPITITIONS, pool));
    releaser.start();
    
    for (int i = 0; i <= REPITITIONS; i++) {
      long start = recorder.begin();
      Object obj = pool.claim();
      recorder.record(start);
      while (!queue.offer(obj)) {
        Thread.yield();
      }
    }
    
    releaser.interrupt();
    releaser.join();
  }

  @Override
  public void tearDown() throws Exception {
    factory.shutdown(pool);
  }

  public class Releaser implements Runnable {
    private final Queue<Object> queue;
    private final int repititions;
    private final PoolFacade pool;

    public Releaser(Queue<Object> queue, int repititions, PoolFacade pool) {
      this.queue = queue;
      this.repititions = repititions;
      this.pool = pool;
    }

    @Override
    public void run() {
      Object obj;
      for (int i = 0; i <= repititions; i++) {
        while (null == (obj = queue.poll())) {
          Thread.yield();
        }
        try {
          pool.release(obj);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(MessagePassingBenchmark.class);
  }
}
