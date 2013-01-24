package stormpot.benchmark;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import uk.co.real_logic.queues.OneToOneConcurrentArrayQueue3;

public class MessagePassingBenchmark extends Benchmark {

  private static final int REPITITIONS = 10 * 1000 * 1000;

  @Override
  protected String getBenchmarkName() {
    return "Message Passing Benchmark";
  }

  @Override
  protected int[] warmupCycles() {
    return new int[] {1, 11, 1, 8, 1};
  }
  
  @Override
  protected void warmup(Bench bench, int steps) throws Exception {
    System.out.println("Warming up with " + steps + " mio. repititions...");
    benchmark(steps * 1000 * 1000, bench);
  }

  @Override
  protected void benchmark(Bench bench, long trialTimeMillis) throws Exception {
    int repititions = REPITITIONS;
    benchmark(repititions, bench);
  }

  private void benchmark(int repititions, Bench bench) throws Exception {
    bench.recordTime(0);
//    final Queue<Object> queue = new ArrayBlockingQueue<Object>(SIZE);
    final Queue<Object> queue = new OneToOneConcurrentArrayQueue3<Object>(SIZE);
    Thread releaser = new Thread(new Releaser(queue, repititions, bench));
    releaser.start();
    long start = System.currentTimeMillis();
    
    cycles(bench, repititions, queue);
    
    releaser.join();
    long end = System.currentTimeMillis();
    bench.setTrials(repititions);
    bench.recordPeriod(end - start);
  }

  private void cycles(Bench bench, int times, Queue<Object> queue) throws Exception {
    for (int i = 0; i <= times; i++) {
      Object obj = bench.claim();
      while (!queue.offer(obj)) {
        Thread.yield();
      }
    }
  }
  
  public class Releaser implements Runnable {
    private final Queue<Object> queue;
    private final int repititions;
    private final Bench bench;

    public Releaser(Queue<Object> queue, int repititions, Bench bench) {
      this.queue = queue;
      this.repititions = repititions;
      this.bench = bench;
    }

    @Override
    public void run() {
      Object obj;
      for (int i = 0; i <= repititions; i++) {
        while (null == (obj = queue.poll())) {
          Thread.yield();
        }
        bench.release(obj);
      }
    }
  }
}
