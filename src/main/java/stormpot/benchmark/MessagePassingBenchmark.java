package stormpot.benchmark;

import uk.co.real_logic.queues.OneToOneConcurrentArrayQueue3;

public class MessagePassingBenchmark extends Benchmark {
  private static final Object POISON_PILL = new Object();

  @Override
  protected String getBenchmarkName() {
    return "Message Passing Benchmark";
  }

  @Override
  protected int[] warmupCycles() {
    return new int[] {1, 11, 1, 1};
  }
  
  private OneToOneConcurrentArrayQueue3<Object> queue;

  @Override
  protected void beforeBenchmark(final Bench bench, long trialTimeMillis) {
    super.beforeBenchmark(bench, trialTimeMillis);
    queue = new OneToOneConcurrentArrayQueue3<Object>(SIZE);
    Runnable releaser = new Runnable() {
      @Override
      public void run() {
        for (;;) {
          Object obj;
          do {
            obj = queue.poll();
          } while (obj == null);
          if (obj == POISON_PILL) {
            return;
          }
          try {
            bench.release(obj);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    new Thread(releaser).start();
  }

  @Override
  protected void benchmark(Bench bench, long trialTimeMillis) throws Exception {
    long start = System.currentTimeMillis();
    long deadline = start + trialTimeMillis;
    long end = 0L;
    do {
      end = cycles(bench, 100);
    } while (end < deadline);
    queue.offer(POISON_PILL);
    bench.recordPeriod(end - start);
  }

  private long cycles(Bench bench, int times) throws Exception {
    long start;
    long end = 0;
    for (int i = 0; i < times; i++) {
      start = now();
      Object obj = bench.claim();
      queue.offer(obj);
      end = now();
      bench.recordTime(end - start);
    }
    return end == 0? System.currentTimeMillis() : end;
  }
}
