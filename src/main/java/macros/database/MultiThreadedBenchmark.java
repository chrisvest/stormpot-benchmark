package macros.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.benchkit.Benchmark;
import org.benchkit.Recorder;

public abstract class MultiThreadedBenchmark implements Benchmark {

  protected void doConcurrently(
      final Runnable runnable,
      final Recorder mainRecorder,
      final ExecutorService executor,
      final int threads,
      final int iterations) throws Exception {
    final CountDownLatch startLatch = new CountDownLatch(1);

    List<Future<Recorder>> recorders = createWorkers(
        runnable, mainRecorder, executor, threads, iterations, startLatch);

    Thread.yield();
    startLatch.countDown();

    collectResults(mainRecorder, recorders);
  }

  private List<Future<Recorder>> createWorkers(
      final Runnable runnable,
      final Recorder mainRecorder,
      final ExecutorService executor,
      final int threads,
      final int iterations,
      final CountDownLatch startLatch) {
    List<Future<Recorder>> recorders = new ArrayList<Future<Recorder>>();
    for (int i = 0; i < threads; i++) {
      final Recorder recorder = mainRecorder.createBlankCopy();
      Callable<Recorder> worker =
          createWorker(runnable, iterations, startLatch, recorder);
      Future<Recorder> futureRecorder = executor.submit(worker);
      recorders.add(futureRecorder);
    }
    return recorders;
  }

  private Callable<Recorder> createWorker(
      final Runnable runnable,
      final int iterations,
      final CountDownLatch startLatch,
      final Recorder recorder) {
    return new Callable<Recorder>() {
      @Override
      public Recorder call() throws Exception {
        startLatch.await();
        long begin = recorder.begin();
        for (int i = 0; i < iterations; i++) {
          runnable.run();
          begin = recorder.record(begin);
        }
        return recorder;
      }
    };
  }

  private void collectResults(
      final Recorder mainRecorder,
      List<Future<Recorder>> recorders) throws Exception {
    for (Future<Recorder> futureRecorder : recorders) {
      mainRecorder.add(futureRecorder.get());
    }
  }
}
