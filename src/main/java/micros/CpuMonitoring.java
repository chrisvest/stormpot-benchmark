package micros;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;
import stormpot.qpool.QueuePool;

/**
 * Exercise the pools so we can look at them under a microscope.
 * 
 * @author cvh
 */
public class CpuMonitoring {
  private static final int ITERATIONS = 100000;

  private static class MyPoolable implements Poolable {
    private final Slot slot;
    
    public MyPoolable(Slot slot) {
      this.slot = slot;
    }

    @Override
    public void release() {
      slot.release(this);
    }
  }
  
  private static class MyAllocator implements Allocator<MyPoolable> {
    @Override
    public MyPoolable allocate(Slot slot) throws Exception {
      return new MyPoolable(slot);
    }

    @Override
    public void deallocate(MyPoolable obj) throws Exception {
    }
  }
  
  
  private static final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
  private static final ExecutorService executor = Executors.newFixedThreadPool(10);
  
  private static BlazePool<MyPoolable> blazePool;
  private static QueuePool<MyPoolable> queuePool;

  public static void main(String[] args) throws Exception {
    Config<MyPoolable> config = new Config<MyPoolable>();
    config.setAllocator(new MyAllocator());
    
    blazePool = new BlazePool<MyPoolable>(config);
    queuePool = new QueuePool<MyPoolable>(config);
    
    System.out.println("Press a key to start...");
    System.in.read();
    for (;;) {
      System.out.println("Blaze...");
      runBlaze();
      System.out.println("Done...");
      System.in.read();
      System.out.println("Queue...");
      runQueue();
      System.out.println("Done...");
      System.in.read();
    }
  }

  private static final Callable<Void> blazeRunner = new Callable<Void>() {
    public Void call() throws Exception {
      for (int i = 0; i < ITERATIONS; i++) {
        blazePool.claim(timeout).release();
      }
      return null;
    }
  };
  
  private static final Callable<Void> queueRunner = new Callable<Void>() {
    public Void call() throws Exception {
      for (int i = 0; i < ITERATIONS; i++) {
        queuePool.claim(timeout).release();
      }
      return null;
    }
  };
  
  private static void runBlaze() throws Exception {
    List<Callable<Void>> tasks = Collections.nCopies(10, blazeRunner);
    awaitAll(executor.invokeAll(tasks));
  }

  private static void runQueue() throws Exception {
    List<Callable<Void>> tasks = Collections.nCopies(10, queueRunner);
    awaitAll(executor.invokeAll(tasks));
  }

  private static <T> void awaitAll(List<Future<T>> futures) throws Exception {
    for (Future<T> future : futures) {
      future.get();
    }
  }
}
