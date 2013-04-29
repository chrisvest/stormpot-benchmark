package stormpot.benchmark;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.PrintingReporter;
import org.benchkit.Recorder;
import org.benchkit.WarmupPrintingReporter;
import org.benchkit.htmlchartsreporter.DataInterpretor;
import org.benchkit.htmlchartsreporter.HtmlChartsReporter;

import java.io.File;
import java.nio.file.Files;
import java.util.Queue;

import uk.co.real_logic.queues.OneToOneConcurrentArrayQueue3;

public class MessagePassingBenchmark implements Benchmark {

  private final int repititions;
  private final int poolSize;
  
  private final PoolFactory factory;
  private Thread releaser;
  private PoolFacade pool;
  
  public MessagePassingBenchmark(
      @Param(value = "pools", defaults = "stack,generic,queue,blaze,furious") PoolFactory factory,
      @Param(value = "poolSize", defaults = "1024") int poolSize,
      @Param(value = "repititions", defaults = "2000000") int repititions) {
    this.factory = factory;
    this.poolSize = poolSize;
    this.repititions = repititions;
  }

  @Override
  public void setUp() {
    this.pool = factory.create(poolSize, 1000);
  }

  @Override
  public void runSession(Recorder recorder) throws Exception {
    Queue<Object> queue = new OneToOneConcurrentArrayQueue3<Object>(poolSize);
    releaser = new Thread(new Releaser(queue, repititions, pool));
    releaser.start();
    
    long start = recorder.begin();
    for (int i = 0; i <= repititions; i++) {
      Object obj = pool.claim();
      start = recorder.record(start);
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
    HtmlChartsReporter chartReporter = new HtmlChartsReporter(new Interpretor());
    PrintingReporter printingReporter = new PrintingReporter();
    WarmupPrintingReporter warmupReporter = new WarmupPrintingReporter();
    
    // Pre-heat the lot:
    System.out.println("## Warmup");
    BenchmarkRunner.run(
        MessagePassingBenchmark.class,
        warmupReporter, 4, 0);
    BenchmarkRunner.run(
        MessagePassingBenchmark.class,
        warmupReporter, 1, 0);
    
    // The real run:
    System.out.println("## Benchmark");
    BenchmarkRunner.run(
        MessagePassingBenchmark.class,
        printingReporter,
        chartReporter,
        5, 0);
    
    String report = chartReporter.generateReport();
    File file = new File("message-passing-results.html");
    if (!file.exists()) file.createNewFile();
    Files.write(file.toPath(), report.getBytes("UTF-8"));
  }
  
  private static final class Interpretor implements DataInterpretor {
    public String getYaxisName(Object[] args) {
      return "Ops/Sec";
    }
    
    public String getBenchmarkName(Object[] args) {
      return "Message Passing Benchmark";
    }

    @SuppressWarnings("unchecked")
    public Comparable<Object> getXvalue(Object[] args) {
      Object value = String.valueOf(args[0]);
      return (Comparable<Object>) value;
    }

    @Override
    public String getXaxisName(Object[] args) {
      return "Threads";
    }

    @Override
    public String getSeriesName(Object[] args) {
      return String.valueOf(args[0]);
    }

    @Override
    public String getChartName(Object[] args) {
      String poolSize = String.valueOf(args[1]);
      String iterations = String.valueOf(args[2]);
      String msg = "Message Passing Benchmark " +
          "[poolSize=%s, iterations=%s]";
      return String.format(msg, poolSize, iterations);
    }
  }
}
