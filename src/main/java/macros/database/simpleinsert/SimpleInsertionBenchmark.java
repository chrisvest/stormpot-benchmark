package macros.database.simpleinsert;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;


import macros.database.Database;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;
import org.benchkit.WarmupPrintingReporter;
import org.benchkit.htmlchartsreporter.DataInterpretor;
import org.benchkit.htmlchartsreporter.HtmlChartsReporter;
import org.benchkit.htmlchartsreporter.LatencyHistogramChart;
import org.benchkit.htmlchartsreporter.ThroughputChart;

public class SimpleInsertionBenchmark implements Benchmark {
  
  private static final class Interpretor implements DataInterpretor {
    public String getBenchmarkName(Object[] args) {
      return "SimpleInsertionBenchmark";
    }

    @SuppressWarnings("unchecked")
    public Comparable<Object> getXvalue(Object[] args) {
      return (Comparable<Object>) args[1];
    }

    @Override
    public String getSeriesName(Object[] args) {
      return String.valueOf(args[0]);
    }
  }
  
  private Fixture fixture;
  private int threads;
  private int poolSize;
  private int iterations;
  private Database database;
  private DataSource dataSource;
  private ExecutorService executor;
  private Inserter inserter;

  public SimpleInsertionBenchmark(
      @Param(value = "fixture", defaults = "hibernate,stormpot") Fixture fixture,
      @Param(value = "threads", defaults = "1,2,3,4,5,6,7,8") int threads,
      @Param(value = "poolSize", defaults = "10") int poolSize,
      @Param(value = "iterations", defaults = "10000") int iterations,
      @Param(value = "database", defaults = "h2") Database database) {
    this.fixture = fixture;
    this.threads = threads;
    this.poolSize = poolSize;
    this.iterations = iterations;
    this.database = database;
  }

  @Override
  public void setUp() throws Exception {
    dataSource = database.createDataSource();
    executor = Executors.newCachedThreadPool();
    database.createDatabase(dataSource);
    inserter = fixture.init(database, poolSize);
  }

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Executor did not shut down fast enough");
    }
    inserter.close();
    database.shutdownAll();
  }

  @Override
  public void runSession(Recorder mainRecorder) throws Exception {
    clearDatabase();
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Recorder>> recorders = startWorkers(mainRecorder, startLatch);
    Thread.yield();
    startLatch.countDown();
    collectResults(mainRecorder, recorders);
  }

  private void clearDatabase() throws Exception {
    Connection connection = dataSource.getConnection();
    try {
      database.tryUpdate(connection, "truncate table log");
    } finally {
      connection.close();
    }
  }

  private List<Future<Recorder>> startWorkers(Recorder mainRecorder,
      CountDownLatch startLatch) {
    List<Future<Recorder>> recorders = new ArrayList<Future<Recorder>>();
    for (int i = 0; i < threads; i++) {
      Recorder recorder = mainRecorder.createBlankCopy();
      Future<Recorder> futureRecorder =
          executor.submit(createWorker(startLatch, recorder));
      recorders.add(futureRecorder);
    }
    return recorders;
  }

  private Callable<Recorder> createWorker(
      final CountDownLatch startLatch,
      final Recorder recorder) {
    return new Callable<Recorder>() {
      @Override
      public Recorder call() throws Exception {
        startLatch.await();
        runBenchmark(recorder);
        return recorder;
      }
    };
  }

  private void collectResults(Recorder mainRecorder,
      List<Future<Recorder>> recorders) throws InterruptedException,
      ExecutionException {
    for (Future<Recorder> futureRecorder : recorders) {
      mainRecorder.add(futureRecorder.get());
    }
  }

  private void runBenchmark(Recorder recorder) throws Exception {
    String name = Thread.currentThread().getName();
    long begin = recorder.begin();
    for (int i = 0; i < iterations; i++) {
      inserter.insertLogRow(name, i);
      begin = recorder.record(begin);
    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.OFF);
    
    HtmlChartsReporter chartReporter = new HtmlChartsReporter(
        new Interpretor(), "Simple Concurrent Insertion [poolSize=%3$s, iterations=%4$s, database=%5$s]");
    chartReporter.addChartRender(new ThroughputChart("Throughput", "Threads", "Ops/Sec"));
    chartReporter.addChartRender(new LatencyHistogramChart("Latency", "Threads"));
    int iterations = BenchmarkRunner.DEFAULT_ITERATIONS;
    int warmupIterations = BenchmarkRunner.DEFAULT_WARMUP_ITERATIONS;

    BenchmarkRunner.run(
        SimpleInsertionBenchmark.class, new WarmupPrintingReporter(), 1, 3);
    BenchmarkRunner.run(
        SimpleInsertionBenchmark.class, chartReporter, iterations, warmupIterations);
    
    String report = chartReporter.generateReport();
    File file = new File("simple-concurrent-insertion.html");
    if (!file.exists()) file.createNewFile();
    Files.write(file.toPath(), report.getBytes("UTF-8"));
  }
}
