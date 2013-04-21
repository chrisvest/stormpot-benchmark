package macros.database;

import java.io.File;
import java.nio.file.Files;


import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;
import org.benchkit.htmlchartsreporter.DataInterpretor;
import org.benchkit.htmlchartsreporter.HtmlChartsReporter;

public class SimpleInsertionBenchmark extends DatabaseBenchmark {
  
  private static final class Interpretor implements DataInterpretor {
    public String getYaxisName(Object[] args) {
      return "Ops/Sec";
    }
    
    public String getBenchmarkName(Object[] args) {
      return "SimpleInsertionBenchmark";
    }

    @SuppressWarnings("unchecked")
    public Comparable<Object> getXvalue(Object[] args) {
      return (Comparable<Object>) args[1];
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
      String poolSize = String.valueOf(args[2]);
      String iterations = String.valueOf(args[3]);
      String database = String.valueOf(args[4]);
      String msg = "Simple Concurrent Insertion " +
      		"[poolSize=%s, iterations=%s, database=%s]";
      return String.format(msg, poolSize, iterations, database);
    }
  }

  public SimpleInsertionBenchmark(
      @Param(value = "fixture", defaults = "hibernate,stormpot") Fixture fixture,
      @Param(value = "threads", defaults = "1,2,3,4") int threads,
      @Param(value = "poolSize", defaults = "10") int poolSize,
      @Param(value = "iterations", defaults = "10000") int iterations,
      @Param(value = "database", defaults = "hsqldb") Database database) {
    this.fixture = fixture;
    this.threads = threads;
    this.poolSize = poolSize;
    this.iterations = iterations;
    this.database = database;
  }

  @Override
  protected void runBenchmark(Recorder recorder) throws Exception {
    long begin = recorder.begin();
    String name = Thread.currentThread().getName();
    for (int i = 0; i < iterations; i++) {
      facade.insertLogRow(name, i);
      begin = recorder.record(begin);
    }
  }

  public static void main(String[] args) throws Exception {
    HtmlChartsReporter reporter = new HtmlChartsReporter(new Interpretor());
    int iterations = BenchmarkRunner.DEFAULT_ITERATIONS;
    int warmupIterations = BenchmarkRunner.DEFAULT_WARMUP_ITERATIONS;
    
    BenchmarkRunner.run(
        SimpleInsertionBenchmark.class, reporter, iterations, warmupIterations);
    
    String report = reporter.generateReport();
    File file = new File("index.html");
    if (!file.exists()) file.createNewFile();
    Files.write(file.toPath(), report.getBytes("UTF-8"));
  }
}
