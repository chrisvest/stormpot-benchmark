package macros.database;

import java.io.File;
import java.nio.file.Files;


import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;
import org.benchkit.htmlchartsreporter.DataInterpretor;
import org.benchkit.htmlchartsreporter.HtmlChartsReporter;
import org.benchkit.htmlchartsreporter.ThroughputChart;

public class SimpleInsertionBenchmark extends DatabaseBenchmark {
  
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
    HtmlChartsReporter chartReporter = new HtmlChartsReporter(
        new Interpretor(), "Simple Concurrent Insertion [poolSize=%3$s, iterations=%4$s, database=%5$s]");
    chartReporter.addChartRender(new ThroughputChart("Throughput", "Threads", "Ops/Sec"));
    int iterations = BenchmarkRunner.DEFAULT_ITERATIONS;
    int warmupIterations = BenchmarkRunner.DEFAULT_WARMUP_ITERATIONS;
    
    BenchmarkRunner.run(
        SimpleInsertionBenchmark.class, chartReporter, iterations, warmupIterations);
    
    String report = chartReporter.generateReport();
    File file = new File("simple-concurrent-insertion.html");
    if (!file.exists()) file.createNewFile();
    Files.write(file.toPath(), report.getBytes("UTF-8"));
  }
}
