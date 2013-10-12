package macros.database.shoppingcart;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;
import macros.database.MultiThreadedBenchmark;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;
import org.benchkit.WarmupPrintingReporter;
import org.benchkit.htmlchartsreporter.DataInterpretor;
import org.benchkit.htmlchartsreporter.HtmlChartsReporter;
import org.benchkit.htmlchartsreporter.LatencyHistogramChart;
import org.benchkit.htmlchartsreporter.ThroughputChart;

public class ShoppingCartBenchmark extends MultiThreadedBenchmark {
  
  private static final class Interpretor implements DataInterpretor {
    public String getBenchmarkName(Object[] args) {
      return "Shopping Cart Benchmark";
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
  private ShoppingCartWork work;

  public ShoppingCartBenchmark(
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
    work = fixture.init(database, poolSize);
    System.out.printf("Benchmarking %s threads doing %s iterations with %s on %s\n",
        threads, iterations, fixture, database);
  }

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Executor did not shut down fast enough");
    }
    work.close();
    database.shutdownAll();
  }

  @Override
  public void runSession(Recorder mainRecorder) throws Exception {
    prepareDatabase();
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        work.doWork();
      }
    };
    doConcurrently(runnable, mainRecorder, executor, threads, iterations);
  }

  private void prepareDatabase() throws Exception {
    Connection connection = dataSource.getConnection();
    try {
      mysqlAnsiQuoteModeHack(connection);
      connection.setAutoCommit(false);
      database.update(connection, "delete from orderline");
      database.update(connection, "delete from \"order\"");
      database.update(connection, "delete from product");
      
      Random rnd = new Random(1234);
      PreparedStatement insertProducts = connection.prepareStatement(
          "insert into product (id, name, quantity, price) values (?, ?, ?, ?)");
      for (int i = 0; i < 1024; i++) {
        insertProducts.setInt(1, i);
        insertProducts.setString(2, UUID.randomUUID().toString());
        insertProducts.setInt(3, 1000);
        insertProducts.setInt(4, 10 + rnd.nextInt(90));
        insertProducts.execute();
      }
      connection.commit();
    } finally {
      connection.close();
    }
  }

  public static void mysqlAnsiQuoteModeHack(Connection con) throws SQLException {
    if (isMySQL(con)) {
      Statement modeChange = con.createStatement();
      modeChange.execute("SET SESSION SQL_MODE=ANSI_QUOTES");
      modeChange.close();
    }
  }

  public static boolean isMySQL(Connection con) {
    return con instanceof com.mysql.jdbc.JDBC4Connection;
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.OFF);
    
    HtmlChartsReporter chartReporter = new HtmlChartsReporter(
        new Interpretor(), "Shopping Cart [poolSize=%3$s, iterations=%4$s, database=%5$s]");
    chartReporter.addChartRender(new ThroughputChart("Throughput", "Threads", "Ops/Sec"));
    chartReporter.addChartRender(new LatencyHistogramChart("Latency", "Threads"));
    int iterations = BenchmarkRunner.DEFAULT_ITERATIONS;
    int warmupIterations = BenchmarkRunner.DEFAULT_WARMUP_ITERATIONS;

    BenchmarkRunner.run(
        ShoppingCartBenchmark.class, new WarmupPrintingReporter(), 1, 3);
    BenchmarkRunner.run(
        ShoppingCartBenchmark.class, chartReporter, iterations, warmupIterations);
    
    String report = chartReporter.generateReport();
    File file = new File("shopping-cart.html");
    if (!file.exists()) file.createNewFile();
    Files.write(file.toPath(), report.getBytes("UTF-8"));
  }
}
