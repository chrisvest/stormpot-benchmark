package macros.database;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

public class DatabaseBenchmark implements Benchmark {
  
  private Fixture fixture;
  private int threads;
  private int poolSize;
  private int iterations;
  private Database database;
  private DataSource dataSource;
  private ExecutorService executor;
  private DatabaseFacade facade;

  public DatabaseBenchmark(
      @Param(value = "fixture", defaults = "stormpot,hibernate") Fixture fixture,
      @Param(value = "threads", defaults = "4") int threads,
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
  public void setUp() throws Exception {
    dataSource = database.createDataSource();
    executor = Executors.newCachedThreadPool();
    database.createDatabase(dataSource);
    facade = fixture.init(database, poolSize);
  }

  @Override
  public void runSession(Recorder mainRecorder) throws Exception {
    clearDatabase();
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Recorder>> recorders = new ArrayList<Future<Recorder>>();
    for (int i = 0; i < threads; i++) {
      Recorder recorder = mainRecorder.createBlankCopy();
      Future<Recorder> futureRecorder =
          executor.submit(createWorker(startLatch, recorder));
      recorders.add(futureRecorder);
    }
    Thread.yield();
    startLatch.countDown();
    for (Future<Recorder> futureRecorder : recorders) {
      mainRecorder.add(futureRecorder.get());
    }
  }

  private void clearDatabase() throws Exception {
    Connection connection = dataSource.getConnection();
    try {
      Statement truncate = connection.createStatement();
      truncate.executeUpdate("truncate table event");
      truncate.close();
    } finally {
      connection.close();
    }
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
  
  private void runBenchmark(Recorder recorder) throws Exception {
    long begin = recorder.begin();
    String name = Thread.currentThread().getName();
    for (int i = 0; i < iterations; i++) {
      facade.insertRow(name, i);
      begin = recorder.record(begin);
    }
  }

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Executor did not shut down fast enough");
    }
    facade.close();
    database.shutdownAll();
  }

  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(DatabaseBenchmark.class);
  }
}
