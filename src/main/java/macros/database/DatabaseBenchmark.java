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
import org.benchkit.Recorder;

public abstract class DatabaseBenchmark implements Benchmark {

  protected Fixture fixture;
  protected int threads;
  protected int poolSize;
  protected int iterations;
  protected Database database;
  protected DataSource dataSource;
  protected ExecutorService executor;
  protected DatabaseFacade facade;

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
      truncate.executeUpdate("truncate table log");
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
  
  protected abstract void runBenchmark(Recorder recorder) throws Exception;

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Executor did not shut down fast enough");
    }
    facade.close();
    database.shutdownAll();
  }
}
