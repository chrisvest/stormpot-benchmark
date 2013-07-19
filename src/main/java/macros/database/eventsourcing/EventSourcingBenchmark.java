package macros.database.eventsourcing;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import macros.database.Database;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.PrintingReporter;
import org.benchkit.Recorder;
import org.benchkit.Reporter;

public class EventSourcingBenchmark implements Benchmark {
  private final Random randomSource;
  private Fixture fixture;
  private int threads;
  private int poolSize;
  private int iterations;
  private Database database;
  private DataSource dataSource;
  private ExecutorService executor;
  private DatabaseFacade facade;
  
  public EventSourcingBenchmark(
      @Param(value = "fixture", defaults = "hibernate,stormpot") Fixture fixture,
      @Param(value = "threads", defaults = "1") int threads,
      @Param(value = "poolSize", defaults = "10") int poolSize,
      @Param(value = "iterations", defaults = "1000") int iterations,
      @Param(value = "database", defaults = "h2") Database database) {
    this.fixture = fixture;
    this.threads = threads;
    this.poolSize = poolSize;
    this.iterations = iterations;
    this.database = database;
    randomSource = new Random();
  }

  private void runBenchmark(Recorder recorder) throws Exception {
    XorShiftRandom prng = new XorShiftRandom(randomSource.nextInt());
    
    long begin = recorder.begin();
    Thread currentThread = Thread.currentThread();
    String threadName = currentThread.getName();
    for (int i = 0; i < iterations; i++) {
      int entityId = (prng.nextInt() & 64) + ((int) currentThread.getId() * 64);
      String name = threadName + "-" + i;
      int age = Math.abs(prng.nextInt()) % 100;
      
      Properties nameChange = new Properties();
      nameChange.setProperty("name", name);
      Properties ageChange = new Properties();
      ageChange.setProperty("age", String.valueOf(age));
      
      for (int x = 0; x < 3; x++) {
        if (runTransaction(entityId, nameChange, ageChange)) {
          break;
        }
        System.err.println("transaction " + x + " failed.");
      }
      
      begin = recorder.record(begin);
    }
  }

  private boolean runTransaction(int entityId, Properties nameChange,
      Properties ageChange) throws AssertionError, Exception {
    Object tx = facade.begin();
    try {
      importantWork(entityId, nameChange, ageChange, tx);
      facade.commit(tx);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    facade.rollback(tx);
    return false;
  }

  private void importantWork(int entityId, Properties nameChange,
      Properties ageChange, Object tx) throws AssertionError, Exception {
    facade.updateEntity(tx, entityId, nameChange);
    facade.updateEntity(tx, entityId, ageChange);
    
    List<Properties> updates = facade.getRecentUpdates(tx, entityId, 2);
    if (!updates.get(0).equals(nameChange)) {
      throw new AssertionError("Expected a name change: " +
          nameChange + " but was: " + updates.get(0));
    }
    if (!updates.get(1).equals(ageChange)) {
      throw new AssertionError("Expected an age change: " +
          ageChange + " but was: " + updates.get(1));
    }
    
    Properties expectedEntity = new Properties();
    expectedEntity.putAll(nameChange);
    expectedEntity.putAll(ageChange);
    Properties actualEntity = facade.getEntity(tx, entityId);
    if (!actualEntity.equals(expectedEntity)) {
      throw new AssertionError("Expected entity: " + expectedEntity +
          " but was: " + actualEntity);
    }
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
      database.tryUpdate(connection, "truncate table log");
      database.tryUpdate(connection, "truncate table event");
    } finally {
      connection.close();
    }
  }

  private Callable<Recorder> createWorker(final CountDownLatch startLatch, final Recorder recorder) {
    return new Callable<Recorder>() {
      @Override
      public Recorder call() throws Exception {
        startLatch.await();
        runBenchmark(recorder);
        return recorder;
      }
    };
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
    Reporter reporter = new PrintingReporter();
    // reduce the iteration counts, because this one is a bit slow
    BenchmarkRunner.run(EventSourcingBenchmark.class, reporter, 3, 8);
  }
}
