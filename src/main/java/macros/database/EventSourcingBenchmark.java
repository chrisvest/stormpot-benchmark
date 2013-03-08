package macros.database;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

public class EventSourcingBenchmark extends DatabaseBenchmark {
  private final Random randomSource;
  
  public EventSourcingBenchmark(
      @Param(value = "fixture", defaults = "stormpot") Fixture fixture,
      @Param(value = "threads", defaults = "4") int threads,
      @Param(value = "poolSize", defaults = "10") int poolSize,
      @Param(value = "iterations", defaults = "200") int iterations,
      @Param(value = "database", defaults = "mysql") Database database) {
    this.fixture = fixture;
    this.threads = threads;
    this.poolSize = poolSize;
    this.iterations = iterations;
    this.database = database;
    randomSource = new Random();
  }

  @Override
  protected void runBenchmark(Recorder recorder) throws Exception {
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
  
  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(EventSourcingBenchmark.class);
  }
}
