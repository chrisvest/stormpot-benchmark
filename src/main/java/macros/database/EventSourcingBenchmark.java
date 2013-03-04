package macros.database;

import java.util.Random;

import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

public class EventSourcingBenchmark extends DatabaseBenchmark {
  private final Random randomSource;
  
  public EventSourcingBenchmark(
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
    randomSource = new Random();
  }

  @Override
  protected void runBenchmark(Recorder recorder) throws Exception {
    XorShiftRandom prng = new XorShiftRandom(randomSource.nextInt());
    
    long begin = recorder.begin();
    String name = Thread.currentThread().getName();
    for (int i = 0; i < iterations; i++) {
      int entityId = prng.nextInt() & 255;
      
      
      
      facade.insertLogRow(name, i);
      begin = recorder.record(begin);
    }
  }
  
  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(EventSourcingBenchmark.class);
  }
}
