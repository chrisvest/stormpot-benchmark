package macros;

import javax.sql.DataSource;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;
import org.benchkit.Param;
import org.benchkit.Recorder;

public class DatabaseBenchmark implements Benchmark {
  
  static DataSource createDataSource() {
    return null;
  }
  
  private static enum Fixture {
    stormpot {
      
    },
    hibernate {
      
    };
    
    
  }
  
  public DatabaseBenchmark(
      @Param(value = "fixture", defaults = "stormpot") Fixture fixture) {
    
  }

  @Override
  public void setUp() throws Exception {
  }

  @Override
  public void runSession(Recorder mainRecorder) throws Exception {
  }

  @Override
  public void tearDown() throws Exception {
  }

  public static void main(String[] args) throws Exception {
    BenchmarkRunner.run(DatabaseBenchmark.class);
  }
}
