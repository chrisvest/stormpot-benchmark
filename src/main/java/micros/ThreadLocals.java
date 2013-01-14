package micros;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;

public class ThreadLocals extends SimpleBenchmark {
  @Param({"0", "10", "100", "10000", "1000000"})
  int priorThreadLocalCount;
  private ThreadLocal<Integer>[] priorThreadLocals;
  
  private static class Ref {
    Integer obj;
  }
  
  private ThreadLocal<Integer> threadLocal;
  private ThreadLocal<Ref> threadLocalRef;
  private Integer obj;
  
  @SuppressWarnings("unchecked")
  @Override
  public void setUp() {
    obj = new Integer(1);
    
    priorThreadLocals = new ThreadLocal[priorThreadLocalCount];
    for (int i = 0; i < priorThreadLocalCount; i++) {
      priorThreadLocals[i] = new ThreadLocal<Integer>();
      priorThreadLocals[i].set(obj);
    }
    
    threadLocal = new ThreadLocal<Integer>();
    threadLocalRef = new ThreadLocal<Ref>() {
      @Override
      protected Ref initialValue() {
        return new Ref();
      }
    };
  }

  public void timeThreadLocalWrite(int reps) {
    ThreadLocal<Integer> tl = threadLocal;
    Integer x = obj;
    for (int i = 0; i < reps; i++) {
      tl.set(x);
    }
  }
  
  public int timeThreadLocalRead(int reps) {
    ThreadLocal<Integer> tl = threadLocal;
    tl.set(obj);
    int sum = 12344324;
    for (int i = 0; i < reps; i++) {
      sum ^= tl.get().intValue();
    }
    return sum;
  }
  
  public int timeThreadLocalReadWrite(int reps) {
    ThreadLocal<Integer> tl = threadLocal;
    tl.set(obj);
    int sum = 93487529;
    for (int i = 0; i < reps; i++) {
      Integer x = tl.get();
      sum ^= x.intValue();
      tl.set(x);
    }
    return sum;
  }
  
  public int timeThreadLocalRefReadWrite(int reps) {
    ThreadLocal<Ref> tl = threadLocalRef;
    Integer xobj = obj;
    tl.get().obj = xobj;
    int sum = 93487529;
    for (int i = 0; i < reps; i++) {
      Ref x = tl.get();
      sum ^= x.obj.intValue();
      x.obj = xobj;
    }
    return sum;
  }
  
  public static void main(String[] args) {
    Runner.main(ThreadLocals.class, args);
  }
}
