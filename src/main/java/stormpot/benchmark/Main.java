package stormpot.benchmark;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.benchkit.Benchmark;
import org.benchkit.BenchmarkRunner;

public class Main {

  public static void main(String[] args) throws Exception {
    List<BenchFactory> benchmarks = selectBenchFabs();
    
    for (BenchFactory benchmark : benchmarks) {
      List<BenchmarkRunner> runners = benchmark.runners();
      for (BenchmarkRunner runner : runners) {
        runner.run();
      }
    }
  }

  private static List<BenchFactory> selectBenchFabs() {
    List<BenchFactory> list = new LinkedList<BenchFactory>();
    for (String name : select("benchmarks", "singleThreaded")) {
      list.add(BenchFactory.valueOf(name));
    }
    return list;
  }

  private static List<String> select(String property, String def) {
    String propertyValue = System.getProperty(property, def);
    List<String> list = new LinkedList<String>();
    for (String element : propertyValue.split("\\s*(,\\s*)+")) {
      list.add(element);
    }
    return list;
  }
  
  private static <T extends Benchmark> List<T> createVariations(Class<T> type)
      throws Exception {
    Constructor<?>[] ctors = type.getConstructors();
    if (ctors.length != 1) {
      throw new IllegalArgumentException("The " + type + " has multiple " +
      		"public constructors, and I don't know which one to use.");
    }
    @SuppressWarnings("unchecked")
    Constructor<T> ctor = (Constructor<T>) ctors[0];
    Class<?>[] paramTypes = ctor.getParameterTypes();
    Annotation[] paramAnnotations = getParamAnnotations(ctor);
    if (paramTypes.length > 0) {
      Seq<Seq<Object>> argss = buildArguments(paramTypes, paramAnnotations, 0);
      Seq<T> benchmarks = construct(ctor, argss);
      return benchmarks.into(new LinkedList<T>());
    }
    
    List<T> list = new LinkedList<T>();
    list.add(ctor.newInstance());
    return list;
  }
  
  @SuppressWarnings("cast")
  private static <T> Seq<T> construct(Constructor<T> ctor, Seq<Seq<Object>> argss)
      throws Exception {
    if (argss == null || argss.head == null) {
      return null;
    }
    List<Object> argList = argss.head.into(new ArrayList<Object>());
    Object[] args = (Object[]) argList.toArray(new Object[argList.size()]);
    return new Seq<T>(ctor.newInstance(args), construct(ctor, argss.tail));
  }
  
  private static Seq<Seq<Object>> buildArguments(
      Class<?>[] paramTypes,
      Annotation[] paramAnnotations,
      int index) {
    // TODO Auto-generated method stub
    return null;
  }

  private static Annotation[] getParamAnnotations(Constructor<?> ctor) {
    Annotation[][] parameterAnnotations = ctor.getParameterAnnotations();
    Annotation[] annotations = new Annotation[parameterAnnotations.length];
    for (int i = 0; i < annotations.length; i++) {
      for (Annotation annotation : parameterAnnotations[i]) {
        if (annotation.annotationType() == Param.class) {
          annotations[i] = annotation;
          break;
        }
      }
      if (annotations[i] == null) {
        throw new IllegalArgumentException("Constructor `" + ctor +
            "` parameter " + i + " has no @" + Param.class.getName() +
            " annotation.");
      }
    }
    return annotations;
  }

  private static class Seq<T> {
    final T head;
    final Seq<T> tail;
    
    Seq(T head, Seq<T> tail) {
      this.head = head;
      this.tail = tail;
    }
    
//    Seq(List<T> list) {
//      if (list.isEmpty()) {
//        head = null;
//        tail = null;
//      } else {
//        head = list.get(0);
//        tail = new Seq<T>(list, 1);
//      }
//    }
    
//    Seq(List<T> list, int index) {
//      head = list.get(index);
//      index++;
//      if (index == list.size()) {
//        tail = null;
//      } else {
//        tail = new Seq<T>(list, index);
//      }
//    }
    
    <C extends Collection<T>> C into(C coll) {
      if (head != null) {
        coll.add(head);
      }
      if (tail != null) {
        tail.into(coll);
      }
      return coll;
    }
  }
  
  private static enum BenchFactory {
    messagePassing(MessagePassingBenchmark.class),
    singleThreaded(SingleThreadedBenchmark.class),
    multiThreaded(MultiThreadedBenchmark.class);
    
    private Class<? extends Benchmark> benchmarkType;
    
    BenchFactory(Class<? extends Benchmark> cls) {
      this.benchmarkType = cls;
    }

    public List<BenchmarkRunner> runners() throws Exception {
      List<BenchmarkRunner> runners = new LinkedList<BenchmarkRunner>();
      for (Benchmark benchmark : createVariations(benchmarkType)) {
        runners.add(new BenchmarkRunner(benchmark));
      }
      return runners;
    }
  }
}
