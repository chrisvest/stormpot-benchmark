package stormpot.benchmark;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
    Param[] paramAnnotations = getParamAnnotations(ctor);
    if (paramTypes.length > 0) {
      Seq<Seq<Object>> argss = buildArguments(paramTypes, paramAnnotations);
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
      Param[] paramAnnotations) {
    /*
     * Given a parameter list of (x,y,z), this function will compute a Seq
     * of all the possible arguments to these parameters. A bit like this:
     * 
     *  [(x1, y1, z1),
     *   (x1, y2, z1),
     *   (x1, y3, z1),
     *   (x2, y1, z1),
     *   (x2, y2, z1),
     *   (x2, y3, z1)]
     * 
     * The input is a list of argument types, eg. [X, Y, Z]. We arrive at our
     * result by first computing the list of possible values for each argument
     * type, like this:
     * 
     *  [[x1, x2],
     *   [y1, y2, y3],
     *   [z1]]
     * 
     * Then we transform it into a sequence of permutations of argument lists,
     * like the one described in the first figure... Somehow.
     */
    Seq<Seq<Object>> valueSets = buildValues(paramTypes, paramAnnotations, 0);
    Seq<Seq<Object>> argTree = new Seq<Seq<Object>>(null, null);
    for (Seq<Object> argValues : valueSets) {
      Seq<Seq<Object>> permutations = null;
      for (Object arg : argValues) for (Seq<Object> argBranch : argTree) {
        Seq<Object> argRoot = new Seq<Object>(arg, argBranch);
        permutations = new Seq<Seq<Object>>(argRoot, permutations);
      }
      argTree = permutations;
    }
    return argTree;
  }

  private static Seq<Seq<Object>> buildValues(
      Class<?>[] types,
      Param[] annotations,
      int i) {
    if (i >= types.length) {
      return null;
    }
    Class<?> type = types[i];
    Param annotation = annotations[i];
    List<String> strVals = select(annotation.value(), annotation.defaults());
    List<Object> vals = new ArrayList<Object>();
    for (String valName : strVals) {
      vals.add(toValue(valName, type));
    }
    return new Seq<Seq<Object>>(
        new Seq<Object>(vals),
        buildValues(types, annotations, i + 1));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object toValue(String strVal, Class<?> type) {
    if (type == String.class) {
      return strVal;
    }
    if (strVal == null || strVal.isEmpty()) {
      String typeName = type.getName();
      throw new IllegalArgumentException(
          "Cannot turn null or empty string into value of type " + typeName);
    }
    if (type.isEnum()) {
      return Enum.valueOf((Class) type, strVal);
    }
    if (type == Long.TYPE || type == Long.class) {
      return Long.parseLong(strVal);
    }
    if (type == Integer.TYPE || type == Integer.class) {
      return Integer.parseInt(strVal);
    }
    if (type == Short.TYPE || type == Short.class) {
      return Short.parseShort(strVal);
    }
    if (type == Byte.TYPE || type == Byte.class) {
      return Byte.parseByte(strVal);
    }
    if (type == Character.TYPE || type == Character.class) {
      return strVal.charAt(0);
    }
    if (type == Double.TYPE || type == Double.class) {
      return Double.parseDouble(strVal);
    }
    if (type == Float.TYPE || type == Float.class) {
      return Float.parseFloat(strVal);
    }
    throw new IllegalArgumentException(
        "Don't know how to create value '" + strVal +
        "' of type " + type.getName());
  }

  private static Param[] getParamAnnotations(Constructor<?> ctor) {
    Annotation[][] parameterAnnotations = ctor.getParameterAnnotations();
    Param[] annotations = new Param[parameterAnnotations.length];
    for (int i = 0; i < annotations.length; i++) {
      for (Annotation annotation : parameterAnnotations[i]) {
        if (annotation.annotationType() == Param.class) {
          annotations[i] = (Param) annotation;
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

  private static class Seq<T> implements Iterable<T> {
    final T head;
    final Seq<T> tail;
    
    Seq(T head, Seq<T> tail) {
      this.head = head;
      this.tail = tail;
    }
    
    Seq(List<T> list) {
      if (list.isEmpty()) {
        head = null;
        tail = null;
      } else {
        head = list.get(0);
        tail = list.size() > 1? new Seq<T>(list, 1) : null;
      }
    }
    
    Seq(List<T> list, int index) {
      head = list.get(index);
      index++;
      if (index == list.size()) {
        tail = null;
      } else {
        tail = new Seq<T>(list, index);
      }
    }
    
    <C extends Collection<T>> C into(C coll) {
      if (head != null) {
        coll.add(head);
      }
      if (tail != null) {
        tail.into(coll);
      }
      return coll;
    }

    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        Seq<T> curr = Seq.this;
        @Override
        public boolean hasNext() {
          return curr != null;
        }

        @Override
        public T next() {
          T obj = curr.head;
          curr = curr.tail;
          return obj;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public String toString() {
      return "Seq [head=" + head + ", tail=" + tail + "]";
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
