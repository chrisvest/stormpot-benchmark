package macros.database.eventsourcing;

/**
 * Based on George Marsaglias XorShift:
 * http://www.jstatsoft.org/v08/i14
 * 
 * This class is NOT thread-safe!
 */
public final class XorShiftRandom {
  private int seed;
  
  public XorShiftRandom(int seed) {
    this.seed = seed;
  }
  
  public int nextInt() {
    return seed = xorShift(seed);
  }
  
  private static int xorShift(int seed) {
    // Marsaglias XorShift algorithm:
    seed ^= (seed << 6);
    seed ^= (seed >>> 21);
    return seed ^ (seed << 7);
  }
}
