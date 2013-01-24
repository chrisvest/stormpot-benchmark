package stormpot.benchmark;

import java.util.concurrent.atomic.AtomicInteger;

import stormpot.Poolable;
import stormpot.Slot;

public class MyPoolable implements Poolable {
  private static final AtomicInteger COUNTER = new AtomicInteger();
  
  private Slot slot;
  private long allocated;
  private int id;
  
  public MyPoolable(Slot slot) {
    this.slot = slot;
    this.allocated = System.currentTimeMillis();
    this.id = COUNTER.incrementAndGet();
  }

  @Override
  public void release() {
    slot.release(this);
  }
  
  public boolean olderThan(long timeMillis) {
    return allocated + timeMillis < System.currentTimeMillis();
  }
  
  @Override
  public String toString() {
    return "MyPoolable#" + id;
  }
}
