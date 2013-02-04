package stormpot.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.BaseObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;

import nf.fr.eraasoft.pool.ObjectPool;
import nf.fr.eraasoft.pool.PoolException;
import nf.fr.eraasoft.pool.PoolSettings;
import nf.fr.eraasoft.pool.PoolableObjectBase;
import nf.fr.eraasoft.pool.impl.PoolControler;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Slot;
import stormpot.TimeExpiration;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;
import stormpot.qpool.QueuePool;

public enum PoolFactory {
  
  // IMPLEMENTATIONS
  
  queue {
    @Override
    public PoolFacade create(int size, long objTtl) {
      Config<MyPoolable> config = createStormpotConfig(size, objTtl);
      return new StormpotPoolFacade(new QueuePool<MyPoolable>(config));
    }

    @Override
    public void shutdown(PoolFacade facade) throws Exception {
      ((StormpotPoolFacade) facade).shutdown();
    }
  },
  blaze {
    @Override
    public PoolFacade create(int size, long objTtl) {
      Config<MyPoolable> config = createStormpotConfig(size, objTtl);
      return new StormpotPoolFacade(new BlazePool<MyPoolable>(config));
    }

    @Override
    public void shutdown(PoolFacade facade) throws Exception {
      ((StormpotPoolFacade) facade).shutdown();
    }
  },
  furious {
    @Override
    public PoolFacade create(int size, final long objTtl) {
      PoolSettings<MyPoolable> settings = createFuriousSettings(size, objTtl);
      return new FuriousPoolFacade(settings.pool());
    }

    @Override
    public void shutdown(PoolFacade facade) throws Exception {
      PoolControler.shutdown();
    }
  },
  stack {
    @Override
    public PoolFacade create(int size, long objTtl) {
      PoolableObjectFactory<MyPoolable> factory = new MyPoolableObjectFactory(objTtl);
      return new CommonsPoolFacade(new StackObjectPool<MyPoolable>(factory, size));
    }
  },
  generic {
    @Override
    public PoolFacade create(int size, long objTtl) {
      PoolableObjectFactory<MyPoolable> factory = new MyPoolableObjectFactory(objTtl);
      return new CommonsPoolFacade(new GenericObjectPool<MyPoolable>(factory, size));
    }
  };
  
  
  // INTERFACE
  
  public abstract PoolFacade create(int size, long objTtl);
  
  public void shutdown(PoolFacade facade) throws Exception {
  }
  
  
  // IMPLEMENTATION HELPER STUFF
  
  private static Config<MyPoolable> createStormpotConfig(int size, long objTtl) {
    Allocator<MyPoolable> allocator = new Allocator<MyPoolable>() {
      @Override
      public void deallocate(MyPoolable obj) throws Exception {}
      
      @Override
      public MyPoolable allocate(Slot slot) throws Exception {
        return new MyPoolable(slot);
      }
    };
    Config<MyPoolable> config = new Config<MyPoolable>();
    config.setAllocator(allocator);
    config.setSize(size);
    config.setExpiration(new TimeExpiration(objTtl, TimeUnit.MILLISECONDS));
    return config;
  }
  
  private static class StormpotPoolFacade implements PoolFacade {
    private final LifecycledPool<MyPoolable> pool;

    public StormpotPoolFacade(LifecycledPool<MyPoolable> pool) {
      this.pool = pool;
    }

    public void shutdown() throws Exception {
      if (!pool.shutdown().await(new Timeout(10, TimeUnit.SECONDS))) {
        throw new IllegalStateException("Shutdown timeout.");
      }
    }

    @Override
    public Object claim() throws Exception {
      return pool.claim(new Timeout(1, TimeUnit.SECONDS));
    }

    @Override
    public void release(Object obj) {
      ((MyPoolable) obj).release();
    }
  }
  
  private static PoolSettings<MyPoolable> createFuriousSettings(int size, final long objTtl) {
    PoolableObjectBase<MyPoolable> allocator = new PoolableObjectBase<MyPoolable>() {
      @Override
      public MyPoolable make() throws PoolException {
        return new MyPoolable(null);
      }
      
      @Override
      public void activate(MyPoolable obj) throws PoolException {}

      @Override
      public boolean validate(MyPoolable t) {
        return !t.olderThan(objTtl);
      }
    };
    PoolSettings<MyPoolable> settings = new PoolSettings<MyPoolable>(allocator);
    return settings.min(size).max(size);
  }
  
  private static class FuriousPoolFacade implements PoolFacade {
    private final ObjectPool<MyPoolable> pool;

    public FuriousPoolFacade(ObjectPool<MyPoolable> pool) {
      this.pool = pool;
    }

    @Override
    public Object claim() throws Exception {
      return pool.getObj();
    }

    @Override
    public void release(Object obj) throws Exception {
      pool.returnObj((MyPoolable) obj);
    }
  }
  
  private static class MyPoolableObjectFactory implements PoolableObjectFactory<MyPoolable> {
    private final long maxTtlMillis;
    
    public MyPoolableObjectFactory(long maxTtlMillis) {
      this.maxTtlMillis = maxTtlMillis;
    }

    @Override
    public void activateObject(MyPoolable obj) throws Exception {
    }

    @Override
    public void destroyObject(MyPoolable obj) throws Exception {
    }

    @Override
    public MyPoolable makeObject() throws Exception {
      return new MyPoolable(null);
    }

    @Override
    public void passivateObject(MyPoolable obj) throws Exception {
    }

    @Override
    public boolean validateObject(MyPoolable obj) {
      return !obj.olderThan(maxTtlMillis);
    }
  }
  
  private static class CommonsPoolFacade implements PoolFacade {
    private final BaseObjectPool<MyPoolable> objectPool;

    public CommonsPoolFacade(BaseObjectPool<MyPoolable> objectPool) {
      this.objectPool = objectPool;
    }

    @Override
    public Object claim() throws Exception {
      return objectPool.borrowObject();
    }

    @Override
    public void release(Object obj) throws Exception {
      objectPool.returnObject((MyPoolable) obj);
    }
  }
}
