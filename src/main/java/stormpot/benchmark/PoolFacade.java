package stormpot.benchmark;

public interface PoolFacade {
  Object claim() throws Exception;
  void release(Object obj) throws Exception;
}
