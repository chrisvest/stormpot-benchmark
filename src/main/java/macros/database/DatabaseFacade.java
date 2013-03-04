package macros.database;

public interface DatabaseFacade {

  void close() throws Exception;

  void insertLogRow(String txt, int x) throws Exception;
  
}