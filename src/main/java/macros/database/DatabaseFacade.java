package macros.database;

interface DatabaseFacade {

  void close() throws Exception;

  void insertRow(String txt, int x) throws Exception;
  
}