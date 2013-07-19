package macros.database.simpleinsert;

public interface Inserter {

  void insertLogRow(String txt, int x) throws Exception;

  void close() throws Exception;

}
