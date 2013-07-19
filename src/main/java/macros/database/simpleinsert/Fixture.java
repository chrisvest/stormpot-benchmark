package macros.database.simpleinsert;

import macros.database.Database;

enum Fixture {
  stormpot {
    @Override
    public Inserter init(Database database, int poolSize) {
      return new StormpotInserter(database, poolSize);
    }
  },
  hibernate {
    @Override
    public Inserter init(Database database, int poolSize) {
      return new HibernateInserter(database, poolSize);
    }
  };
  
  public abstract Inserter init(Database database, int poolSize);
}