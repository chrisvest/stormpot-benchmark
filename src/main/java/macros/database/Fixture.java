package macros.database;

import macros.database.hibernate.HibernateDatabaseFacade;
import macros.database.stormpot.StormpotDatabaseFacade;

enum Fixture {
  stormpot {
    @Override
    public DatabaseFacade init(Database database, int poolSize) {
      return new StormpotDatabaseFacade(database, poolSize);
    }
  },
  hibernate {
    @Override
    public DatabaseFacade init(Database database, int poolSize) {
      return new HibernateDatabaseFacade(database, poolSize);
    }
  };
  
  public abstract DatabaseFacade init(Database database, int poolSize);
}