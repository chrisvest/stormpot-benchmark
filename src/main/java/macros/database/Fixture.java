package macros.database;

import javax.sql.DataSource;

enum Fixture {
  stormpot {
    @Override
    public DatabaseFacade init(DataSource dataSource, int poolSize) {
      return new StormpotDatabaseFacade(dataSource, poolSize);
    }
  },
  hibernate {
    @Override
    public DatabaseFacade init(DataSource dataSource, int poolSize) {
      return new HibernateDatabaseFacade(dataSource, poolSize);
    }
  };
  
  public abstract DatabaseFacade init(DataSource dataSource, int poolSize);
}