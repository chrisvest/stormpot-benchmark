package macros.database;

import javax.sql.DataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public enum Database {
  mysql {
    @Override
    public DataSource createDataSource() {
      MysqlDataSource ds = new MysqlDataSource();
      ds.setCachePreparedStatements(true);
      ds.setUrl("jdbc:mysql://localhost:3306/test");
      ds.setUser("root"); // this is perfectly acceptable, right??
      ds.setPassword("");
      return ds;
    }
  };

  public abstract DataSource createDataSource();
}
