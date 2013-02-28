package macros.database;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public enum Database {
  mysql("jdbc:mysql://localhost:3306/test?autoCommit=true") {
    @Override
    public DataSource createDataSource() {
      MysqlDataSource ds = new MysqlDataSource();
      ds.setCachePreparedStatements(true);
      ds.setUrl(getConnectionString());
      ds.setUser("root"); // this is perfectly acceptable, right??
      ds.setPassword("");
      return ds;
    }

    @Override
    public void createDatabase(DataSource dataSource) throws Exception {
      Connection connection = dataSource.getConnection();
      try {
        Statement drop = connection.createStatement();
        drop.executeUpdate("drop table if exists event");
        drop.close();
        Statement create = connection.createStatement();
        create.executeUpdate("create table event (" +
            "id int auto_increment primary key, " +
            "txt varchar(255), x int) " +
            "engine = MyISAM");
        create.close();
      } finally {
        connection.close();
      }
    }
  };
  
  private final String connectionString;
  
  Database(String connectionString) {
    this.connectionString = connectionString;
  }
  
  public String getConnectionString() {
    return connectionString;
  }

  public abstract DataSource createDataSource();

  public abstract void createDatabase(DataSource dataSource) throws Exception;
}
