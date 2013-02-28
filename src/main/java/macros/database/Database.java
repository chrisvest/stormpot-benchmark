package macros.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedDataSource40;
import org.hsqldb.jdbc.JDBCDataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public enum Database {
  mysql(
      "jdbc:mysql://localhost:3306/test?autoCommit=true",
      "org.hibernate.dialect.MySQL5Dialect",
      "com.mysql.jdbc.Driver",
      "root",
      "") {
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
      Connection con = dataSource.getConnection();
      try {
        update(con, "drop table if exists event");
        update(con, "create table event (" +
            "id int auto_increment primary key, " +
            "txt varchar(255), x int) " +
            "engine = MyISAM");
      } finally {
        con.close();
      }
    }
  },
  derby(
      "jdbc:derby:derbyDB;create=true",
      "org.hibernate.dialect.DerbyTenSevenDialect",
      "org.apache.derby.jdbc.EmbeddedDriver",
      "",
      "") {
    @Override
    public DataSource createDataSource() {
      EmbeddedDataSource40 ds = new EmbeddedDataSource40();
      ds.setDatabaseName("derbyDB");
      ds.setCreateDatabase("create");
      return ds;
    }

    @Override
    public void createDatabase(DataSource dataSource) throws Exception {
      Connection con = dataSource.getConnection();
      try {
        try {
          update(con, "drop table event");
        } catch (Exception _) {} // ignored in the absence of 'if exists'
        update(con, "create table event (" +
            "id int generated always as identity," +
            "txt varchar(255), x int, primary key (id))");
      } finally {
        con.close();
      }
    }

    @Override
    public void shutdownAll() {
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException e) {}
    }
  },
  hsqldb(
      "jdbc:hsqldb:mem:hsqltestdb",
      "org.hibernate.dialect.HSQLDialect",
      "org.hsqldb.jdbc.JDBCDriver",
      "SA",
      "") {

    @Override
    public DataSource createDataSource() {
      JDBCDataSource ds = new JDBCDataSource();
      ds.setUrl(getConnectionString());
      ds.setUser(getUser());
      ds.setPassword(getPass());
      return ds;
    }

    @Override
    public void createDatabase(DataSource dataSource) throws Exception {
      Connection con = dataSource.getConnection();
      try {
        try {
          update(con, "drop table event");
        } catch (Exception _) {} // ignored in the absence of 'if exists'
        update(con, "create table event (" +
            "id int generated always as identity," +
            "txt varchar(255), x int, primary key (id))");
      } finally {
        con.close();
      }
    }
  };

  private final String connectionString;
  private final String dialect;
  private final String driver;
  private final String user;
  private final String pass;

  private Database(
      String connectionString,
      String dialect,
      String driver,
      String user,
      String pass) {
    this.connectionString = connectionString;
    this.dialect = dialect;
    this.driver = driver;
    this.user = user;
    this.pass = pass;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public abstract DataSource createDataSource();

  public abstract void createDatabase(DataSource dataSource) throws Exception;

  public String getDialect() {
    return dialect;
  }

  public String getDriver() {
    return driver;
  }

  public String getUser() {
    return user;
  }

  public String getPass() {
    return pass;
  }

  public void shutdownAll() {
  }

  protected void update(Connection con, String sql) throws Exception {
    Statement statement = con.createStatement();
    try {
      statement.executeUpdate(sql);
    } finally {
      statement.close();
    }
  }
}
