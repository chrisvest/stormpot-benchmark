package macros.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedDataSource40;
import org.h2.jdbcx.JdbcDataSource;
import org.hsqldb.jdbc.JDBCDataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public enum Database {
  mysql(
      "jdbc:mysql://localhost:3306/test",
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
        update(con, "drop table if exists log");
        update(con, "create table log (" +
            "id int auto_increment primary key, " +
            "txt varchar(255), x int) " +
            "engine = MyISAM");
        
        update(con, "drop table if exists event");
        update(con, "create table event (" +
        		"id int auto_increment primary key, " +
        		"entity_id int not null, " +
        		"type int not null, " + // 1 = snapshot, 2 = update, 3 = delete.
        		"payload varchar(4000) not null," +
        		"index entity_lookup using btree (entity_id asc, id desc)) " +
        		"engine = InnoDB");
      } finally {
        con.close();
      }
    }
  },
  @Deprecated // DOES NOT WORK!
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
        tryUpdate(con, "drop table log");
        update(con, "create table log (" +
            "id int generated always as identity," +
            "txt varchar(255), x int, primary key (id))");
        
        tryUpdate(con, "drop table event");
        update(con, "create table event (" +
            "id int generated always as identity, " +
            "entity_id int not null, " +
            "type int not null, " + // 1 = snapshot, 2 = update, 3 = delete.
            "payload varchar(4000) not null)");
        update(con, "create index entity_lookup on event (entity_id asc, id desc)");
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
        tryUpdate(con, "drop table log");
        update(con, "create table log (" +
            "id int generated always as identity," +
            "txt varchar(255), x int, primary key (id))");
        
        tryUpdate(con, "drop table event");
        update(con, "create table event (" +
            "id int generated always as identity, " +
            "entity_id int not null, " +
            "type int not null, " + // 1 = snapshot, 2 = update, 3 = delete.
            "payload varchar(4000) not null)");
        update(con, "create index entity_lookup on event (entity_id, id desc)");
      } finally {
        con.close();
      }
    }
  },
  h2(
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
      "org.hibernate.dialect.H2Dialect",
      "org.h2.Driver",
      "sa",
      "") {
    @Override
    public DataSource createDataSource() {
      org.h2.jdbcx.JdbcDataSource ds = new JdbcDataSource();
      ds.setURL(getConnectionString());
      ds.setUser(getUser());
      ds.setPassword(getPass());
      return ds;
    }

    @Override
    public void createDatabase(DataSource dataSource) throws Exception {
      Connection con = dataSource.getConnection();
      try {
        tryUpdate(con, "drop table log");
        update(con, "create table log (" +
            "id int generated always as identity," +
            "txt varchar(255), x int, primary key (id))");
        
        tryUpdate(con, "drop table event");
        update(con, "create table event (" +
            "id int generated always as identity, " +
            "entity_id int not null, " +
            "type int not null, " + // 1 = snapshot, 2 = update, 3 = delete.
            "payload varchar(4000) not null)");
        update(con, "create index entity_lookup on event (entity_id, id desc)");
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

  public void update(Connection con, String sql) throws Exception {
    Statement statement = con.createStatement();
    try {
      statement.executeUpdate(sql);
      con.commit();
    } catch (Exception e) {
      try {
        con.rollback();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      throw e;
    } finally {
      statement.close();
    }
  }
  
  public boolean tryUpdate(Connection con, String sql) {
    try {
      update(con, sql);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
