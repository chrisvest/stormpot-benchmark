package macros.database.stormpot;

import java.sql.Connection;
import java.sql.PreparedStatement;

import stormpot.Poolable;
import stormpot.Slot;

class Dao implements Poolable {
  private final Slot slot;
  private final Connection connection;
  private final PreparedStatement insertRowStatement;
  
  public Dao(Slot slot, Connection connection) throws Exception {
    this.slot = slot;
    this.connection = connection;
    insertRowStatement = connection.prepareStatement(
        "insert into log (txt, x) values (?, ?)");
  }

  @Override
  public void release() {
    slot.release(this);
  }

  public void close() throws Exception {
    connection.close();
  }

  public void insertRow(String txt, int x) throws Exception {
    insertRowStatement.setString(1, txt);
    insertRowStatement.setInt(2, x);
    insertRowStatement.execute();
  }
}
