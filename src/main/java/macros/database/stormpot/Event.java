package macros.database.stormpot;

public class Event {
  public static final int TYPE_SNAPSHOT = 1;
  public static final int TYPE_UPDATE = 2;
  public static final int TYPE_DELETE = 3;
  
  public final int id;
  public final int entityId;
  public final int type;
  public final String payload;
  
  public Event(int id, int entityId, int type, String payload) {
    this.id = id;
    this.entityId = entityId;
    this.type = type;
    this.payload = payload;
  }
}
