package macros.database.hibernate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

@Entity
@Table(name = "event")
@NamedQueries({
  @NamedQuery(
      name = "getRecent",
      query = "select e from EventEntity e where e.entityId = :entityId " +
              "order by e.id desc")
})
public class EventEntity {
  public static final int TYPE_SNAPSHOT = 1;
  public static final int TYPE_UPDATE = 2;
  public static final int TYPE_DELETE = 3;
  
  private Long id;
  
  private int entityId;
  
  private int type;
  
  private String payload;

  @Id @GeneratedValue
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  @Column(name = "entity_id", nullable = false)
  public int getEntityId() {
    return entityId;
  }

  public void setEntityId(int entityId) {
    this.entityId = entityId;
  }
  
  @Column(name = "type", nullable = false)
  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Column(name = "payload", nullable = false, length = 4000)
  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "EventEntity [id=" + id + ", entityId=" + entityId + ", type="
        + type + ", payload=" + payload + "]";
  }
}
