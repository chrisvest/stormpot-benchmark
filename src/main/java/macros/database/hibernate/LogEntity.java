package macros.database.hibernate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "log")
public class LogEntity {
  private Long id;
  
  private String txt;
  
  private int x;

  @Id @GeneratedValue
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  @Column(name ="txt", nullable = false, length = 255)
  public String getTxt() {
    return txt;
  }

  public void setTxt(String txt) {
    this.txt = txt;
  }

  @Column(name = "x", nullable = false)
  public int getX() {
    return x;
  }

  public void setX(int x) {
    this.x = x;
  }
}
