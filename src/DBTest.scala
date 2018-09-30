import java.sql.{Connection, DriverManager, ResultSet}


/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-25 上午10:50
  *
  */
object DBTest {
  def main(args: Array[String]) {
    val conn_str = "jdbc:mysql://test01.mysql.db.thejoyrun.com:3310/yp_ecommerce?user=ypecommerce&password=ypecommerce&Unicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    println(conn_str)
    val conn = connect(conn_str)
    val statement =conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    // Execute Query
    val rs = statement.executeQuery("select * from ec_refund")
    // Iterate Over ResultSet
    while (rs.next) {
      // 返回行号
      // println(rs.getRow)
      val name = rs.getString("order_no")
      println(name)
    }
    closeConn(conn)
  }

  def connect(conn_str: String): Connection = {
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    return  DriverManager.getConnection(conn_str)
  }

  def closeConn(conn:Connection): Unit ={
    conn.close()
  }




}
