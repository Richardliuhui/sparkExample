import scala.collection.mutable.ArrayBuffer

/***
  *伴生类
  * 伴生类可以访问伴生对象private字段
  * 伴生对象可以访问伴生对像的私有方法
  */
 class Goods {
  private def output(): Unit ={
      println("我的姓名是:"+Goods.name+",年龄是:"+Goods.age);
   }

}
/***
  * 伴生对象
  */
object Goods{
   private var name:String="aa";
   private var age:Int=10;
   def print(): Unit ={
      var goods=new Goods();
      goods.output();
   }
   def main(args: Array[String]) {
      Goods.print();
   }
}
