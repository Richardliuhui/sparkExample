import scala.collection.mutable.ArrayBuffer

object Test5 {

  def main(args: Array[String]) {
      var a=Array(1,2,3,4);
      a(0)=2;
      println(a(0));
      println(a.sum);
      var b=ArrayBuffer[Int]();
      b+=1;
      b+=(2,3,4,5);
      for (i<-b){
        println(i);
      }
  }

}
