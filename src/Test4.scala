object Test4 {

  def main(args: Array[String]) {
      print("aa");
      val k=cal(10,9);
      println("k="+k);
      val s=sum(100);
      println("s="+s);
      println(fab(3));
      sayhello();
      sayhello("jack");
      println(sums(1,2,3));

  }
  def print(name:String): Unit ={
     println(name);
  }
  def cal(num1:Int,num2:Int)={
    num1+num2;
  }
  def sum(n:Int)={
    var sum=0;
    for (i<-1 to n) sum+=i;
    sum;
  }
  def fab(n:Int):Int={
    if(n<=1) 1
    else fab(n-1)+fab(n-2);
  }

  def sayhello(name:String="liu",age:Int=18): Unit ={
      println("name="+name+",age="+age)
  }
  def sums(nums:Int*)={
    var sum=0;
    for(i<-nums){
      sum+=i;
    }
    sum;
  }

}
