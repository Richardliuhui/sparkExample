import java.text.SimpleDateFormat
import java.util

import scala.None
import scala.actors.Actor
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
import scala.util.control.Breaks

object Test{


  def main(args: Array[String]) {

   //val ex=new ExceptionHandler();
  //  ex.handler();
    // testArray();
   //  testArrayBuffer();
   // testMap();
   // testTuple();
   // testConstruction();
   // testAnony();
   // testAbstract();
   // testPerson();
   // testTrait();
    testTraitChain();
    // sum("aa");
    //testFun();
   // testFun1()
    // testFun2();
    //testFun3();
    //testFun4();
    // testFun5();
    //testFun6();
   // testMatch();
   // testMatch1();
   // testMatch2();
   // testMatch3();
   // testMatch4()
   // testT1();
    //testT2();
   // testImplicit();
   // testImplicit1();
   // testImplicit2();
    //testActor();
    //testActor1();

  }

  def testVariable(): Unit ={
     val a=1;
     var c="c";
     c="b";
     println("c="+c)
     var d:Int=100;
     var e,f=200;
  }
  class ApplyTest{

  }
  object ApplyTest{
     def apply: ApplyTest ={
       var test=new ApplyTest();
        test;
     }
    def hello(): Unit ={
      println("hello")
    }
  }
  def testApply: Unit ={
     val apply=ApplyTest;
     apply.hello();
  }

  def testIf(): Int ={
    val  age=30
    val c=if(age>10) "a" else "b";
    if(age>18) 1 else 0
  }
  def testWhile(): Unit ={
    var a=1
     while(true){
       a+=1;
       if(a>10){
         Breaks.break()
       }
     }
  }
  def testFunction(a:Int,b:Int):Int={
    a+b
  }
  def testFunction2(a:Int,b:Int)={
     a+b
  }
  def testFunction3(a:Int,b:Int):Int={
    return a+b;
  }
  def testFunction4(a:Int): Unit ={
    print("a="+a)
  }
  def testFunction5(name:String="jack",age:Int): Unit ={
    println("name="+name+",age="+age)
  }
  def testFunction6(num:Int *): Unit ={
    var sum=0;
    for (x<-num){
      sum+=x;
    }
    println("sum="+sum);
  }
  def testFunction7(): Unit ={
    testFunction6(1 to 6:_*);
  }

  def testArray(): Unit ={
    var array=Array(1,2,3,4);
    val array1=new Array[Int](10);
    array1(0)=1;
    array1(1)=2;
    println(array1(0)+","+array1(1))
    val array2=Array(1,3,9,4,27);
    println(array2(0)+","+array1(1))
    for (e<-array2){
      println(e);
      if(e.equals("a")){
        Breaks.break();
      }
    }
    println(array2.mkString(","));
    val array3=for(e<-array2)yield e*e;
    for (e<-array3){
      println(e);
    }
   val  array4=array3.filter(_%2!=0).map(2*_);
    for (e<-array4){
      println(e);
    }
  }
  def testArrayBuffer(): Unit ={
     val ab=new ArrayBuffer[Int](10);
     ab+=1;
     ab+=(2,3,4,5,6);
     //println(ab(1))
     for (e<-ab){
       println(e);
     }
    for (i<- 0 until ab.length){
      println(ab(i));
    }
  }
  def testMap(): Unit ={
    //不可变map
    var ages=Map("aa"->30,"bb"->40);
    println(ages("aa"))
    //可变
    var map=scala.collection.mutable.Map("aa"->20,"cc"->44)
    map("aa")=30;
    println(map("aa"));
    for((key,value)<-map){
       println("key="+key+",value="+value)
    }
    var map1=new scala.collection.mutable.HashMap[String,Int]();
     map1.put("aa",10);
     map1.put("kk",12);
    for((key,value)<-map1){
      println("key="+key+",value="+value)
    }
    var map3=new mutable.LinkedHashMap[String,Int]();
    map3.put("aa",11);
    map3.put("cc",13);
    map3.foreach(a=>println(a._1+","+a._2))
  }

  def testTuple(): Unit ={
     val a=(10,"aa","addd","aa");
     println(a._1+","+a._2+","+a._3);
  }
  class HelloWorld{
    private var name="jack";
    def sayhello(): Unit ={
      println("hello,"+name)
    }
    def getName=name;
  }
  class Badge{
    var badgeName="勋章";
    val remark="勋章说明";
    private var url="www.baidu.com";
  }
  class Topic(topicId:Int,topicName:String){
    def print(): Unit ={
      println("topicId="+topicId+",topicName="+topicName)
    }
  }
  class Wallet{
    private var uid:Int=0;
    private var mobile:String="";
    def this(uid:Int){
      this()
      this.uid=uid;
    }
    def this(uid:Int,mobile:String){
      this(uid);
      this.mobile=mobile;
    }

  }
  object Person{
    private var eyeNum=2;
    println("this person object!");
    def getEyeNum()=eyeNum;
  }
  def testPerson(): Unit ={
     println("eysNum="+Person.getEyeNum());
     println("eysNum="+Person.getEyeNum());
  }


  def testAnony(): Unit ={
     var topic=new Topic(1,"22"
     ) {
       override def print(): Unit = {
          println("hi");
       }
     }.print()
  }
  class Fish{
     var eyeNum=4;
     var color:String="white";
     def swim(): Unit ={
        println("我会游戏");
     }
  }
  class GoldFish extends Fish{
    override def swim(): Unit = {
      println("我是金鱼,我会游戏");
    }
  }
  def testAbstract(): Unit ={
     var job=new YpJob();
     job.execute();
  }
  trait SwimTrait{
    def swim();
  }
  trait PlayTrait{
    def play();
  }
  trait Logger{
    def print(msg:String)=println(msg)
  }
  class TraitImpl extends SwimTrait with PlayTrait with Logger{
    override def swim(): Unit = print("我会游泳")

    override def play(): Unit = print("play");
  }
  def testTraitChain(): Unit ={
     val persionHandler=new PersionHandler("KK");
     persionHandler.sayHello();
  }

  /***
    * return
    *
    * @param name
    */
  def sum(name:String): Unit ={
     def print2(n:String): String ={
        return n;
     }
    val s= print2(name);
    println(s);
  }

  /**
    *把函数赋值给一个变量
    */
  def testFun(): Unit ={
      val print3=sum _;
      print3("aa");
  }

  /**
    *匿名函数
    */
  def testFun1(): Unit ={
    val b=(name:String)=>print(name);
    b("kk")
  }

  /**
    * 高阶函数
    *
    */
  def testFun2(): Unit ={
      val b=(name:String)=>println(name);
      def greet(fun:(String)=>Unit,name:String){fun(name)};
      greet(b,"kk");
      greet((name:String)=>println(name),"aa")
      greet((name)=>println(name),"aa")
      greet(name=>println(name),"aa")
  }

  /***
    * 高阶函数
    */
  def testFun3(): Unit ={
    def triple(fun:(Int)=>Int)={fun(3)};
    print(triple((num:Int)=>3*num));
    print(triple(num=>3*num));
    print(triple(3*_));
  }

  def testFun4(): Unit ={
     var a=Array(1,2,3,4).map(3*_);
     for(x<-a){
        println(x)
     }
     (1 to 9).map("*"*_).foreach(println(_))
  }
  def testFun5(): Unit ={
    Array(1,2,13,9).sortWith(_<_).foreach(println(_));
  }

  /***
    * currying函数
    */
  def testFun6(): Unit ={
     def sum1(a:Int,b:Int)=a+b;
     println(sum1(10,20));
     def sum2(a:Int)=(b:Int)=>a+b;
     print(sum2(2)(3));
  }

  def testMatch(): Unit ={
     def judgeGrade(grade:String): Unit ={
        grade match {
          case "A"=>println("excellent");
          case "B"=>println("good");
          case  _=>println("you need work harder")
        }
     }
    judgeGrade("D");
  }
  def testMatch1(): Unit ={
    def judgeGrade(grade:String,name:String): Unit ={
      grade match {
        case "A"=>println("excellent");
        case "B"=>println("good");
        case  _ if name == "aa" =>println(name+" you are a good boy")
        case  _=>println("you need work harder")
      }
    }
    judgeGrade("D","aa");
  }
  def testMatch2(): Unit ={
    def judgeGrade(arry:Array[String]): Unit ={
      arry match {
        case Array("liu")=>println("hi liu");
        case Array(girl1,girl2,girl3)=>println("hi,"+girl1+","+girl2+","+girl3);
        case  _=>println("hey")
      }
    }
    judgeGrade(Array("girl2","girls2","girls3","aa"));
  }
  def testMatch3(): Unit ={
    def judgeGrade(p:Person): Unit ={
      p match {
        case Teacher(name,subject)=>println("teacher");
        case Student(name,classroom)=>println("student");
        case  _=>println("hey")
      }
    }
    judgeGrade(new Teacher("KK","英语"));
  }
  class Person;
  case class Teacher(name:String,subject:String) extends Person;
  case class Student(name:String,classroom:String) extends Person;

  def testMatch4(): Unit ={
    val grades=Map("leo"->"A","jack"->"B","jen"->"C")
    def getGrade(name:String){
      val grade=grades.get(name);
      grade match {
        case Some(grade) => println("your grade is "+grade)
        case  None => println("sorry")
      }
    }
    getGrade("leo1")
  }
  def testT1(): Unit ={
    var stu=new Info[String]("123");
    println(stu.getScholld("aa"));
  }
  class Info[T](val localId:T){
     def getScholld(huk:T)="S-"+huk+"_"+localId
  }

  class Zoo(val name:String){
    def sayHello=println("hello,"+name);
    def markZoos(p:Zoo): Unit ={
        sayHello
        p.sayHello
    }
  }
  class Dog(name:String)extends Zoo(name)
  /**
    * 上边界 泛型限定子类
    *
    * @param p1
    * @param p2
    * @tparam T
    */
  class Party[T<:Zoo](p1:T,p2:T){
     def play=p1.markZoos(p2);
  }
  def testT2(): Unit ={
    var dog1=new Dog("jack");
    var dog2=new Dog("pack");
    new Party[Dog](dog1,dog2).play
  }
  class SpecialPerson(val name:String);
  class Young(val name:String);
  class Older(val name:String);
  implicit def object2SpecialPerson(obj:Object):SpecialPerson={
    if(obj.isInstanceOf[Young]){
      val stu=obj.asInstanceOf[Young];
      new SpecialPerson(stu.name);
    }else if(obj.isInstanceOf[Older]){
      val older=obj.asInstanceOf[Older];
      new SpecialPerson(older.name);
    }else null
  }
   var ticketNumer:Int=0
  def buySpecialTicket(p:SpecialPerson)={
    ticketNumer += 1;
     "T"+ticketNumer+p.name
  }

  /**
    *隐式转换,没有这个类
    */
  def testImplicit(): Unit ={
    println(buySpecialTicket(new Young("年轻人")))
    println(buySpecialTicket(new Older("老人")))
  }
  class Man(val name:String);
  class Superman(val name:String){
    def fly=println("我会飞")
  }
  implicit def man2Superman(man:Man):Superman=new Superman(man.name)

  /**
    * 隐式转换,没有这个方法
    */
  def testImplicit1(): Unit ={
     val man=new Man("len");
     man.fly
  }
  class SignPen{
    def write(content:String)=println(content)
  }
  implicit val signPen=new SignPen
  def signForExam(name:String)(implicit signPen: SignPen): Unit ={
    signPen.write(name);
  }

  /***
    * 隐式参数
    */
  def testImplicit2(): Unit ={
    signForExam("aa");
  }

  class HelloActor extends scala.actors.Actor{
     def act(){
       while(true){
          receive{
            case name:String=>println("hello,"+name)
          }
       }
    }
  }
  def testActor(): Unit ={
    val actor=new HelloActor();
    actor.start();
    //发消息
    actor!"jack";
  }

  case class Message(content:String,send:Actor);
  class LiuTelephoneActor extends Actor{
    override def act(): Unit ={
       while (true){
          receive{
            case Message(content,send) =>{
              println("liu telephone:"+content);
              send!"i'm liu,please call me after 10 minutes"
            }
          }
       }
    }
  }
  class JackTelephoneActor(val liuTelephoneActor: Actor) extends Actor{
    override def act(): Unit ={
       liuTelephoneActor!Message("hello,liu,i'm jack",this);
       receive{
         case response:String=>println("jack telephone:"+response)
       }
    }
  }
  def testActor1(): Unit ={
    val liuActor=new LiuTelephoneActor();
    liuActor.start();
    val jackActor=new JackTelephoneActor(liuActor);
    jackActor.start();
  }




}
