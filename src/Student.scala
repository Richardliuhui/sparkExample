/**
  */
class Student(name:String,age:Int) extends Person(name,age){
  override def info(): String ={
     val s=name+","+age;
     s;
  }

  override def sayHello(): Unit = {
     println("my name is"+name);
  }
}
