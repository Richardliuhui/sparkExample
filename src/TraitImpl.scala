class TraitImpl extends Trait1 with Trait2 with Logger{
  override def sayHello(): Unit = {
      log("say hello")
  }

  override def getInfo(name: String, age: Int): String ={
     name+","+age;
  }
}
