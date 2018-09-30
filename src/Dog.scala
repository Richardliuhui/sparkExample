class Dog extends ZOO with Constant with Logger{

  override var ear: String = "4";

  override def getName(): String ="dog"

  override def move(): Unit ={
      log("我会走"+","+address)
  }
}
