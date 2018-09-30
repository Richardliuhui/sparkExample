
trait DataValidHandler extends Handler{
  override def handle(data: String): Unit = {
      println("data valid:"+data);
      super.handle(data);
  }
}
