
trait SignatureValidHandler extends Handler{
  override def handle(data: String): Unit ={
    println("singature valid :"+data);
    super.handle(data);
  }
}
