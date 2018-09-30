import java.io.{IOException, FileNotFoundException}


class ExceptionHandler {

  def handler():Unit={

     try{
       throw new NullPointerException("空指针异常");
     }catch {
       case e:NullPointerException=>print(e);
       case e:ArrayIndexOutOfBoundsException=>print(e);
     }finally {
       println("结束了");
     }
  }

  def print(e:Exception): Unit ={
     println("出错了,原因是:"+e.getMessage)
  }
  def proccessException(e:Exception): Unit ={
    e match {
      case e1:FileNotFoundException=>println("文件没找到")
      case e2:IOException=>println("io error")
      case _:Exception=>println("未知错误")
    }
  }

}
