class PersionHandler(name:String) extends SignatureValidHandler with DataValidHandler{

    def sayHello():Unit={
        println("say hello,"+name)
        handle(name);
    }
}
