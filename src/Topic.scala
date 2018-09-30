class Topic(newTopicId:Int=1) {

   var topicId:Int=newTopicId;
   var topicName:String=_;

   def this(topicId:Int,newTopicName:String ){
     this(topicId);
     this.topicName=newTopicName;
   }

   def print(): Unit ={
      println("topicId="+topicId+","+topicName);
   }


}
