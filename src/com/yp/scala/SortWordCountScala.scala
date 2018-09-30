package com.yp.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-20 下午5:20
  *
  */
object SortWordCountScala {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]");
    val sc=new SparkContext(conf);
    val fileRdd=sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/words.txt");
    val result=fileRdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y);
    val rdd1=result.map(x=>(x._2,x._1));
    val rdd2=rdd1.sortByKey(false);
    val rdd3=rdd2.map(x=>(x._2,x._1));
    rdd3.foreach(x=>println(x._1+","+x._2))
    sc.stop();
  }

}
