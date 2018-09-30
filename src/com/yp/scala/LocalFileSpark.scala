package com.yp.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-18 下午5:35
  *
  */
object LocalFileSpark {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]");
    val sc=new SparkContext(conf);
    val fileRdd=sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/words.txt");
    val result=fileRdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y);
    result.foreach(x=>println(x._1+","+x._2))
    sc.stop();
  }

}
