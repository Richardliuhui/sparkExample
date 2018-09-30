package com.yp.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-21 上午9:37
  *
  */
object TopNScala {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]");
    val sc=new SparkContext(conf);
    val fileRdd=sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/top.txt");
    val pairRDD=fileRdd.map(x=>(x.toInt,x));
    val sortRDD=pairRDD.sortByKey(false);
    val resultRDD=sortRDD.map(x=>x._1);
    val topN=resultRDD.take(3);
    for (a<-topN){
      println(a)
    }
  }

}
