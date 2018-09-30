package com.yp.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-20 下午2:23
  *
  */
object LineCountScala {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val fileRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/lines.txt");
    // val result=fileRdd.map(x=>(x,1)).reduceByKey((x,y)=>x+y);
    val result = fileRdd.map(x => (x, 1)).countByKey();

    result.foreach(x => println(x._1 + "," + x._2))
    sc.stop();
  }

}
