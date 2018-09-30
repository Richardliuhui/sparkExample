package com.yp.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-05 下午2:27
  *
  */
object HdfsWordCountSpark {
  def main(args: Array[String]) {

     val conf=new SparkConf().setAppName("worldCountSpark").setMaster("local[1]");
     val context=new SparkContext(conf);
     val hdfsRdd=context.textFile("hdfs://119.147.160.74:15270/worldcount.txt",10);
     val wordCount=hdfsRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_);
     wordCount.foreach(x=>println(x._1+","+x._2));
     context.stop();
  }

}
