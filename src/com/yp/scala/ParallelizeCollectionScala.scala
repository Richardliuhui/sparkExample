package com.yp.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-20 上午11:35
  *
  */
object ParallelizeCollectionScala {

  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]");
    val sc=new SparkContext(conf);
    val list=Array(1,2,3,4,5,6);
    val rdd=sc.parallelize(list);
    val sum=rdd.reduce(_+_);
    println("sum="+sum)
    sc.stop();
  }

}
