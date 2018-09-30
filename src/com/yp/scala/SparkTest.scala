package com.yp.scala

import org.apache.spark._;

/**
  * ${DESCRIPTION} 
  *
  * @author liuhui
  * @create 2018-09-18 下午4:55
  *
  */
object SparkTest {

  def main(args: Array[String]) {
     val conf=new SparkConf().setAppName("test").setMaster("local[*]");
     val sc=new SparkContext(conf);
     val array=Array(1,2,3,4,5);
     val rdd1=sc.parallelize(array);
     val maprdd=rdd1.map(x=>x*2);
     val result=maprdd.reduce(_+_);
     println("s="+result.toString)
  }

}
