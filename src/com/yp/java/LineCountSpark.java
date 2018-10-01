package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author liuhui
 * @date 2018-09-20 下午2:15
 */
public class LineCountSpark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> linesRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/lines.txt");
        JavaPairRDD<String, Integer> pairRDD = linesRdd.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD<String, Integer> reduceByKeyRdd = pairRDD.reduceByKey((x, y) -> x + y);
        reduceByKeyRdd.foreach(x-> System.out.println(x._1+","+x._2));
        sc.close();
    }
}
