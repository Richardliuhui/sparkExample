package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author liuhui
 * @date 2018-09-20 下午4:49
 */
public class SortWordCountSpark {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/words.txt");
        JavaRDD<String> worldRdd = lineRdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> onesRdd = worldRdd.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
        JavaPairRDD<String, Integer> worldCountRdd = onesRdd.reduceByKey((x1, x2) -> x1 + x2);
        //反转
        JavaPairRDD<Integer, String> mapToPair=worldCountRdd.mapToPair(x->new Tuple2<Integer, String>(x._2,x._1));
        JavaPairRDD<Integer, String> sortByKeyRDD = mapToPair.sortByKey(false);
        //再反转回来
        JavaPairRDD<String, Integer> resultRDD=sortByKeyRDD.mapToPair(x->new Tuple2<String, Integer>(x._2,x._1));
        resultRDD.foreach(x-> System.out.println(x._1+","+x._2));
        sc.close();
    }


}

