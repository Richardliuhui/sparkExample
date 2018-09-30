package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuhui
 * @date 2018-09-20 上午11:50
 */
public class LocalFileWordCountSpark {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/words.txt");
        JavaRDD<String> worldRdd = lineRdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> onesRdd = worldRdd.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
        JavaPairRDD<String, Integer> worldCountRdd = onesRdd.reduceByKey((x1, x2) -> x1 + x2);
        List<Tuple2<String, Integer>> tuple2List = worldCountRdd.collect();
        for (Tuple2<String,Integer> tuple : tuple2List) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        sc.close();
    }
}
