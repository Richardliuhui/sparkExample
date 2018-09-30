package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 二次排序
 * @author liuhui
 * @date 2018-09-20 下午5:34
 */
public class SecondSortSpark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/secondsort.txt");
        JavaPairRDD<SecondSortKey, String> javaPairRDD = lineRdd.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] array = line.split(" ");
                return new Tuple2<SecondSortKey, String>(new SecondSortKey(Integer.parseInt(array[0]), Integer.parseInt(array[1])), line);
            }
        });
        JavaPairRDD<SecondSortKey, String> sortByKeyRDD = javaPairRDD.sortByKey(false);
        JavaRDD<String> rdd = sortByKeyRDD.map(x -> x._2);
        rdd.foreach(x-> System.out.println(x));
        sc.close();
    }
}
