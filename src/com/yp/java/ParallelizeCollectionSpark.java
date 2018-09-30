package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuhui
 * @date 2018-09-20 上午11:29
 */
public class ParallelizeCollectionSpark {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(1,2,3,4,9,1,4);
        JavaRDD<Integer> javaRDD = sc.parallelize(list);
      //  JavaRDD<Integer> filterRdd = javaRDD.filter(x -> x % 2 == 0);
        Integer sum = javaRDD.reduce((x1, x2) -> x1 + x2);
        System.out.println("sum="+sum);
        sc.close();
    }
}
