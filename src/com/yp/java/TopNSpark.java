package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * @author liuhui
 * @date 2018-09-21 上午9:24
 */
public class TopNSpark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/top.txt");
        JavaRDD<Integer> mapRDD = lineRdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.parseInt(v1);
            }
        });
        JavaRDD<Integer> integerJavaRDD = mapRDD.sortBy(new Function<Integer, Object>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, false, 1);
        List<Integer> topN = integerJavaRDD.take(3);
        for (Integer n:topN){
            System.out.println(n);
        }


    }
}
