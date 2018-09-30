package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author liuhui
 * @date 2018-09-20 上午11:16
 */
public class FileWordSumSpark {

    public static void main(String[] args) throws Exception{
        SparkConf conf=new SparkConf()
                   .setAppName("FileWordSumSpark")
                   .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/spark.txt");
        JavaRDD<Integer> mapRDD = javaRDD.map(x -> x.length());
        int sum=mapRDD.reduce((a,b)->a+b);
        System.out.printf("文件总文字数:"+sum);
        Thread.sleep(1000*60*2);
        sc.close();
    }
}
