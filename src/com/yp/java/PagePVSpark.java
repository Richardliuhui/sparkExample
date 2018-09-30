package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: TODO
 * @date Date : 2018年09月30日 下午2:53
 */
public class PagePVSpark {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("pagePvSpark");
        JavaSparkContext sc=new JavaSparkContext(conf);

        sc.stop();
    }
}
