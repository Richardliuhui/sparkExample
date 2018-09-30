package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author liuhui
 * @date 2018-09-30 上午9:21
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("AreaHotGoodsStatSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);


        sc.stop();
    }
}
