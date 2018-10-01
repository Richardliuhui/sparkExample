package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: session统计
 * @date Date : 2018年10月01日 下午3:33
 */
public class SessionStatSpark {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("SessionStatSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //session_aggr_stat  session聚合统计结果
        //session_random_extrat 随机抽取功能抽取出来的100个session
        //top10_category表存储点击、下单和支付排序出来的top10品类数据
        //top10_category_session存储top100每个品类的点击top10品类数据
        //session_detail用来存储随机抽取出来的session的明细数据
        //task表存储

        sc.stop();
    }
}
