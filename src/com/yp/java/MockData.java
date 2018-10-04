package com.yp.java;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: TODO
 * @date Date : 2018年10月01日 下午5:55
 */
public class MockData {

    public static void mockData(JavaSparkContext context, SQLContext sqlContext){
        List<Row> list=new ArrayList<Row>();
        String []searchKeywords=new String[]{"hello","java"};
        String data="2018-10-01 23:44:23";
        String []actions=new String[]{"search","click","order","pay"};
        Random random=new Random();

        JavaRDD<Row> rowRdd = context.parallelize(list);
        StructType schema = DataTypes.createStructType(java.util.Arrays.asList(
                DataTypes.createStructField("data", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.StringType, true),
                DataTypes.createStructField("click_product_id", DataTypes.StringType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)
        ));
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRdd, schema);
        dataFrame.registerTempTable("user_visit_action");
    }
}
