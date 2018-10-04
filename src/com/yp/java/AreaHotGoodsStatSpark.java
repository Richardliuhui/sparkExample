package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhui
 * @date 2018-09-29 上午9:53
 * 主要是spark sql的功能,热门商品统计
 */
public class AreaHotGoodsStatSpark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("AreaHotGoodsStatSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //mysql表要有city_info city_id,city_name,area
        //hive表中要有一个product_info表,product_id,product_name,extend_info
        //mysql中,设计结果表 task_id,area,area_level,product_id,city_names,click_count,product_name,product_status
        SQLContext sqlContext=new SQLContext(sc);
        sqlContext.udf().register("concat_string",new ConcatStringUDF(), DataTypes.StringType);

        JavaPairRDD<String, Row> clickActionByDateRDD=getClickActionByDate(sqlContext,args[1],args[2]);
        //从mysql查询城市信息
        JavaPairRDD<String, Row> cityInfoRDD=getClickInfo(sqlContext);



        sc.stop();
    }

    private static JavaPairRDD<String, Row> getClickActionByDate(SQLContext context,String startDate,String endDate){
        String sql="select" +
                "city_id" +
                "click_product_id" +
                " from user_visit_action" +
                " where click_product_id is not null" +
                " AND action_time>=' " +startDate+
                "' AND action_time<='" +endDate+
                " '";
        Dataset<Row> dataset = context.sql(sql);
        JavaRDD<Row> rowRDD=dataset.javaRDD();
        JavaPairRDD<String, Row> mapToPairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(0), row);
            }
        });
        return  mapToPairRDD;
    }
    private static JavaPairRDD<String, Row> getClickInfo(SQLContext context){
        Map<String,String> options=new HashMap<String,String>();
        options.put("url","xxx");
        options.put("dbtable","city_info");
        Dataset<Row> dataset = context.read().format("jdbc").options(options).load();
        JavaRDD<Row> rowRDD=dataset.javaRDD();
        JavaPairRDD<String, Row> mapToPairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(0), row);
            }
        });
        return mapToPairRDD;
    }
    private static void joinClickActionAndCityInfo(JavaPairRDD<String, Row> clickActionByDateRDD,JavaPairRDD<String, Row> cityInfoRDD ){
        JavaPairRDD<String, Tuple2<Row, Row>> joinRDD = clickActionByDateRDD.join(cityInfoRDD);
        JavaRDD<Row> map = joinRDD.map(new Function<Tuple2<String, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Row, Row>> tuple) throws Exception {
                String cityId=tuple._1;
                Row clickActionRow=tuple._2._1;
                Row cityInfoRow=tuple._2._2;
                String productId=clickActionRow.getString(1);
                String cityName=cityInfoRow.getString(1);
                String areaName=cityInfoRow.getString(2);
                return RowFactory.create(cityId,cityName,areaName,productId);
            }
        });
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("cityId", DataTypes.StringType, true),
                DataTypes.createStructField("cityName", DataTypes.StringType, true),
                DataTypes.createStructField("areaName", DataTypes.StringType, true),
                DataTypes.createStructField("productId", DataTypes.LongType, true)
        ));
        SparkSession session = new SparkSession(map.context());
        Dataset<Row> dataFrame = session.createDataFrame(map, schema);
        dataFrame.registerTempTable("tmp_clk_prod_basic");
    }
}
