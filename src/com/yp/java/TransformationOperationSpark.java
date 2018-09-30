package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuhui
 * @date 2018-09-20 下午3:18
 */
public class TransformationOperationSpark {
    public static void main(String[] args) {
        joinAndGroup();
    }

    public static void groupByKey(){
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list= Arrays.asList(
                new Tuple2("class1",90),
                new Tuple2("class2",70),
                new Tuple2("class1",70),
                new Tuple2("class2",50)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                System.out.printf("班级:"+tuple._1()+",成绩:");
                for (Integer score:tuple._2()){
                    System.out.printf(score+"\t");
                }
                System.out.println("");
            }
        });
        sc.close();
    }
    public static void reduceByKey(){
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list= Arrays.asList(
                new Tuple2("class1",90),
                new Tuple2("class2",70),
                new Tuple2("class1",70),
                new Tuple2("class2",50)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey(((v1, v2) -> v1 + v2));
        reduceByKeyRDD.foreach(x-> System.out.println(x._1+","+x._2));
        sc.close();
    }
    public static void sortByKey(){
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> list= Arrays.asList(
                new Tuple2(90,"tom"),
                new Tuple2(100,"jack"),
                new Tuple2(80,"richard"),
                new Tuple2(40,"rom")
        );
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(list);
        JavaPairRDD<Integer, String> reduceByKeyRDD = pairRDD.sortByKey(false);
        reduceByKeyRDD.foreach(x-> System.out.println(x._1+","+x._2));
        sc.close();
    }
    public static void joinAndGroup(){
        SparkConf conf=new SparkConf()
                .setAppName("joinAndGroup")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> studentList= Arrays.asList(
                new Tuple2(1,"tom"),
                new Tuple2(2,"jack"),
                new Tuple2(3,"richard"),
                new Tuple2(4,"rom")
        );
        List<Tuple2<Integer,Integer>> scores= Arrays.asList(
                new Tuple2(1,90),
                new Tuple2(2,70),
                new Tuple2(3,87),
                new Tuple2(4,66)
        );
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentRDD.join(scoresRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple2) throws Exception {
                System.out.println("学号:"+tuple2._1()+",姓名:"+tuple2._2._1()+",成绩:"+tuple2._2._2());
            }
        });

        sc.close();
    }

}
