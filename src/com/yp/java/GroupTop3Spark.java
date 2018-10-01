package com.yp.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author liuhui
 * @date 2018-09-21 上午10:33
 */
public class GroupTop3Spark {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("FileWordSumSpark")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("file:///Users/thejoyrun/Documents/workspace/gzserver/scala02/src/com/yp/group.txt");
        JavaPairRDD<String, Integer> pairRDD = lineRdd.mapToPair(s->{
            String array[] = s.split(" ");
            return new Tuple2(array[0], Integer.parseInt(array[1]));
        });
        JavaPairRDD<String, Iterable<Integer>> groupRdd = pairRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> javaPairRDD = groupRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                String className=tuple._1;
                Iterator<Integer> iterable=tuple._2.iterator();
                Integer []top3=new Integer[3];
                while (iterable.hasNext()){
                    Integer score=iterable.next();
                    for(int i=0;i<3;i++){
                        if(top3[i]==null){
                            top3[i]=score;
                            break;
                        }else if(score>top3[i]){
                           for (int j=2;j>i;j--){
                               top3[j]=top3[j-1];
                           }
                            top3[i]=score;
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });
        javaPairRDD.foreach(tuple2->{
            System.out.println(tuple2._1+":");
            Iterator<Integer> iterator=tuple2._2.iterator();
            while (iterator.hasNext()){
                System.out.print(iterator.next()+"\t");
            }
            System.out.println("aa");
        });
        sc.stop();
    }
}
