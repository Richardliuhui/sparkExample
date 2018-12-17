package com.yp.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author liuhui
 * @date 2018-09-28 上午10:14
 */
public class RealTimeAdClickNumStatSpark {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <groupId> is a consumer group name to consume from topics\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }


        String brokers = args[0];
        String groupId = args[1];
        String topics = args[2];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
       // sparkConf.set("spark.default.parallelism","4");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
      //  jssc.checkpoint("hdfs://192.168.1.105:8099/checkpoint");

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        //过滤黑名单的数据
        JavaDStream<String> filterBlacklistDStream = filterBlacklistRDD(lines);
        //动态创建黑名单
        createBlacklistRDD(filterBlacklistDStream);
        //广告实时统计 <yyyyMMdd_province_city_adid,clickCount>
        JavaPairDStream<String, Long> realTimeStatDStream = calculateRealTimeStat(filterBlacklistDStream);
        //每天每个省top3热门广告
        calculateAdTop3(realTimeStatDStream);
        //统计近一小时的点击量
        calculateAdByWindow(lines);

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

    private static JavaDStream<String> filterBlacklistRDD(JavaDStream<String> lines) {
        JavaDStream<String> filterBlackListRDD = lines.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                //假如这个是从数据拿到的黑名单
                List<String> blacklist = new ArrayList<String>();
                List<Tuple2<String, Boolean>> list = new ArrayList<Tuple2<String, Boolean>>();
                for (String line : blacklist) {
                    list.add(new Tuple2(line, true));
                }
                JavaSparkContext sc = new JavaSparkContext(rdd.context());
                JavaPairRDD<String, Boolean> pairsBlacklistRDD = sc.parallelizePairs(list);
                //把原始RDD转成uid与原始数据的RDD
                JavaPairRDD<String, String> uidtoPairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String line) throws Exception {
                        //timestamp province city uid adid
                        String[] array = line.split(" ");
                        String uid = array[3];
                        return new Tuple2(uid, line);
                    }
                });
                //与黑名单RDD进行join
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> stringTuple2JavaPairRDD = uidtoPairRDD.leftOuterJoin(pairsBlacklistRDD);
                //去掉黑名单的数据
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = stringTuple2JavaPairRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        Optional<Boolean> exist = tuple._2._2;
                        if (exist.isPresent() && exist.get()) {
                            return true;
                        }
                        return false;
                    }
                });
                //返回原始数据
                JavaRDD<String> resultRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });
                return resultRDD;
            }
        });
        return filterBlackListRDD;
    }

    private static void createBlacklistRDD(JavaDStream<String> filterBlackListRDD) {
        JavaPairDStream<String, Long> dailyUserClickNumRDD = filterBlackListRDD.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String line) throws Exception {
                //timestamp province city uid adid
                String[] array = line.split(" ");
                Date time = new Date(Long.parseLong(array[0]));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                String timeStr = sdf.format(time);
                String uid = array[3];
                String adId = array[4];
                String key = timeStr +"_"+ uid +"_"+ adId;
                return new Tuple2(key, 1L);
            }
        });
        //统计yyyyMMdd_uid_adId的点击量
        JavaPairDStream<String, Long> dailyUserClickNumCountRDD = dailyUserClickNumRDD.reduceByKey((x, y) -> x + y);
        //用户点击数入库
        dailyUserClickNumCountRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                //此处把统计数据落库
                //foreachPartition就是为每个RDD分区创建一个dao实例,以达到高性能
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> tuple) throws Exception {

                    }
                });
            }
        });
        //点击量大于100的uid找出来
        JavaPairDStream<String, Long> blacklistRDD = dailyUserClickNumCountRDD.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                String[] keyArray = tuple._1.split("_");
                String time = keyArray[0];
                String uid = keyArray[1];
                String adid = keyArray[2];
                //此处可以查询数据库点击的数量
                int count = 1;
                if (count > 100) {
                    return true;
                }
                return false;
            }
        });
        //黑名单的UID
        JavaDStream<String> blacklistUidRDD = blacklistRDD.map(new Function<Tuple2<String, Long>, String>() {
            @Override
            public String call(Tuple2<String, Long> tuple) throws Exception {
                String[] keyArray = tuple._1.split("_");
                return keyArray[1];
            }
        });
        //对黑名单进行去重
        JavaDStream<String> blacklistUidDistinctRDD = blacklistUidRDD.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                return rdd.distinct();
            }
        });
        //黑名单入库
        blacklistUidDistinctRDD.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> uidIterator) throws Exception {
                        //以上是入库黑名单逻辑
                    }
                });
            }
        });
    }

    private static JavaPairDStream<String, Long> calculateRealTimeStat( JavaDStream<String> filterBlacklistDStream){
        final JavaPairDStream<String, Long> mapDStream = filterBlacklistDStream.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String line) throws Exception {
                //timestamp province city uid adid
                String[] array = line.split(" ");
                Date time = new Date(Long.parseLong(array[0]));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                String timeStr = sdf.format(time);
                String province = array[1];
                String city = array[2];
                String adId = array[4];
                String key = timeStr + "_" + province + "_" + city + "_" + adId;
                return new Tuple2(key, 1L);
            }
        });
        //统计yyyyMMdd_province_city_adid的访问量
        JavaPairDStream<String, Long> eggDStream = mapDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
                long clickCount = 0;
                if (v2.isPresent()) {
                    clickCount = v2.get();
                }
                for (Long num : v1) {
                    clickCount += num;
                }
                return Optional.of(clickCount);
            }
        });
        //把广告统计数据保存到数据库
        eggDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        //保持数据库的代码
                    }
                });
            }
        });
        return eggDStream;
    }

    /***
     * 计算每个省top3的AD
     * @param realTimeStatDStream
     */
    private static void calculateAdTop3(JavaPairDStream<String, Long> realTimeStatDStream){
        JavaDStream<Row> transformDStreamTop3 = realTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                //
                JavaPairRDD<String, Long> pairRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple2) throws Exception {
                        String[] array = tuple2._1.split("_");
                        //array[0]为timestamp,array[1]为province,array[3]为adID
                        String key = array[0] + "_" + array[1] + "_" + array[4];
                        return new Tuple2(key, tuple2._2);
                    }
                });
                //计算每个省第一个用户的访问量
                JavaPairRDD<String, Long> aggRDD = pairRDD.reduceByKey((x, y) -> x + y);
                JavaRDD<Row> rowRDD = aggRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                        String[] array = tuple._1.split("_");
                        String date = array[0];
                        String province = array[1];
                        String adid = array[2];
                        return RowFactory.create(date, province, adid, tuple._2);
                    }
                });
                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("adid", DataTypes.StringType, true),
                        DataTypes.createStructField("clickCount", DataTypes.LongType, true)
                ));
                SparkSession session = new SparkSession(rdd.context());
                Dataset<Row> dataFrame = session.createDataFrame(rowRDD, schema);
                dataFrame.registerTempTable("tmp_daily_ad_click_count");
                Dataset<Row> provinceTop3 = dataFrame.sqlContext().sql("select date,province,adid,clickCount from (" +
                        "select date,province,adid,clickCount" +
                        "ROW_NUMBER() OVER(PARTITON BY province ORDER BY clickCount DESC) rank " +
                        "from tmp_daily_ad_click_count ) t where rank>=3"
                );
                return provinceTop3.javaRDD();
            }
        });
    }

    private static void calculateAdByWindow(JavaDStream<String> lines){
        JavaPairDStream<String, Long> pairDStream = lines.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String line) throws Exception {
                //timestamp province city uid adid
                String[] array = line.split(" ");
                Date time = new Date(Long.parseLong(array[0]));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
                String timeStr = sdf.format(time);
                String province = array[1];
                String city = array[2];
                String adid = array[4];
                return new Tuple2<String, Long>(timeStr +"_"+ adid, 1L);
            }
        });
        //每小时作为一个窗口
        JavaPairDStream<String, Long> reduceByKeyAndWindowDtream = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.minutes(10));
        reduceByKeyAndWindowDtream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        //进行数据库的操作
                        while (iterator.hasNext()){
                            Tuple2<String,Long> tuple2=iterator.next();
                            String[]split=tuple2._1.split("_");

                        }
                    }
                });
            }
        });
    }
}
