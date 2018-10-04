package com.yp.java;

import com.alibaba.fastjson.JSONObject;
import com.yp.java.conf.ConfigurationManager;
import com.yp.java.constant.Constants;
import com.yp.java.dao.DAOFactory;
import com.yp.java.dao.ISessionAggrStatDAO;
import com.yp.java.dao.ITaskDAO;
import com.yp.java.model.SessionAggrStatModel;
import com.yp.java.model.Task;
import com.yp.java.util.DateUtils;
import com.yp.java.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: session统计
 * @date Date : 2018年10月01日 下午3:33
 * @author liu
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
        SQLContext sqlContext=getSQLContext(sc);
        MockData.mockData(sc,sqlContext);
        ITaskDAO taskDAO= DAOFactory.getTaskDAO();
        Task task = taskDAO.findTaskById(Integer.parseInt(args[0]));
        JSONObject jsonObject=JSONObject.parseObject(task.getDataJson());
        //根据条件过滤数据
        JavaRDD<Row> actionByDateRangeRDD = getActionByDateRange(sqlContext, jsonObject);
        //转换成<sessionId,Row>的RDD
        JavaPairRDD<String,Row> session2ActionRDD=getSession2ActionRDD(actionByDateRangeRDD);
        // 把访问信息聚合成<sessionId,egginfo>
        JavaPairRDD<String, String> session2FullAggrInfoRDD = aggregateBySession(sqlContext,session2ActionRDD);

        AccumulatorV2<String,String> accumulatorV2=new SessionAggrStatAccumulator();
        sc.sc().register(accumulatorV2);
        //过滤session数据
        JavaPairRDD<String, String> filterSessionRDD = filterSessionAndAggrStat(session2FullAggrInfoRDD, jsonObject,accumulatorV2);
        //过滤session详细数据RDD
        JavaPairRDD<String, Row> session2DetailRDD = getSession2DetailRDD(filterSessionRDD, session2ActionRDD);

        //把session统计的结果入库
        calculateAndPersistAggrStat(accumulatorV2.value());
        //随机生成session数据
        randomExtractSession(session2FullAggrInfoRDD,session2ActionRDD);
        //计算top 10的分类
        List<Tuple2<CategorySortKey, String>> top10CategoryList=getTop10Category(session2DetailRDD);

        top10Session(sc,top10CategoryList,session2DetailRDD);


        sc.stop();
    }

    private static SQLContext getSQLContext(JavaSparkContext sc){
        boolean isLocal= ConfigurationManager.getBooleanValue(Constants.SPARK_LOCAL);
        if(isLocal){
            return new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }

    /***
     * 查询指定范围的行为数据
     * @param context
     * @param jsonObject
     * @return
     */
    private static JavaRDD<Row> getActionByDateRange(SQLContext context, JSONObject jsonObject){
        String startDate=jsonObject.getString("startDate");
        String endDate=jsonObject.getString("startDate");

        String sql="select * from user_visit_action" +
                "where date>='" +startDate+"'"+
                " and date<='" +endDate+"'";
        return context.sql(sql).javaRDD();
    }

    private static JavaPairRDD<String,Row> getSession2ActionRDD(JavaRDD<Row> actionRDD){
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(x -> {
            String sessionId = x.getString(2);
            return new Tuple2<>(sessionId, x);
        });
        return session2ActionRDD;
    }
    private static JavaPairRDD<String,String> aggregateBySession(SQLContext sqlContext,JavaPairRDD<String,Row> sessionToPairRDD){

        //对每个session进行聚合
        JavaPairRDD<String, Iterable<Row>> session2ActionRDD = sessionToPairRDD.groupByKey();
        JavaPairRDD<String, String> uid2PartAggrInfoRDD = session2ActionRDD.mapToPair(x -> {
            String sessionId = x._1;
            String uid = null;
            Iterator<Row> iterable = x._2.iterator();
            StringBuffer searchKeywordBuffer = new StringBuffer();
            StringBuffer categoryIdsBuffer = new StringBuffer();
            Date startTime=null;
            Date endTime=null;
            int stepLength=0;
            while (iterable.hasNext()) {
                Row row = iterable.next();
                if (null == uid) {
                    uid = row.getString(1);
                }
                //计算session开始时间与结束时间
                Date actionTime= DateUtils.parseTime(row.getString(4));
                if(startTime==null){
                    startTime=actionTime;
                }
                if(endTime==null){
                    endTime=actionTime;
                }
                if(actionTime.before(startTime)){
                    startTime=actionTime;
                }
                if(actionTime.after(endTime)){
                    endTime=actionTime;
                }
                String searchKeyword = row.getString(5);
                String clickCategoryId = row.getString(6);
                if (StringUtils.isNoneBlank(searchKeyword)) {
                    if (searchKeywordBuffer.toString().contains(searchKeyword)) {
                        searchKeywordBuffer.append(searchKeyword + "，");
                    }
                }
                if (StringUtils.isNoneBlank(clickCategoryId)) {
                    if (categoryIdsBuffer.toString().contains(clickCategoryId)) {
                        categoryIdsBuffer.append(clickCategoryId + "，");

                    }
                }
                stepLength++;
            }
            long visitTime=(endTime.getTime()-startTime.getTime())/1000;
            String searchKeyword = searchKeywordBuffer.substring(0, searchKeywordBuffer.length() - 1);
            String categoryIds = categoryIdsBuffer.substring(0, categoryIdsBuffer.length() - 1);
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS
                    + "=" + searchKeyword + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + categoryIds+
                    Constants.FIELD_VISIT_TIME+"="+visitTime+"|"+Constants.FIELD_VISIT_STEP+"="+stepLength+
                    "|"+Constants.FIELD_START_TIME+DateUtils.formatTime(startTime);
            return new Tuple2<>(uid, partAggrInfo);
        });
        String sql="select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<String, Row> uid2UserInfoRDD = userInfoRDD.mapToPair(x -> {
            String uid = x.getString(0);
            return new Tuple2<>(uid, x);
        });
        JavaPairRDD<String, Tuple2<String, Row>> joinRDD = uid2PartAggrInfoRDD.join(uid2UserInfoRDD);
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = joinRDD.mapToPair(x -> {
            String partAggrInfo = x._2._1;
            Row row = x._2._2;
            String sessionId = StringUtil.getFieldFromConcatString(partAggrInfo,"|","sessionId");
            String age = row.getString(3);
            String professional = row.getString(4);
            String city = row.getString(5);
            String sex = row.getString(6);
            String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
                    Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY
                    + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex;
            return new Tuple2<>(sessionId, fullAggrInfo);
        });
        return sessionId2FullAggrInfoRDD;
    }

    /***
     * 过滤session数据
     * @return
     */
    private static JavaPairRDD<String,String> filterSessionAndAggrStat(JavaPairRDD<String,String>session2FullAggrInfoRDD,final JSONObject jsonObject,AccumulatorV2<String,String> accumulatorV2){
        JavaPairRDD<String, String> filterRDD = session2FullAggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> x) throws Exception {
                String fullAgrInfo = x._2;
                int startAge = Integer.parseInt(jsonObject.getString("startAge"));
                int endAge = Integer.parseInt(jsonObject.getString("endAge"));
                //从fullAgrInfo去获取
                int age = Integer.parseInt(StringUtil.getFieldFromConcatString(fullAgrInfo, "\\|", "age"));
                if (age >= startAge && age <= endAge) {
                    return true;
                }
                //统计session数量
                accumulatorV2.add(Constants.FIELD_SESSION_COUNT);
                long visitStep=Long.parseLong(StringUtil.getFieldFromConcatString(fullAgrInfo,"\\|",Constants.FIELD_VISIT_STEP));
                long visitTime=Long.parseLong(StringUtil.getFieldFromConcatString(fullAgrInfo,"\\|",Constants.FIELD_VISIT_TIME));
                //统计每个区间的访问步长
                calculateVisitStep(visitStep,accumulatorV2);
                //统计每个区间的访问时长
                calculateVisitTime(visitTime,accumulatorV2);
                return null;
            }
            private  void calculateVisitStep(long visitStep,AccumulatorV2<String,String> accumulatorV2){
                if(visitStep>=1 && visitStep<=3){
                    accumulatorV2.add("time_1s_3s");
                }else if(visitStep>3 && visitStep<=6){
                    accumulatorV2.add("time_4s_6s");
                }else if(visitStep>6 && visitStep<9){
                    accumulatorV2.add("time_7s_9s");
                }else if(visitStep>9 && visitStep<30){
                    accumulatorV2.add("time_10s_30s");
                }else if(visitStep>30 && visitStep<60){
                    accumulatorV2.add("time_30s_60s");
                }
            }
            private  void calculateVisitTime(long visitTime,AccumulatorV2<String,String> accumulatorV2){
                if(visitTime>=1 && visitTime<=3){
                    accumulatorV2.add("step_1_3");
                }else if(visitTime>3 && visitTime<=6){
                    accumulatorV2.add("step_4s_6s");
                }else if(visitTime>6 && visitTime<9){
                    accumulatorV2.add("step_7s_9s");
                }else if(visitTime>9 && visitTime<30){
                    accumulatorV2.add("step_10s_30s");
                }else if(visitTime>30 && visitTime<60){
                    accumulatorV2.add("step_30s_60s");
                }
            }
        });
        return filterRDD;
    }

    private static JavaPairRDD<String,Row> getSession2DetailRDD(JavaPairRDD<String,String> session2AggrInfoRDD,
                                                               JavaPairRDD<String,Row> session2ActionRDD ){
        JavaPairRDD<String, Tuple2<String, Row>> joinRDD = session2AggrInfoRDD.join(session2ActionRDD);
        JavaPairRDD<String, Row> session2DetailRDD = joinRDD.mapToPair(x -> {
            return new Tuple2<>(x._1, x._2._2);
        });
        return session2DetailRDD;
    }

    /***
     * 把session统计的结果入库
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value){
        long sessionCount=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|",Constants.FIELD_SESSION_COUNT));
        long visit_length_1s_3s=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","time_1s_3s"));
        long visit_length_4s_6s=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","time_4s_6s"));
        long visit_length_7s_9s=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","time_7s_9s"));
        long visit_step_1_3=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","step_1_3"));
        long visit_step_4_6=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","time_4_6"));
        long visit_step_7_9=Long.parseLong(StringUtil.getFieldFromConcatString(value,"\\|","time_7_9"));
        double visit_length_1_3_ratio=visit_length_1s_3s/sessionCount;

        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        SessionAggrStatModel statModel=new SessionAggrStatModel();
        statModel.setSessionCount(sessionCount);
        //具体细节不写了
        sessionAggrStatDAO.insertSessionAggrStat(statModel);
    }
    private static void randomExtractSession(JavaPairRDD<String,String> session2AggrInfoRDD,JavaPairRDD<String,Row> session2ActionRDD){
        JavaPairRDD<String, String> hour2SessionRDD = session2AggrInfoRDD.mapToPair(x -> {
            String aggrInfo = x._2;
            String startTime = StringUtil.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
            String startHour = DateUtils.getDateHour(startTime);
            return new Tuple2<>(startHour, aggrInfo);
        });
        Map<String, Long> hourSessionCountMap = hour2SessionRDD.countByKey();
        Map<String,Map<String,Long>> dayHourCountMap=new HashMap<>();
        for (Map.Entry<String,Long> entry:hourSessionCountMap.entrySet()){
            String dateHour=entry.getKey();
            String day=dateHour.split("_")[0];
            String hour=dateHour.split("_")[1];
            long count=entry.getValue();
            Map<String,Long> hourCountMap=dayHourCountMap.get(day);
            if(null==hourCountMap){
                hourCountMap=new HashMap<>();
                dayHourCountMap.put(day,hourCountMap);
            }
            hourCountMap.put(hour,count);
        }
        long extractNumberPerDay=100/dayHourCountMap.size();
        Random random=new Random();
        Map<String,Map<String,List<Integer>>> dateHourExtractMap=new HashMap<>();
        for (Map.Entry<String,Map<String,Long>> entry:dayHourCountMap.entrySet()){
             String day=entry.getKey();
             Map<String,Long> hourCountMap=entry.getValue();
             long sessionCount=0;
             for (long hourCount:hourCountMap.values()){
                 sessionCount+=hourCount;
             }
             Map<String,List<Integer>> hourExtractMap=dateHourExtractMap.get(day);
             if(null==hourExtractMap){
                 hourExtractMap=new HashMap<>();
                 dateHourExtractMap.put(day,hourExtractMap);
             }
             for (Map.Entry<String,Long> hourCountEntry:hourCountMap.entrySet()){
                 String hour=hourCountEntry.getKey();
                 long count=hourCountEntry.getValue();
                 Long hourExtractNumber=(count/sessionCount)*extractNumberPerDay;
                 List<Integer> extractIndexList=hourExtractMap.get(hour);
                 if(null==extractIndexList){
                     extractIndexList=new ArrayList<>();
                     hourExtractMap.put(hour,extractIndexList);
                 }
                 for (int i=0;i<hourExtractNumber;i++){
                     int extractIndex=random.nextInt((int)count);
                     while (extractIndexList.contains(extractIndex)){
                         extractIndex=random.nextInt((int)count);
                     }
                     extractIndexList.add(extractIndex);
                 }
             }
        }
        //遍历每天每小时的数据
        final JavaPairRDD<String, Iterable<String>> time2SessionRDD = hour2SessionRDD.groupByKey();
        JavaPairRDD<String, String> sessionToSessionRDD = time2SessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                Iterator<String> iterator = tuple._2.iterator();
                int index = 0;
                String dateHour = tuple._1.split("_")[0];
                String hour = tuple._1.split("_")[1];
                List<Tuple2<String, String>> sessions = new ArrayList<>();
                List<Integer> extractIndexList = dateHourExtractMap.get(dateHour).get(hour);
                while (iterator.hasNext()) {
                    String sessionAggr = iterator.next();
                    index++;
                    if (extractIndexList.contains(index)) {
                        String sessionId = StringUtil.getFieldFromConcatString(sessionAggr, "\\|", Constants.FIELD_SESSION_ID);
                        sessions.add(new Tuple2<>(sessionId, sessionId));
                        //后续入数据库操作的不写了
                    }

                }
                return sessions.iterator();

            }
        });
        JavaPairRDD<String, Tuple2<String, Row>> joinRDD = sessionToSessionRDD.join(session2ActionRDD);
        joinRDD.foreach(tuple2->{
            Row row=tuple2._2._2;

            //下面保存数据的操作不写了
        });

    }

    private static List<Tuple2<CategorySortKey, String>> getTop10Category(JavaPairRDD<String, Row> session2DetailRDD ){
        JavaPairRDD<Long, Long> categoryRDD = session2DetailRDD.flatMapToPair(x -> {
            Row row = x._2;
            List<Tuple2<Long, Long>> list = new ArrayList<>();
            Long clickCategory = row.getLong(8);
            if (null != clickCategory) {
                list.add(new Tuple2<>(clickCategory, clickCategory));
            }
            String orderCategoryIds = row.getString(8);
            if (null != orderCategoryIds) {
                String[] splitCategoryId = orderCategoryIds.split(",");
                for (String orderCategoryId : splitCategoryId) {
                    list.add(new Tuple2<>(Long.parseLong(orderCategoryId), Long.parseLong(orderCategoryId)));
                }
            }
            String payCategoryIds = row.getString(10);
            if (null != payCategoryIds) {
                String[] splitPayCategoryId = payCategoryIds.split(",");
                for (String payCategoryId : splitPayCategoryId) {
                    list.add(new Tuple2<>(Long.parseLong(payCategoryId), Long.parseLong(payCategoryId)));
                }
            }
            return list.iterator();
        });
        //计算各品类点击次数、下单次数、支付次数
        //点击数
        JavaPairRDD<String, Row> clickActionRDD = session2DetailRDD.filter(x -> {
            Row row = x._2;
            return row.getString(6) != null ? true : false;
        });
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(x -> {
            Row row = x._2;
            Long categoryId = row.getLong(6);
            return new Tuple2<>(categoryId, 1L);
        });
        JavaPairRDD<Long, Long> clickActionCountRDD = clickCategoryIdRDD.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Row> orderActionRDD = session2DetailRDD.filter(x -> {
            Row row = x._2;
            return row.getString(8) != null ? true : false;
        });
        JavaPairRDD<Long, Long> orderActionPairRDD = orderActionRDD.flatMapToPair(x -> {
            Row row = x._2;
            String categoryIds = row.getString(8);
            String[] categoryIdsArray = categoryIds.split(",");
            List<Tuple2<Long, Long>> list = new ArrayList<>();
            for (String categoryId : categoryIdsArray) {
                list.add(new Tuple2<>(Long.parseLong(categoryId), 1L));
            }
            return list.iterator();
        });
        JavaPairRDD<Long, Long> orderActionCountRDD = orderActionPairRDD.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Row> payActionRDD = session2DetailRDD.filter(x -> {
            Row row = x._2;
            return row.getString(10) != null ? true : false;
        });
        JavaPairRDD<Long, Long> payActionPairRDD = payActionRDD.flatMapToPair(x -> {
            Row row = x._2;
            String categoryIds = row.getString(10);
            String[] categoryIdsArray = categoryIds.split(",");
            List<Tuple2<Long, Long>> list = new ArrayList<>();
            for (String categoryId : categoryIdsArray) {
                list.add(new Tuple2<>(Long.parseLong(categoryId), 1L));
            }
            return list.iterator();
        });
        JavaPairRDD<Long, Long> payActionCountRDD = payActionPairRDD.reduceByKey((x, y) -> x + y);
        //分类的点击数、下单数、支付数
        JavaPairRDD<Long,String> categoryId2CountRDD=joinCategoryAndData(categoryRDD,clickActionCountRDD,orderActionCountRDD,payActionCountRDD);

        //将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序
        JavaPairRDD<CategorySortKey, String> categorySortKeyStringRDD = categoryId2CountRDD.mapToPair(x -> {
            String countInfo = x._2;
            long clickCount = Long.parseLong(StringUtil.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.parseLong(StringUtil.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.parseLong(StringUtil.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            CategorySortKey categorySortKey = new CategorySortKey();
            categorySortKey.setClickCount(clickCount);
            categorySortKey.setOrderCount(orderCount);
            categorySortKey.setPayCount(payCount);
            return new Tuple2<>(categorySortKey, countInfo);
        });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = categorySortKeyStringRDD.sortByKey(false);
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        //下面是把top 10的数据落库，此段代码不写了

        return top10CategoryList;


    }

    private static JavaPairRDD<Long,String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryRDD,
                                                                JavaPairRDD<Long, Long> clickActionCountRDD,
                                                                JavaPairRDD<Long, Long> orderActionCountRDD ,
                                                                JavaPairRDD<Long, Long> payActionCountRDD ){
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tempJoinPairRDD = categoryRDD.leftOuterJoin(clickActionCountRDD);
        JavaPairRDD<Long, String> tempMapRDD = tempJoinPairRDD.mapToPair(x -> {
            Long categoryId = x._1;
            Optional<Long> clickCountOption = x._2._2;
            long clickCount = 0;
            if (clickCountOption.isPresent()) {
                clickCount = clickCountOption.get();
            }
            String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
            return new Tuple2<>(categoryId, value);
        });
        tempMapRDD=tempMapRDD.leftOuterJoin(orderActionCountRDD).mapToPair(x->{
            Long categoryId = x._1;
            String value=x._2._1;
            Optional<Long> optional = x._2._2;
            long orderCount = 0;
            if (optional.isPresent()) {
                orderCount = optional.get();
            }
            value = value+"|"+Constants.FIELD_ORDER_COUNT + "=" + orderCount;
            return new Tuple2<>(categoryId, value);
        });
        tempMapRDD=tempMapRDD.leftOuterJoin(payActionCountRDD).mapToPair(x->{
            Long categoryId = x._1;
            String value=x._2._1;
            Optional<Long> optional = x._2._2;
            long payCount = 0;
            if (optional.isPresent()) {
                payCount = optional.get();
            }
            value = value+"|"+Constants.FIELD_PAY_COUNT + "=" + payCount;
            return new Tuple2<>(categoryId, value);
        });
        return tempMapRDD;
    }

    private static void top10Session(JavaSparkContext sc,List<Tuple2<CategorySortKey, String>> list,JavaPairRDD<String, Row> session2DetailRDD){

        List<Tuple2<Long,Long>> top10CategoryIdList=new ArrayList<>();
        for (Tuple2<CategorySortKey, String> tuple2:list){
            Long categoryId=Long.parseLong(StringUtil.getFieldFromConcatString(tuple2._2,"\\|",Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId,categoryId));
        }
        JavaPairRDD<Long, Long> top10CategoryRDD = sc.parallelizePairs(top10CategoryIdList);
        JavaPairRDD<String, Iterable<Row>> session2DetailsRDD = session2DetailRDD.groupByKey();

        JavaPairRDD<Long, String> category2SessionCountRDD = session2DetailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String sessionId = tuple2._1;
                Iterator<Row> iterator = tuple2._2.iterator();
                Map<Long, Long> categoryCountMap = new HashMap<>();
                //计算每个session对每个分类的点击次数
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (row.getString(6) != null) {
                        long categoryId = row.getLong(6);
                        Long count = categoryCountMap.get(categoryId);
                        if (null == count) {
                            count = 0L;
                        }
                        count++;
                        categoryCountMap.put(categoryId, count);
                    }
                }
                List<Tuple2<Long, String>> category2SessionCountList = new ArrayList<>();
                for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                    String value = sessionId + "," + entry.getValue();
                    category2SessionCountList.add(new Tuple2<>(entry.getKey(), value));
                }
                return category2SessionCountList.iterator();
            }
        });
        //获取top 10分类，被各个session的点击数
        JavaPairRDD<Long, Tuple2<Long, String>> joinRDD = top10CategoryRDD.join(category2SessionCountRDD);
        JavaPairRDD<Long, String> top10CategorySessionRDD = joinRDD.mapToPair(x -> {
            return new Tuple2<>(x._1, x._2._2);
        });
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionRDD.groupByKey();
        JavaPairRDD<String, String> top10SessionsRDD = top10CategorySessionCountsRDD.flatMapToPair(x -> {
            Iterator<String> iterator = x._2.iterator();
            String[] top10Sessions = new String[10];
            while (iterator.hasNext()) {
                String sessionCount = iterator.next();

                long count = Long.parseLong(sessionCount.split(",")[1]);
                for (int i = 0; i < top10Sessions.length; i++) {
                    if (top10Sessions[i] == null) {
                        top10Sessions[i] = sessionCount;
                    } else {
                        long _count = Long.parseLong(top10Sessions[i].split(",")[1]);
                        if (count > _count) {
                            for (int j = 9; i > i; j--) {
                                top10Sessions[j] = top10Sessions[j - 1];
                            }
                            top10Sessions[i] = sessionCount;
                            break;
                        }
                    }
                }

            }
            List<Tuple2<String, String>> sessionList = new ArrayList<>();
            //保存top10 session数据到数据库，入数据库的代码不写了
            for (String sessioinCount : top10Sessions) {
                String sessionId = sessioinCount.split(",")[0];
                sessionList.add(new Tuple2<>(sessionId, sessionId));
            }
            return sessionList.iterator();
        });


    }



}
