package com.yp.java;

import com.yp.java.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: TODO
 * @date Date : 2018年10月02日 下午10:53
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String,String>{
    private String result="sessionCount=0|time_1s_3s=0|time_4s_6s=0|time_7s_9s=0|" +
            "time_10s_30s=0|time_30s_60s=0|time_1m_3m=0|" +
            "time_3m_10m=0|time_10m_30=0|time_30m=0|step_1_3=0" +
            "step_4_6=0|step_7_9=0|step_10_30=0|step_30_60=0|step_60=0";
    @Override
    public boolean isZero() {
        return true;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator statAccumulator=new SessionAggrStatAccumulator();
        statAccumulator.result=result;
        return statAccumulator;
    }

    /***
     * 数据初始化
     */
    @Override
    public void reset() {
        result="sessionCount=0|time_1s_3s=0|time_4s_6s=0|time_7s_9s=0|" +
                "time_10s_30s=0|time_30s_60s=0|time_1m_3m=0|" +
                "time_3m_10m=0|time_10m_30=0|time_30m=0|step_1_3=0" +
                "step_4_6=0|step_7_9=0|step_10_30=0|step_30_60=0|step_60=0";
    }

    @Override
    public void add(String v) {
        String v1 = result;
        String v2 = v;
        if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
            String newResult = "";
            // 从v1中，提取v2对应的值，并累加
            String oldValue = StringUtil.getFieldFromConcatString(v1, "\\|", v2);
            if (oldValue != null) {
                Integer newValue = Integer.parseInt(oldValue) + 1;
                newResult = StringUtil.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
            }
            result = newResult;
        }
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
          result=other.value();
    }

    @Override
    public String value() {
        return result;
    }
}
