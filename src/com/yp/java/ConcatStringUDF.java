package com.yp.java;

import org.apache.spark.sql.api.java.UDF3;

/**
 * @author liuhui
 * @date 2018-09-29 上午11:10
 */
public class ConcatStringUDF implements UDF3<String,String,String,String>{
    @Override
    public String call(String v1, String v2, String v3) throws Exception {
        return v1+"_"+v2+"_"+v3;
    }
}
