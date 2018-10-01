package com.yp.java.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Project: scala-test
 * @Package com.yp.java.util
 * @Description: TODO
 * @date Date : 2018年10月01日 下午4:00
 */
public class DateUtils {

    public static final SimpleDateFormat TIME_FORMT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd");

    public static boolean before(String time1,String time2){
        try {
            Date dateTime1=TIME_FORMT.parse(time1);
            Date dateTime2=TIME_FORMT.parse(time2);
            if(dateTime1.before(dateTime2)){
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        return false;
    }
}
