package com.yp.java.util;

import java.text.ParseException;
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
    public static final SimpleDateFormat DATE_HOUR_FORMAT=new SimpleDateFormat("yyyy-MM-dd_HH");


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
    public static Date parseTime(String str) throws ParseException {
        return TIME_FORMT.parse(str);
    }
    public static Date parseTimeHour(String str) throws ParseException {

        return DATE_HOUR_FORMAT.parse(str);
    }
    public static String formatTime(Date date) throws ParseException {
        return TIME_FORMT.format(date);
    }
    public static String formatHour(Date date) throws ParseException {
        return DATE_HOUR_FORMAT.format(date);
    }
    public static String getDateHour(String str)throws ParseException{
        Date date=parseTime(str);
        String dateHour=formatHour(date);
        return dateHour;
    }

    public static void main(String[] args)throws ParseException {
        String str="2018-10-01 12:23:58";
        String dateHour=getDateHour(str);
        System.out.println(dateHour);
    }
}
