package com.yp.java.jdbc;

import com.yp.java.conf.ConfigurationManager;
import com.yp.java.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @Project: scala-test
 * @Package com.yp.java.jdbc
 * @Description: TODO
 * @date Date : 2018年10月01日 下午4:48
 */
public class JDBCHelper {

    static {
        try {
            Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance=null;
    private LinkedList<Connection> dataSource=new LinkedList<>();
    private JDBCHelper(){
        int dataSize=ConfigurationManager.getIntegerValue(Constants.JDBC_DATASOURCE_SIZE);
        String url=ConfigurationManager.getProperty(Constants.JDBC_DATASOURCE_URL);
        String user=ConfigurationManager.getProperty(Constants.JDBC_DATASOURCE_USER);
        String password=ConfigurationManager.getProperty(Constants.JDBC_DATASOURCE_PASSWORD);
        for (int i=0;i<dataSize;i++){
            Connection con= null;
            try {
                con = DriverManager.getConnection(url,user,password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            dataSource.push(con);
        }
    }
    public static JDBCHelper getInstance(){
        if(null==instance){
            synchronized (JDBCHelper.class){
                if(null==instance){
                    instance=new JDBCHelper();
                }
            }
        }
        return instance;
    }
    public synchronized Connection getConnection(){
        while (dataSource.size()==0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }

    /***
     * 执行增、删、改
     * @param sql
     * @param params
     * @return
     */
    public int excuteUpdate(String sql,Object[]params){
        Connection con=null;
        PreparedStatement statement=null;
        int rtn=0;
        try {
            con=getConnection();
            statement=con.prepareStatement(sql);
            for (int i = 0; i <params.length ; i++) {
               statement.setObject(i+1,params[i]);
            }
            rtn=statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null!=con){
                dataSource.push(con);
            }
        }
        return rtn;
    }
    public void excuteQuery(String sql,Object[]params,QueryCallback callback){
          Connection con=null;
          PreparedStatement statement=null;
          ResultSet rs=null;
        try {
            con=getConnection();
            statement=con.prepareStatement(sql);
            for (int i = 0; i <params.length ; i++) {
                statement.setObject(i+1,params[i]);
            }
            rs=statement.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null!=con){
                dataSource.push(con);
            }
        }
    }
    public  int[] excuteBatch(String sql, List<Object[]> paramList){
        int []rnt=null;
        Connection con=null;
        PreparedStatement statement=null;
        try {
            con=getConnection();
            con.setAutoCommit(false);
            statement=con.prepareStatement(sql);
            for (Object[] param:paramList){
                for (int i=0;i<param.length;i++){
                    statement.setObject(i+1,param[i]);
                }
                statement.addBatch();
            }
            rnt=statement.executeBatch();
            con.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null!=con){
                dataSource.push(con);
            }
        }
        return rnt;
    }
    static interface QueryCallback{
         void process(ResultSet resultSet);
    }
}
