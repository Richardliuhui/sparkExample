package com.yp.java.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Project: scala-test
 * @Package com.yp.java.conf
 * @Description: TODO
 * @date Date : 2018年10月01日 下午4:12
 */
public class ConfigurationManager {

    private static Properties properties=new Properties();
    static {
        InputStream inputStream=ConfigurationManager.class.getClassLoader().getResourceAsStream("application.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
