package com.yp.java.dao;

import com.yp.java.dao.impl.SessionAggrDAOImpl;
import com.yp.java.dao.impl.TaskDAOImpl;

/**
 * @Project: scala-test
 * @Package com.yp.java.dao
 * @Description: TODO
 * @date Date : 2018年10月01日 下午5:46
 */
public class DAOFactory {
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrDAOImpl();
    }
}
