package com.yp.java.model;

/**
 * @Project: scala-test
 * @Package com.yp.java.model
 * @Description: TODO
 * @date Date : 2018年10月03日 上午11:13
 */
public class SessionAggrStatModel {

    private long taskId;
    private long sessionCount;
    private double visit_length_1_3_ratio;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(long sessionCount) {
        this.sessionCount = sessionCount;
    }

    public double getVisit_length_1_3_ratio() {
        return visit_length_1_3_ratio;
    }

    public void setVisit_length_1_3_ratio(double visit_length_1_3_ratio) {
        this.visit_length_1_3_ratio = visit_length_1_3_ratio;
    }
}
