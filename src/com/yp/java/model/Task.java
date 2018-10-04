package com.yp.java.model;

/**
 * @Project: scala-test
 * @Package com.yp.java.model
 * @Description: TODO
 * @date Date : 2018年10月01日 下午5:41
 */
public class Task {
    private Integer taskId;
    private String dataJson;
    private String taskName;
    private Integer taskType;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Integer getTaskType() {
        return taskType;
    }

    public void setTaskType(Integer taskType) {
        this.taskType = taskType;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getDataJson() {
        return dataJson;
    }

    public void setDataJson(String dataJson) {
        this.dataJson = dataJson;
    }
}
