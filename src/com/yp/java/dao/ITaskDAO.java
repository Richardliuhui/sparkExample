package com.yp.java.dao;

import com.yp.java.model.Task;

public interface ITaskDAO {

    Task findTaskById(Integer taskId);

}
