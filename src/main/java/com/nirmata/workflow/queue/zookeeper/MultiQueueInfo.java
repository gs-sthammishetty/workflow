package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;

/**
 * @author sthammishetty on 07/08/20
 */
public class MultiQueueInfo {

    private TaskType primaryTaskType;
    private TaskType secondaryTaskType;
    private TaskExecutor taskExecutor;
    private int qty;

    public MultiQueueInfo(TaskType primaryTaskType, TaskType secondaryTaskType, TaskExecutor taskExecutor, int qty) {
        this.primaryTaskType = primaryTaskType;
        this.secondaryTaskType = secondaryTaskType;
        this.taskExecutor = taskExecutor;
        this.qty = qty;
    }

    public TaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public TaskType getPrimaryTaskType() {
        return primaryTaskType;
    }

    public void setPrimaryTaskType(TaskType primaryTaskType) {
        this.primaryTaskType = primaryTaskType;
    }

    public TaskType getSecondaryTaskType() {
        return secondaryTaskType;
    }

    public void setSecondaryTaskType(TaskType secondaryTaskType) {
        this.secondaryTaskType = secondaryTaskType;
    }

    public int getQty() {
        return qty;
    }

    public void setQty(int qty) {
        this.qty = qty;
    }
}
